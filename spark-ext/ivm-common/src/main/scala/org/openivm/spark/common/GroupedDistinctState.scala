package org.openivm.spark.common

import java.nio.charset.StandardCharsets.UTF_8
import java.security.MessageDigest
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.{DataFrame, Encoders, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.catalyst.expressions.{
  And,
  ArrayContains,
  If,
  IsNotNull,
  Alias,
  AttributeReference,
  Cast,
  CreateNamedStruct,
  DateDiff,
  Expression,
  Literal
}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Count}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Expand}
import org.openivm.spark.common.rocksdb.{OpenIvmRocksDB, OpenIvmRocksDBBatchOps, OpenIvmRocksDBRegistry, RocksDBCodec}

/** Bounded grouped state with native weighted DISTINCT zero crossings,
  * with exact current-frequency lookup on cache misses instead of an eager full
  * frequency-table bootstrap. Missing cache entry ALWAYS means unknown.
  * Ordinary floating-point aggregates are recomputed by unchanged Spark SQL.
  */
private[spark] final class GroupedDistinctState(
    spark: SparkSession,
    view: String,
    program: String,
    inputSchema: StructType,
    summarySchema: StructType,
    keys: Vector[String],
    countName: String,
    channels: Vector[(String, String)],
    maxCache: Int = 100000
) {
  require(keys.nonEmpty && channels.nonEmpty && maxCache >= 0)
  require(
    (inputSchema.fieldNames ++ summarySchema.fieldNames).forall(n =>
      !n.toLowerCase(java.util.Locale.ROOT).startsWith("__rd_")
    ),
    "reserved grouped-state field"
  )
  private val JsonOptions = Map(
    "ignoreNullFields"   -> "false",
    "timeZone"           -> "UTC",
    "timestampFormat"    -> "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX",
    "timestampNTZFormat" -> "yyyy-MM-dd'T'HH:mm:ss.SSSSSS",
    "mode"               -> "FAILFAST"
  )
  private val mapper      = new ObjectMapper()
  private val path        = OpenIvmStatePaths.perMvDbPath(spark, view)
  private val db          = OpenIvmRocksDBRegistry.getOrOpen(spark, path, OpenIvmStatePaths.PerMvColumnFamilies)
  private val Cf          = "default"
  private val GroupPrefix = RocksDBCodec.utf8("grouped_distinct:v1:g:")
  private val GroupEnd    = RocksDBCodec.utf8("grouped_distinct:v1:g;")
  private val FreqPrefix  = RocksDBCodec.utf8("grouped_distinct:v1:f:")
  private val FreqEnd     = RocksDBCodec.utf8("grouped_distinct:v1:f;")
  private val Header      = RocksDBCodec.utf8("grouped_distinct:v1:header")
  private def settingsFingerprint: String = {
    val conf    = spark.sessionState.conf
    val defined = conf.getAllDefinedConfs.map(entry => entry._1 -> entry._2).toMap
    // REFRESH overrides this execution-policy knob in its isolated session.
    // It changes join selection, not the meaning of cached grouped values.
    val sql = (defined ++ conf.getAllConfs).toVector
      .filter { case (key, _) =>
        key.startsWith("spark.sql.") && key != "spark.sql.adaptive.autoBroadcastJoinThreshold"
      }
      .sortBy(_._1)
    val encoded = mapper.writeValueAsBytes(sql.map { case (k, v) => Array(k, v) }.toArray)
    spark.version + ":" + MessageDigest.getInstance("SHA-256").digest(encoded).map(b => f"${b & 255}%02x").mkString
  }
  private val settings                = settingsFingerprint
  private def requireSettings(): Unit = require(settingsFingerprint == settings, "group-state SQL settings changed")
  private val identity =
    "typed-frequency-v3\n" + program + "\n" + inputSchema.json + "\n" + summarySchema.json + "\n" + keys.mkString(",") +
      "\n" + channels.mkString(",") + "\n" + spark.conf.get("spark.sql.session.timeZone") + "\n" + settings
  private val g  = "__rd_g"; private val ch     = "__rd_ch"; private val value = "__rd_v"; private val weight = "__rd_w"
  private val tg = "__rd_group"; private val tv = "__rd_value"
  private val typedIndex = Vector(tg, ch, tv)
  private val GroupSchema = StructType(keys.map(n => {
    val f = inputSchema(n); if (f.dataType == StringType) f.copy(dataType = BinaryType) else f
  }))
  private val ValueSchema = StructType(Vector(StructField("l", LongType), StructField("b", BinaryType)))
  private val MaxGroups   = 100000; private val MaxUpdates = 1000000; private val MaxBytes = 64L * 1024L * 1024L
  private def supported(t: DataType): Boolean =
    t == LongType || t == IntegerType || t == ShortType || t == ByteType || t == DateType || t == StringType
  require(
    keys.forall(n => supported(inputSchema(n).dataType)) &&
      channels.forall(c => supported(inputSchema(c._1).dataType)),
    "typed frequency keys require integral/date/binary-string fields"
  )
  require(
    (countName +: channels.map(_._2)).forall(n => summarySchema(n).dataType == LongType),
    "COUNT output schema mismatch"
  )
  private def quoted(name: String): String = "`" + name.replace("`", "``") + "`"
  private def field(name: String)          = col(quoted(name))
  private def json(names: Seq[String])     = to_json(struct(names.map(field): _*), JsonOptions)
  private def keyValue(name: String, schema: StructType) =
    (if (schema(name).dataType == StringType) field(name).cast(BinaryType) else field(name)).as(name)
  private def withGroupKey(df: DataFrame): DataFrame =
    df.withColumn(g, to_json(struct(keys.map(n => keyValue(n, df.schema)): _*), JsonOptions))
  private def selectChanged(joined: DataFrame, requested: Vector[String]): DataFrame = {
    // A single group's literal filter avoids a broadcast stage and can reach
    // source scans through input joins. Larger sets keep the original semi-join.
    if (
      requested.size <= 1 && requested.size.toLong * keys.size <= 256L &&
      requested.iterator.map(_.getBytes(UTF_8).length.toLong).sum * keys.size <= 64L * 1024L
    ) {
      val predicate = requested.headOption
        .map { key =>
          val decoded = from_json(lit(key), GroupSchema, JsonOptions)
          keys
            .map { n =>
              val actual = if (inputSchema(n).dataType == StringType) field(n).cast(BinaryType) else field(n)
              actual.eqNullSafe(decoded.getField(n))
            }
            .reduce(_ && _)
        }
        .getOrElse(lit(false))
      return joined.filter(predicate)
    }
    val decoded = spark
      .createDataset(requested)(Encoders.STRING)
      .toDF(g)
      .select(from_json(col(g), GroupSchema, JsonOptions).as("k"))
    val source = joined.alias("s"); val wanted = broadcast(decoded).alias("d")
    val condition = keys
      .map { n =>
        val source = col(s"s.${quoted(n)}")
        val actual = if (inputSchema(n).dataType == StringType) source.cast(BinaryType) else source
        actual.eqNullSafe(col(s"d.k.${quoted(n)}"))
      }
      .reduce(_ && _)
    source.join(wanted, condition, "left_semi").select("s.*")
  }
  private def groupKey(key: String) = GroupPrefix ++ key.getBytes(UTF_8)
  private case class Index(group: String, channel: Int, value: String)
  private case class FrequencyProbe(channel: Int, request: String, input: String, count: String)
  private def requestFrame(groups: Map[String, Vector[Index]], probes: Vector[FrequencyProbe]): DataFrame = {
    val schema = StructType(StructField(g, StringType) +: probes.map(p => StructField(p.request, StringType)))
    val rows   = new java.util.ArrayList[Row](groups.size)
    groups.foreach { case (group, requested) =>
      val values = requested.map(k => k.channel -> k.value).toMap
      rows.add(Row.fromSeq(group +: probes.map(p => values.get(p.channel).orNull)))
    }
    spark.createDataFrame(rows, schema)
  }
  private def frequencyKey(k: Index) = FreqPrefix ++ RocksDBCodec.compositeKey(
    Seq(k.group.getBytes(UTF_8), RocksDBCodec.encodeLongBE(k.channel.toLong), k.value.getBytes(UTF_8))
  )
  private def decode(rows: Vector[String]): DataFrame = spark.read
    .schema(summarySchema)
    .options(JsonOptions)
    .json(spark.createDataset(rows)(Encoders.STRING))
  private def header(epoch: String, cached: Long): Array[Byte] =
    RocksDBCodec.utf8(mapper.writeValueAsString(Vector(identity, epoch, cached.toString).toArray))
  private def check(epoch: String): Long = {
    requireSettings()
    val raw = db.get(Cf, Header).getOrElse(throw new IllegalArgumentException("missing demand-state generation"))
    val v   = mapper.readTree(raw)
    require(
      v.size() == 3 && v.get(0).asText() == identity && v.get(1).asText() == epoch,
      "stale demand-state generation"
    )
    val n = v.get(2).asText().toLong; require(n >= 0 && n <= maxCache, "invalid frequency cache size"); n
  }
  def close(): Unit                   = OpenIvmRocksDBRegistry.close(path)
  def cachedSize(epoch: String): Long = db.withSession { check(epoch) }
  private def encodeRows(df: DataFrame): Vector[(String, String)] = {
    val found = GroupedDistinctState.collectBounded(
      withGroupKey(df).select(col(g), json(summarySchema.fieldNames.toVector)),
      MaxGroups + 1
    )
    require(found.size <= MaxGroups, "group-state cap")
    val rows = found.map(r => r.getString(0) -> r.getString(1))
    require(rows.map(_._1).distinct.size == rows.size, "duplicate full group identity")
    require(
      rows.map { case (k, v) => k.getBytes(UTF_8).length.toLong + v.getBytes(UTF_8).length }.sum <= MaxBytes,
      "group-state byte cap"
    )
    rows
  }
  def prepareBootstrap(summary: DataFrame, epoch: String): Bootstrap = {
    requireSettings(); new Bootstrap(encodeRows(summary), epoch, db.get(Cf, Header))
  }
  final class Bootstrap private[GroupedDistinctState] (
      rows: Vector[(String, String)],
      epoch: String,
      prior: Option[Array[Byte]]
  ) {
    def publish(checkpoint: Array[Byte]): Unit = db.withBatch { b =>
      stageInto(db, b); OpenIvmRocksDBBatchOps.put(db, b, Cf, GroupedDistinctState.CheckpointKey, checkpoint)
    }
    def stageInto(shared: OpenIvmRocksDB, b: org.rocksdb.WriteBatch): Unit = {
      requireSettings()
      require(shared eq db, "demand bootstrap must share the owning MV shard")
      val current = db.get(Cf, Header)
      require(
        current.size == prior.size && current.zip(prior).forall { case (a, b) => java.util.Arrays.equals(a, b) },
        "demand bootstrap generation changed"
      )
      OpenIvmRocksDBBatchOps.deleteRange(db, b, Cf, GroupPrefix, GroupEnd)
      OpenIvmRocksDBBatchOps.deleteRange(db, b, Cf, FreqPrefix, FreqEnd)
      rows.foreach { case (k, v) => OpenIvmRocksDBBatchOps.put(db, b, Cf, groupKey(k), v.getBytes(UTF_8)) }
      OpenIvmRocksDBBatchOps.put(db, b, Cf, Header, header(epoch, 0L))
    }
  }
  def all(epoch: String): DataFrame = decode(db.withSession {
    check(epoch);
    db.boundedPrefixScan(Cf, GroupPrefix, MaxGroups, MaxBytes)
      .map(p => RocksDBCodec.fromUtf8(p._2))
  })
  private def unfold(joined: DataFrame, active: Vector[Int], includeCount: Boolean): DataFrame = {
    val group = joined.withColumn(tg, struct(keys.map(n => keyValue(n, joined.schema)): _*))
    val empty = struct(lit(null).cast(LongType).as("l"), lit(null).cast(BinaryType).as("b"))
    val row   = struct(lit(-1).as(ch), empty.as(tv), col("openivm_multiplicity").cast(LongType).as(weight))
    // Payload-only UPDATEs may cancel every count/frequency contribution.
    // Still recompute the affected group's ordinary aggregates and projections.
    val dirty = struct(
      lit(-2).as(ch),
      empty.as(tv),
      when(col("openivm_multiplicity") =!= 0L, lit(1L)).otherwise(lit(0L)).as(weight)
    )
    val distinct = active.map { i =>
      val name = channels(i)._1
      val t    = joined.schema(name).dataType
      val long =
        if (t == StringType) lit(null).cast(LongType)
        else if (t == DateType) datediff(field(name), lit("1970-01-01").cast(DateType)).cast(LongType)
        else field(name).cast(LongType)
      val binary = if (t == StringType) field(name).cast(BinaryType) else lit(null).cast(BinaryType)
      val typed  = struct(long.as("l"), binary.as("b"))
      when(
        field(name).isNotNull,
        struct(lit(i).as(ch), typed.as(tv), col("openivm_multiplicity").cast(LongType).as(weight))
      )
    }
    val entries = (if (includeCount) Vector(row, dirty) else Vector.empty) ++ distinct
    group
      .select(col(tg), explode(array(entries: _*)).as("v"))
      .filter(col("v").isNotNull)
      .select(col(tg), col("v.*"))
  }
  private def expandCurrent(joined: DataFrame, probes: Vector[(Int, String)]): DataFrame = {
    val group  = joined.withColumn(tg, struct(keys.map(n => keyValue(n, joined.schema)): _*))
    val child  = group.queryExecution.analyzed
    val source = child.output.map(a => a.name -> a).toMap
    val empty = CreateNamedStruct(
      Seq(Literal("l"), Literal.create(null, LongType), Literal("b"), Literal.create(null, BinaryType))
    )
    val tag      = AttributeReference(ch, IntegerType, nullable = true)()
    val valueTag = AttributeReference(tv, ValueSchema, nullable = false)()
    // Payload slots can be nulled by Expand. Fresh IDs keep the optimizer from
    // restoring the original input's non-nullability after constant folding.
    val outputs =
      child.output.map(a => if (keys.contains(a.name) || a.name == tg) a else a.withNullability(true).newInstance())
    val regular: Seq[Expression] = child.output ++ Seq(Literal(-1), empty)
    val projections = Vector(regular) ++ probes.map { case (channel, request) =>
      val name = channels(channel)._1; val a = source(name)
      val long: Expression =
        if (a.dataType == StringType) Literal.create(null, LongType)
        else if (a.dataType == DateType) Cast(DateDiff(a, Literal.create(0, DateType)), LongType)
        else Cast(a, LongType)
      val binary: Expression = if (a.dataType == StringType) Cast(a, BinaryType) else Literal.create(null, BinaryType)
      val typed              = CreateNamedStruct(Seq(Literal("l"), long, Literal("b"), binary))
      child.output.map(a => if (keys.contains(a.name) || a.name == tg) a else Literal.create(null, a.dataType)) ++
        Seq(
          If(
            And(IsNotNull(a), ArrayContains(source(request), typed)),
            Literal(channel),
            Literal.create(null, IntegerType)
          ),
          typed
        )
    }
    org.apache.spark.sql.openivm.StreamingDatasetAccess
      .ofRows(spark, Expand(projections, outputs ++ Seq(tag, valueTag), child))
      .filter(col(ch).isNotNull)
  }
  private case class Counts(n: Long, distinct: Vector[Long]) {
    def valid(): Unit = {
      require(n >= 0 && distinct.forall(x => x >= 0 && x <= n), "invalid group/distinct multiplicity")
    }
  }
  private def counts(row: Option[String]): Counts = row
    .map { v =>
      val node = mapper.readTree(v)
      Counts(node.get(countName).asLong(), channels.map(c => node.get(c._2).asLong()))
    }
    .getOrElse(Counts(0L, Vector.fill(channels.size)(0L)))

  def prepare(delta: DataFrame, currentJoined: DataFrame, epoch: String)(
      ordinary: DataFrame => DataFrame
  ): Prepared#Completed = {
    val cacheBefore = db.withSession { check(epoch) }
    val contributions = unfold(delta, channels.indices.toVector, true)
      .groupBy(typedIndex.map(col): _*)
      .agg(sum(weight).as(weight))
      .filter(col(weight) =!= 0L)
      .select(to_json(col(tg), JsonOptions).as(g), col(ch), to_json(col(tv), JsonOptions).as(value), col(weight))
    val consolidated = GroupedDistinctState.collectBounded(contributions, MaxUpdates + 1)
    require(consolidated.size <= MaxUpdates, "demand contribution cap")
    val changes = consolidated.map(r =>
      Index(r.getAs[String](g), r.getAs[Int](ch), r.getAs[String](value)) -> r.getAs[Long](weight)
    )
    require(
      changes.map { case (k, _) =>
        k.group.getBytes(UTF_8).length.toLong + k.value.getBytes(UTF_8).length + 16L
      }.sum <= MaxBytes,
      "demand contribution byte cap"
    )
    val affected  = changes.map(_._1.group).distinct
    val frequency = changes.filter(_._1.channel >= 0)
    val (oldRows, cached) = db.withSession {
      check(epoch)
      (
        db.multiGet(Cf, affected.map(groupKey)).map(_.map(RocksDBCodec.fromUtf8)).toVector,
        db.multiGet(Cf, frequency.map(p => frequencyKey(p._1))).map(_.map(RocksDBCodec.decodeLongBE)).toVector
      )
    }
    val misses  = frequency.zip(cached).collect { case ((k, _), None) => k }
    val touched = selectChanged(currentJoined, affected)
    val requestBytes =
      misses.map(k => k.group.getBytes(UTF_8).length.toLong + k.value.getBytes(UTF_8).length + 8L).sum
    val requests          = misses.groupBy(_.group)
    val requestedChannels = misses.map(_.channel).distinct
    val wideBytes         = requestBytes + requests.size.toLong * requestedChannels.size * ValueSchema.defaultSize
    // A unique requested value per (group,channel) is a functional dependency,
    // not a group-count threshold. Attach all channels once before aggregation.
    val requestedValues = misses.map(k => (k.group -> k.channel) -> k.value).toMap
    val direct          = requestedValues.size == misses.size && wideBytes <= MaxBytes
    val probes =
      if (direct) requestedChannels.zipWithIndex.map { case (channel, i) =>
        FrequencyProbe(channel, "__rd_probe_value_" + i, "__rd_probe_input_" + i, "__rd_probe_count_" + i)
      }
      else Vector.empty
    val requestRows = if (probes.nonEmpty) Some(requestFrame(requests, probes)) else None
    // One Spark action produces ordinary aggregates AND requested current
    // frequencies. There is no persisted intermediate or second group job.
    val tagged =
      if (direct) {
        val input = requestRows
          .map { rows =>
            val wanted = rows.select(
              (Vector(from_json(col(g), GroupSchema, JsonOptions).as(tg)) ++
                probes.map(p => from_json(col(p.request), ValueSchema, JsonOptions).as(p.request))): _*
            )
            val probe = if (wideBytes <= 1024L * 1024L) broadcast(wanted) else wanted
            touched.withColumn(tg, struct(keys.map(n => keyValue(n, touched.schema)): _*)).join(probe, Seq(tg), "left")
          }
          .getOrElse(touched)
        probes.foldLeft(input) { case (input, p) =>
          val column = channels(p.channel)._1
          val t      = inputSchema(column).dataType
          val actual =
            if (t == StringType) field(column).cast(BinaryType)
            else if (t == DateType) datediff(field(column), lit("1970-01-01").cast(DateType)).cast(LongType)
            else field(column).cast(LongType)
          val wanted = col(p.request).getField(if (t == StringType) "b" else "l")
          input.withColumn(
            p.input,
            when(col(p.request).isNotNull && field(column).isNotNull && actual.eqNullSafe(wanted), lit(1L))
          )
        }
      } else {
        val frequencyProbes = requestedChannels.zipWithIndex.map { case (channel, i) =>
          channel -> ("__rd_requested_values_" + i)
        }
        val schema = StructType(
          StructField(g, StringType) +:
            frequencyProbes.map(p => StructField(p._2, ArrayType(StringType, containsNull = false)))
        )
        val rows = new java.util.ArrayList[Row](requests.size)
        requests.foreach { case (group, values) =>
          val byChannel = values.groupBy(_.channel)
          rows.add(Row.fromSeq(group +: frequencyProbes.map(p => byChannel.getOrElse(p._1, Vector.empty).map(_.value))))
        }
        val wanted = spark
          .createDataFrame(rows, schema)
          .select(
            (Vector(from_json(col(g), GroupSchema, JsonOptions).as(tg)) ++ frequencyProbes
              .map(p => transform(col(p._2), v => from_json(v, ValueSchema, JsonOptions)).as(p._2))): _*
          )
        val probe = if (requestBytes <= 1024L * 1024L) broadcast(wanted) else wanted
        val input = touched
          .withColumn(tg, struct(keys.map(n => keyValue(n, touched.schema)): _*))
          .join(probe, Seq(tg), "left")
        // Attach one request row per group before Expand, keeping ordinary
        // aggregates unchanged while discarding unrequested frequency lanes.
        expandCurrent(input, frequencyProbes)
      }
    val normal = ordinary(tagged).queryExecution.analyzed match {
      case a: Aggregate => a
      case other => throw new IllegalArgumentException("one ordinary grouped aggregate required: " + other.nodeName)
    }
    require(
      normal.groupingExpressions.size == keys.size &&
        normal.collect { case a: Aggregate => a }.size == 1 && normal.expressions.forall(_.deterministic),
      "unsupported ordinary aggregation graph"
    )
    val rowCount = normal.aggregateExpressions
      .find(_.name == countName)
      .get
      .collect { case a: AggregateExpression => a }
    require(
      rowCount.size == 1 && !rowCount.head.isDistinct && rowCount.head.filter.isEmpty &&
        rowCount.head.aggregateFunction.isInstanceOf[Count] &&
        rowCount.head.aggregateFunction.children.forall(e => e.foldable && e.eval() != null),
      "frequency output requires the original COUNT(*)"
    )
    val ordinaryNames = normal.output.map(_.name).toVector
    val fused = if (direct) {
      val counts = probes.map { p =>
        val value = normal.child.output.find(_.name == p.input).get
        Alias(Count(Seq(value)).toAggregateExpression(), p.count)()
      }
      normal.copy(aggregateExpressions = normal.aggregateExpressions ++ counts)
    } else {
      val tags = Vector(ch, tv).map(n => normal.child.output.find(_.name == n).get)
      normal.copy(
        groupingExpressions = normal.groupingExpressions ++ tags,
        aggregateExpressions = normal.aggregateExpressions ++ tags
      )
    }
    val aggregate = org.apache.spark.sql.openivm.StreamingDatasetAccess.ofRows(spark, fused)
    val grouped   = withGroupKey(aggregate)
    val result = if (direct) {
      val ordinaryRow = struct(
        lit(-1).as(ch),
        lit("{\"l\":null,\"b\":null}").as(value),
        field(countName).as("__rd_current_count"),
        json(ordinaryNames).as("__rd_ordinary")
      )
      val frequencyRows = probes.map { p =>
        struct(
          lit(p.channel).as(ch),
          lit(null).cast(StringType).as(value),
          col(p.count).as("__rd_current_count"),
          lit(null).cast(StringType).as("__rd_ordinary")
        )
      }
      grouped
        .select(col(g), explode(array((ordinaryRow +: frequencyRows): _*)).as("__rd_record"))
        .select(col(g), col("__rd_record.*"))
        .filter(col(ch) === -1 || col("__rd_current_count") > 0L)
    } else
      grouped.select(
        col(g),
        col(ch),
        to_json(col(tv), JsonOptions).as(value),
        field(countName).as("__rd_current_count"),
        when(col(ch) === -1, json(ordinaryNames)).otherwise(lit(null).cast(StringType)).as("__rd_ordinary")
      )
    val records = GroupedDistinctState.collectBounded(result, affected.size + misses.size + 1)
    require(records.size <= affected.size + misses.size, "unexpected fused aggregate cardinality")
    // Unique (group,channel) requests are already driver-local. Restore their
    // original JSON after the bounded collect, without another Spark join.
    def recordValue(row: Row): String = {
      val channel = row.getAs[Int](ch)
      if (direct && channel >= 0) requestedValues(row.getAs[String](g) -> channel)
      else row.getAs[String](value)
    }
    require(
      records
        .map(r =>
          r.getString(0).getBytes(UTF_8).length.toLong +
            Option(r.getAs[String]("__rd_ordinary")).fold(0L)(_.getBytes(UTF_8).length.toLong) +
            Option(recordValue(r)).fold(0L)(_.getBytes(UTF_8).length.toLong) + 16L
        )
        .sum <= MaxBytes,
      "fused aggregate byte cap"
    )
    val currentFrequencies = records
      .filter(_.getAs[Int](ch) >= 0)
      .map { r =>
        Index(r.getAs[String](g), r.getAs[Int](ch), recordValue(r)) -> r.getAs[Long]("__rd_current_count")
      }
      .toMap
    require(currentFrequencies.keySet.subsetOf(misses.toSet), "unrequested fused frequency")
    val signed   = frequency.toMap
    val resolved = misses.map(k => k -> Math.subtractExact(currentFrequencies.getOrElse(k, 0L), signed(k))).toMap
    val next     = scala.collection.mutable.LinkedHashMap.empty[String, Counts]
    affected.zip(oldRows).foreach { case (k, v) => next.update(k, counts(v)) }
    changes.filter(_._1.channel == -1).foreach { case (k, w) =>
      val c = next(k.group); next.update(k.group, c.copy(n = Math.addExact(c.n, w)))
    }
    val writes = frequency.zip(cached).map { case ((k, w), hit) =>
      val old     = hit.getOrElse(resolved(k)); require(old >= 0, "negative cached frequency")
      val updated = Math.addExact(old, w); require(updated >= 0, "negative frequency transition")
      val dd      = (if (updated > 0) 1L else 0L) - (if (old > 0) 1L else 0L)
      val c       = next(k.group)
      next.update(k.group, c.copy(distinct = c.distinct.updated(k.channel, Math.addExact(c.distinct(k.channel), dd))))
      k -> updated
    }
    next.values.foreach(_.valid())
    val strategy = if (direct) "conditional_counts" else "expanded_frequencies"
    val prepared = new Prepared(epoch, cacheBefore, next.toVector, writes, misses.size, strategy)
    val completedRows = records.filter(_.getAs[Int](ch) == -1).map { r =>
      val key     = r.getAs[String](g)
      val payload = r.getAs[String]("__rd_ordinary")
      // This is Spark's nonempty struct JSON. Preserve its numeric lexemes:
      // parsing/re-emitting ordinary values through DoubleNode rounds DECIMAL.
      val distinct = channels.zipWithIndex
        .map { case ((_, name), i) =>
          mapper.writeValueAsString(name) + ":" + next(key).distinct(i).toString
        }
        .mkString(",")
      key -> (payload.dropRight(1) + "," + distinct + "}")
    }
    prepared.complete(completedRows)
  }
  final class Prepared private[GroupedDistinctState] (
      epoch: String,
      cacheBefore: Long,
      changed: Vector[(String, Counts)],
      frequencies: Vector[(Index, Long)],
      val misses: Int,
      strategy: String
  ) {
    val touchedKeys: Vector[String] = changed.map(_._1)
    val hits: Int                   = frequencies.size - misses
    def complete(rows: Vector[(String, String)]): Completed = {
      require(rows.size <= MaxGroups && rows.map(_._1).distinct.size == rows.size, "invalid fused group bag")
      val expected = changed.filter(_._2.n > 0).toMap
      require(rows.map(_._1).toSet == expected.keySet, "recomputed group set disagrees with signed input")
      rows.foreach { case (k, json) =>
        val observed = counts(Some(json));
        require(observed == expected(k), "grouped COUNT disagrees with native delta/frequency state")
      }
      new Completed(rows)
    }
    final class Completed private[Prepared] (rows: Vector[(String, String)]) {
      val hits: Int              = Prepared.this.hits
      val misses: Int            = Prepared.this.misses
      val lookupStrategy: String = strategy
      private val nextRows: Vector[String] = {
        val old = db
          .withSession { check(epoch); db.boundedPrefixScan(Cf, GroupPrefix, MaxGroups, MaxBytes) }
          .map { case (k, v) => new String(k.drop(GroupPrefix.length), UTF_8) -> RocksDBCodec.fromUtf8(v) }
          .toMap
        val all = (old -- touchedKeys) ++ rows
        require(
          all.size <= MaxGroups && all.valuesIterator.map(_.getBytes(UTF_8).length.toLong).sum <= MaxBytes,
          "next summary cap"
        )
        all.values.toVector
      }
      def summary: DataFrame = decode(nextRows)
      def publish(nextEpoch: String, checkpoint: Array[Byte]): Unit = db.withBatch { b =>
        stageInto(db, b, nextEpoch);
        OpenIvmRocksDBBatchOps.put(db, b, Cf, GroupedDistinctState.CheckpointKey, checkpoint)
      }
      def stageInto(shared: OpenIvmRocksDB, b: org.rocksdb.WriteBatch, nextEpoch: String): Unit = {
        require(shared eq db, "demand state must share the owning MV shard")
        require(check(epoch) == cacheBefore, "frequency cache changed during prepare")
        val reset = cacheBefore + misses > maxCache
        if (reset) OpenIvmRocksDBBatchOps.deleteRange(db, b, Cf, FreqPrefix, FreqEnd)
        val retained = if (reset && frequencies.size > maxCache) Vector.empty else frequencies
        retained.foreach { case (k, n) =>
          OpenIvmRocksDBBatchOps.put(db, b, Cf, frequencyKey(k), RocksDBCodec.encodeLongBE(n))
        }
        touchedKeys.foreach(k => OpenIvmRocksDBBatchOps.delete(db, b, Cf, groupKey(k)))
        rows.foreach { case (k, v) => OpenIvmRocksDBBatchOps.put(db, b, Cf, groupKey(k), v.getBytes(UTF_8)) }
        val size = if (reset) retained.size.toLong else cacheBefore + misses
        OpenIvmRocksDBBatchOps.put(db, b, Cf, Header, header(nextEpoch, size))
      }
    }
  }
}

private[spark] object GroupedDistinctState {
  private[common] def collectBounded(frame: DataFrame, limit: Int): Vector[Row] = {
    val query = frame.limit(limit).queryExecution
    SQLExecution.withNewExecutionId(query, Some("collectBounded")) {
      query.executedPlan.resetMetrics()
      val convert = CatalystTypeConverters.createToScalaConverter(frame.schema)
      // Execute the bounded plan as an RDD: CollectLimit gathers its output
      // once instead of executeTake repeatedly probing sparse partitions.
      // Copy rows before collecting because Spark operators reuse UnsafeRows.
      query.toRdd.map(_.copy()).collect().iterator.map(row => convert(row).asInstanceOf[Row]).toVector
    }
  }
  private val CheckpointKey = RocksDBCodec.utf8("grouped_window:v1:checkpoint")
  def checkpoint(spark: SparkSession, view: String): Option[Array[Byte]] = {
    val db = OpenIvmRocksDBRegistry.getOrOpen(
      spark,
      OpenIvmStatePaths.perMvDbPath(spark, view),
      OpenIvmStatePaths.PerMvColumnFamilies
    )
    db.withSession { db.get("default", CheckpointKey) }
  }
}
