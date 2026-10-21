package org.openivm.spark.commands

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.util.UUID
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit, raise_error, when}
import org.apache.spark.sql.types.{DataType, DecimalType, DoubleType, IntegerType, LongType, StructType}
import org.apache.spark.storage.StorageLevel
import org.openivm.spark.common._
import org.openivm.spark.compiler.{CompileRequest, OpenIvmCompileException, OpenIvmCompiler}
import org.openivm.spark.parser.GroupedWindowStagePlan

/** Optional grouped state before ranking. The command still owns target writes,
  * admission, pinned-operation guards, cascade and source-watermark publication.
  * This only stages a derived cache/checkpoint in the existing MV shard.
  */
private[commands] object GroupedWindowStatePlan extends Logging {
  private val ProgramKey       = "_ivm_grouped_window_input_v1"
  private val NativeView       = "openivm_grouped_input"
  private def quote(n: String) = "`" + n.replace("`", "``") + "`"
  private case class Source(name: String, path: String, id: String, version: Long, schema: StructType)
  private case class Target(path: String, id: String, version: Long, schema: StructType)
  private case class Checkpoint(sources: Vector[Source], target: Target)
  private def pathOf(path: org.apache.hadoop.fs.Path) = path.toUri.normalize.toString.stripSuffix("/")
  private def target(spark: SparkSession, path: String): Target = {
    val log = DeltaTableVersion
      .deltaLogOption(spark, path)
      .getOrElse(throw new IllegalArgumentException("missing grouped-window target identity"))
    val snapshot = log.update()
    Target(pathOf(log.dataPath), snapshot.metadata.id, snapshot.version, snapshot.metadata.schema)
  }
  private def aliases(spark: SparkSession, sources: Seq[String]): Map[String, String] = {
    val entries = sources.flatMap { source =>
      val parts = spark.sessionState.sqlParser.parseMultipartIdentifier(source)
      (1 to parts.size).map(size => parts.takeRight(size).map(quote).mkString(".") -> source)
    }
    require(
      entries.groupBy(_._1.toLowerCase(java.util.Locale.ROOT)).values.forall(_.map(_._2).distinct.size == 1),
      "ambiguous grouped-window source alias"
    )
    entries.toMap
  }
  private def rejected(t: Throwable): None.type = {
    logDebug(s"Grouped-window state unavailable: ${t.getMessage}"); None
  }
  private def unsupported[A](body: => A): Option[A] = try { Some(body) }
  catch {
    case t: IllegalArgumentException => rejected(t)
    case t: AnalysisException        => rejected(t)
    case t: OpenIvmCompileException  => rejected(t)
  }
  private def capture(
      spark: SparkSession,
      names: Seq[String],
      schemas: Map[String, StructType],
      versions: Map[String, Long]
  ): Vector[Source] =
    names.sorted.map { name =>
      val log = DeltaTableVersion
        .deltaLogOption(spark, name)
        .getOrElse(throw new IllegalArgumentException("source is not a physical Delta relation"))
      val current = log.update(); val version = versions(name)
      require(version >= 0 && current.version >= version, "invalid grouped-window source bound")
      Source(name, pathOf(log.dataPath), current.metadata.id, version, schemas(name))
    }.toVector
  private def verify(spark: SparkSession, sources: Vector[Source]): Unit = sources.foreach { s =>
    val log = DeltaTableVersion
      .deltaLogOption(spark, s.name)
      .getOrElse(throw new IllegalStateException("grouped-window source disappeared"))
    val current = log.update()
    require(
      pathOf(log.dataPath) == s.path && current.metadata.id == s.id && current.version >= s.version,
      "grouped-window source identity changed"
    )
  }
  private def frames(spark: SparkSession, sources: Vector[Source]): Map[String, DataFrame] = sources.map { s =>
    val raw = spark.read.format("delta").option("versionAsOf", s.version).load(s.path)
    s.name -> MvProjectionSource.project(raw, s.schema)
  }.toMap
  private def align(df: DataFrame, schema: StructType): DataFrame = {
    require(
      df.columns.length == schema.length && df.columns
        .map(_.toLowerCase(java.util.Locale.ROOT))
        .distinct
        .length == df.columns.length,
      "grouped-window target fields differ"
    )
    df.select(schema.fields.toIndexedSeq.map { f =>
      val matches = df.schema.fields.filter(_.name.equalsIgnoreCase(f.name))
      require(matches.length == 1, "grouped-window target field is missing or ambiguous")
      val actual        = matches.head.dataType; val value = col(quote(matches.head.name))
      val decimalDouble = actual.isInstanceOf[DecimalType] && f.dataType == DoubleType
      require(
        actual == f.dataType || (actual == IntegerType && f.dataType == LongType) || decimalDouble,
        "grouped-window target field type differs"
      )
      val cast = value.cast(f.dataType)
      val result =
        if (!decimalDouble) cast
        else
          when(value.eqNullSafe(cast.cast(actual)), cast)
            .otherwise(raise_error(lit("grouped-window target cannot preserve decimal result")))
            .cast(f.dataType)
      result.as(f.name)
    }: _*)
  }
  private def encode(c: Checkpoint): Array[Byte] = {
    val bytes = new ByteArrayOutputStream(); val out = new DataOutputStream(bytes)
    out.writeInt(1); out.writeUTF(c.target.path); out.writeUTF(c.target.id); out.writeLong(c.target.version)
    out.writeUTF(c.target.schema.json); out.writeInt(c.sources.size)
    c.sources.foreach { s =>
      out.writeUTF(s.name); out.writeUTF(s.path); out.writeUTF(s.id); out.writeLong(s.version);
      out.writeUTF(s.schema.json)
    }
    out.close(); bytes.toByteArray
  }
  private def decode(bytes: Array[Byte]): Checkpoint = {
    val in = new DataInputStream(new ByteArrayInputStream(bytes));
    require(in.readInt() == 1, "unknown grouped-window checkpoint")
    val target =
      Target(in.readUTF(), in.readUTF(), in.readLong(), DataType.fromJson(in.readUTF()).asInstanceOf[StructType])
    val n = in.readInt(); require(n > 0 && n <= 64, "invalid grouped-window source vector")
    val sources = Vector.fill(n) {
      Source(
        in.readUTF(),
        in.readUTF(),
        in.readUTF(),
        in.readLong(),
        DataType.fromJson(in.readUTF()).asInstanceOf[StructType]
      )
    }
    require(in.available() == 0 && sources.map(_.name).distinct.size == n, "invalid grouped-window checkpoint")
    Checkpoint(sources, target)
  }

  final class Plan private[GroupedWindowStatePlan] (
      spark: SparkSession,
      view: String,
      names: Seq[String],
      schemas: Map[String, StructType],
      val programSql: String,
      contract: GroupedWindowStagePlan.Contract
  ) {
    val properties: Map[String, String] = Map(ProgramKey -> programSql)
    private val native                  = GroupedWindowStagePlan.inputDelta(spark, programSql, NativeView)
    private def state(input: DataFrame, summarySchema: StructType) = {
      new GroupedDistinctState(
        spark,
        view,
        contract.identity + "\n" + native.identity,
        input.schema,
        summarySchema,
        contract.groupColumns,
        contract.countName,
        contract.channels.map(c => c.input -> c.output)
      )
    }
    def initial(versions: Map[String, Long], targetSchema: StructType): Option[Initial] = unsupported {
      val pins = capture(spark, names, schemas, versions); val current = frames(spark, pins)
      encode(Checkpoint(pins, Target("", "", 0L, targetSchema)))
      val input = contract.input(current)
      val summary = contract.summary(current).persist(StorageLevel.MEMORY_AND_DISK)
      try {
        val cache = state(input, summary.schema); val seed = cache.prepareBootstrap(summary, "0")
        val rows  = align(contract.post(current, summary), targetSchema)
        new Initial(
          rows,
          () => { summary.unpersist(blocking = true); () },
          pins,
          (path, version) => {
            verify(spark, pins)
            val after = target(spark, path);
            require(after.version == version, "target changed before grouped-state publication")
            // An extra target commit (e.g. compaction) leaves this derived cache
            // cold, as the existing running-window cache does on version gaps.
            if (version == 0L) seed.publish(encode(Checkpoint(pins, after)))
          }
        )
      } catch { case t: Throwable => summary.unpersist(blocking = true); throw t }
    }
    def refresh(
        meta: MvMetadata,
        batches: Seq[CdfChangeBatch],
        deltaFrames: Map[String, DataFrame]
    ): Option[Refresh] = {
      val before = target(spark, meta.location)
      val cached = unsupported { GroupedDistinctState.checkpoint(spark, view).map(decode) }.flatten
      logDebug(
        s"Grouped-window cache view=$view checkpoint=${cached.nonEmpty} target=${cached.exists(_.target == before)} " +
          s"version=${before.version}/${meta.lastVersion} sources=${cached.exists(_.sources.map(_.name) == names.sorted.toVector)} " +
          s"schemas=${cached.exists(_.sources.forall(s => schemas.get(s.name).contains(s.schema)))}"
      )
      cached
        .filter(c =>
          c.target == before && before.version == meta.lastVersion &&
            c.sources.map(_.name) == names.sorted.toVector && c.sources.forall(s =>
              schemas.get(s.name).contains(s.schema)
            )
        )
        .flatMap { c =>
          val changed = batches.groupBy(_.baseTable)
          if (
            changed.values.exists(_.size != 1) || !changed.keySet.subsetOf(names.toSet) ||
            c.sources.exists(s => changed.get(s.name).exists(_.head.startVersionExclusive != s.version))
          ) None
          else {
            val versions = c.sources.map { s =>
              s.name -> changed.get(s.name).map(_.head.endVersionInclusive).getOrElse(s.version)
            }.toMap
            val pins = capture(spark, names, schemas, versions)
            if (pins.zip(c.sources).exists { case (a, b) => a.path != b.path || a.id != b.id }) None
            else {
              val current = frames(spark, pins); val input = contract.input(current)
              val cache = state(input, contract.summary(current).schema)
              unsupported { cache.cachedSize(before.version.toString) }.flatMap { _ =>
                val bases = names.flatMap { name =>
                  val short = spark.sessionState.sqlParser.parseMultipartIdentifier(name).last
                  Vector(
                    short                      -> current(name),
                    ("memory.main." + short)   -> current(name),
                    ("openivm_delta_" + short) -> deltaFrames(name)
                  )
                }.toMap
                val complete =
                  cache.prepare(native(bases), input, before.version.toString)(contract.ordinary)
                val rows = align(contract.post(current, complete.summary), before.schema)
                encode(Checkpoint(pins, before)) // encoding failures precede target mutation
                verify(spark, pins)
                Some(
                  new Refresh(
                    rows,
                    complete.hits,
                    complete.misses,
                    complete.lookupStrategy,
                    () => {
                      verify(spark, pins);
                      require(target(spark, meta.location) == before, "grouped-window target changed before write")
                    },
                    version => {
                      verify(spark, pins)
                      val after = target(spark, meta.location);
                      require(
                        after.version == version && after.id == before.id && after.path == before.path,
                        "grouped-window target changed during publication"
                      )
                      if (version == before.version + 1L)
                        complete.publish(version.toString, encode(Checkpoint(pins, after)))
                    }
                  )
                )
              }
            }
          }
        }
    }
  }
  final class Initial private[GroupedWindowStatePlan] (
      val rows: DataFrame,
      cleanup: () => Unit,
      private val pins: Vector[Source],
      publish: (String, Long) => Unit
  ) {
    def verifySources(spark: SparkSession): Unit  = verify(spark, pins)
    def commit(path: String, version: Long): Unit = publish(path, version)
    def close(): Unit                             = { cleanup(); () }
  }
  final class Refresh private[GroupedWindowStatePlan] (
      val rows: DataFrame,
      val hits: Int,
      val misses: Int,
      val lookupStrategy: String,
      preflight: () => Unit,
      publish: Long => Unit
  ) {
    val temporaryView: String = "openivm_grouped_result_" + UUID.randomUUID().toString.replace("-", "")
    def register(): String = {
      preflight(); rows.createOrReplaceTempView(temporaryView); s"SELECT * FROM ${quote(temporaryView)}"
    }
    def commit(version: Long): Unit = publish(version)
  }
  def create(
      spark: SparkSession,
      view: String,
      query: String,
      sources: Seq[String],
      schemas: Map[String, StructType],
      compiler: OpenIvmCompiler
  ): Option[Plan] = unsupported {
    require(sources.nonEmpty && sources.size <= 64, "bounded grouped-window source vector required")
    val sourceAliases = aliases(spark, sources)
    val contract      = GroupedWindowStagePlan.extract(spark, query, sources.toVector, sourceAliases)
    val compiled = compiler.compile(
      CompileRequest(
        NativeView,
        contract.inputSql,
        sources.map(n => n.split("\\.").last -> schemas(n)).toMap,
        sourceQualifiedNames = sources.map(n => n.split("\\.").last -> n).toMap
      )
    )
    require(compiled.refreshType == RefreshTypeCode.SimpleProjection, "signed grouped input required")
    new Plan(spark, view, sources, schemas, compiled.sql, contract)
  }
  def load(spark: SparkSession, view: String, meta: MvMetadata, schemas: Map[String, StructType]): Option[Plan] =
    meta.properties.get(ProgramKey).flatMap { program =>
      unsupported {
        val contract = GroupedWindowStagePlan
          .extract(spark, meta.querySql, meta.sourceTables.toVector, aliases(spark, meta.sourceTables))
        new Plan(spark, view, meta.sourceTables, schemas, program, contract)
      }
    }
}
