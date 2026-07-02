package org.openivm.spark.common

import java.io.InputStream
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.spark.SparkConf

final case class RefreshCostFeatures(
    recordsRead: Double = 0.0d,
    recordsWritten: Double = 0.0d,
    filesScanned: Double = 0.0d,
    shuffleReadBytes: Double = 0.0d,
    shuffleWriteBytes: Double = 0.0d,
    spillMemoryBytes: Double = 0.0d,
    spillDiskBytes: Double = 0.0d,
    refreshType: Int = RefreshTypeCode.FullRefresh
)

final case class RefreshCostCoefficients(
    intercept: Double,
    weights: Map[String, Double],
    r2: Option[Double] = None,
    ridgeAlpha: Option[Double] = None
) {
  def weight(name: String): Double = weights.getOrElse(name, 0.0d)
}

final case class RefreshCostModel(coefficients: RefreshCostCoefficients) {
  import RefreshCostModel._

  def predictWallClockMs(features: RefreshCostFeatures): Double =
    math.max(
      0.0d,
      coefficients.intercept + FeatureNames.map(name => coefficients.weight(name) * featureValue(name, features)).sum
    )

  def predictWallClockMs(refreshType: Int, facts: WorkloadFacts): Double =
    predictWallClockMs(RefreshCostFeatures.fromWorkloadFacts(refreshType, facts))
}

object RefreshCostFeatures {
  def fromWorkloadFacts(refreshType: Int, facts: WorkloadFacts): RefreshCostFeatures = {
    val deltaRows  = facts.deltaStats.values.flatMap(_.rowCount).sum.toDouble
    val tableRows  = facts.tableStats.values.flatMap(_.rowCount).sum.toDouble
    val deltaFiles = facts.deltaStats.values.flatMap(_.numFiles).sum.toDouble
    val tableFiles = facts.tableStats.values.flatMap(_.numFiles).sum.toDouble

    RefreshCostFeatures(
      recordsRead = if (deltaRows > 0.0d) deltaRows else tableRows,
      recordsWritten = deltaRows,
      filesScanned = if (deltaFiles > 0.0d) deltaFiles else tableFiles,
      refreshType = refreshType
    )
  }
}

object RefreshCostModel {
  val CalibratedEnabledKey    = "spark.openivm.refresh.costModel.calibrated.enabled"
  val CoefficientsPathKey     = "spark.openivm.refresh.costModel.coefficients.path"
  val CoefficientsResourceKey = "spark.openivm.refresh.costModel.coefficients.resource"

  val ContinuousFeatureNames: Seq[String] = Seq(
    "records_read",
    "records_written",
    "files_scanned",
    "shuffle_read_bytes",
    "shuffle_write_bytes",
    "spill_memory_bytes",
    "spill_disk_bytes"
  )

  val RefreshTypeFeatureNames: Seq[String] = (0 to 9).map(code => s"refresh_type_$code")
  val FeatureNames: Seq[String]            = ContinuousFeatureNames ++ RefreshTypeFeatureNames

  val DefaultCoefficients: RefreshCostCoefficients = RefreshCostCoefficients(
    intercept = 1000.0d,
    weights = Map(
      "records_read"        -> 0.0005d,
      "records_written"     -> 0.002d,
      "files_scanned"       -> 25.0d,
      "shuffle_read_bytes"  -> 0.000001d,
      "shuffle_write_bytes" -> 0.000001d,
      "spill_memory_bytes"  -> 0.000002d,
      "spill_disk_bytes"    -> 0.000004d,
      "refresh_type_0"      -> 500.0d,
      "refresh_type_1"      -> 250.0d,
      "refresh_type_2"      -> 100.0d,
      "refresh_type_3"      -> 1000.0d,
      "refresh_type_4"      -> 750.0d,
      "refresh_type_5"      -> 2000.0d,
      "refresh_type_6"      -> 1500.0d,
      "refresh_type_7"      -> 800.0d,
      "refresh_type_8"      -> 1000.0d,
      "refresh_type_9"      -> 1200.0d
    )
  )

  val Default: RefreshCostModel = RefreshCostModel(DefaultCoefficients)

  private val Json = new ObjectMapper()

  def fromConf(conf: SparkConf): RefreshCostModel = {
    val enabled = conf.getOption(CalibratedEnabledKey).exists(_.toBoolean)
    if (!enabled) Default
    else {
      conf
        .getOption(CoefficientsPathKey)
        .map(loadFromPath)
        .orElse(conf.getOption(CoefficientsResourceKey).flatMap(loadFromResource))
        .getOrElse(Default)
    }
  }

  def loadFromPath(path: String): RefreshCostModel =
    fromJson(new String(Files.readAllBytes(Paths.get(path)), StandardCharsets.UTF_8))

  def loadFromResource(resource: String): Option[RefreshCostModel] = {
    val name   = resource.stripPrefix("/")
    val loader = Option(Thread.currentThread().getContextClassLoader).getOrElse(getClass.getClassLoader)
    Option(loader.getResourceAsStream(name)).map { in =>
      try fromJson(readAll(in))
      finally in.close()
    }
  }

  private[common] def fromJson(json: String): RefreshCostModel = {
    val root        = Json.readTree(json)
    val weightsNode = root.path("weights")
    val weights = weightsNode
      .fields()
      .asScala
      .map(entry => entry.getKey -> entry.getValue.asDouble())
      .toMap
    RefreshCostModel(
      RefreshCostCoefficients(
        intercept = root.path("intercept").asDouble(0.0d),
        weights = weights,
        r2 = optionalDouble(root, "r2"),
        ridgeAlpha = optionalDouble(root, "ridge_alpha")
      )
    )
  }

  private[common] def featureValue(name: String, features: RefreshCostFeatures): Double = name match {
    case "records_read"        => features.recordsRead
    case "records_written"     => features.recordsWritten
    case "files_scanned"       => features.filesScanned
    case "shuffle_read_bytes"  => features.shuffleReadBytes
    case "shuffle_write_bytes" => features.shuffleWriteBytes
    case "spill_memory_bytes"  => features.spillMemoryBytes
    case "spill_disk_bytes"    => features.spillDiskBytes
    case refreshType if refreshType.startsWith("refresh_type_") =>
      if (refreshType.stripPrefix("refresh_type_").toInt == features.refreshType) 1.0d else 0.0d
    case _ => 0.0d
  }

  private def optionalDouble(root: JsonNode, field: String): Option[Double] = {
    val value = root.path(field)
    if (value.isMissingNode || value.isNull) None else Some(value.asDouble())
  }

  private def readAll(in: InputStream): String = {
    val buffer = new Array[Byte](8192)
    val out    = new java.io.ByteArrayOutputStream()
    var n      = in.read(buffer)
    while (n >= 0) {
      out.write(buffer, 0, n)
      n = in.read(buffer)
    }
    new String(out.toByteArray, StandardCharsets.UTF_8)
  }
}
