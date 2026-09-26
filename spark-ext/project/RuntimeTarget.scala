final case class RuntimeTarget(
    id: String,
    sourceSuffix: String,
    scalaVersion: String,
    sparkVersion: String,
    deltaVersion: String,
    deltaArtifact: String,
    moduleName: String,
    javaVersion: String,
    rocksDbVersion: String,
    antlrVersion: String
)

object RuntimeTarget {

  private val Spark35 = RuntimeTarget(
    id = "spark-3.5",
    sourceSuffix = "spark-3.5",
    scalaVersion = "2.12.17",
    sparkVersion = "3.5.1",
    deltaVersion = "3.2.0",
    deltaArtifact = "delta-spark",
    moduleName = "ivmextension-spark-3.5",
    javaVersion = "17",
    rocksDbVersion = "8.3.2",
    antlrVersion = "4.9.3"
  )

  private val Spark41 = RuntimeTarget(
    id = "spark-4.1",
    sourceSuffix = "spark-4.1",
    scalaVersion = "2.13.17",
    sparkVersion = "4.1.0",
    deltaVersion = "4.2.0",
    deltaArtifact = "delta-spark_4.1",
    moduleName = "ivmextension-spark-4.1",
    javaVersion = "21",
    rocksDbVersion = "8.3.2",
    antlrVersion = "4.13.1"
  )

  private val targets = Seq(Spark35, Spark41).map(target => target.id -> target).toMap

  val current: RuntimeTarget = {
    val id = sys.env.getOrElse("OPENIVM_SPARK_TARGET", Spark35.id)
    targets.getOrElse(
      id,
      sys.error(
        s"Unsupported OPENIVM_SPARK_TARGET '$id'. Expected one of: ${targets.keys.toSeq.sorted.mkString(", ")}"
      )
    )
  }
}
