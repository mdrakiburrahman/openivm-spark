import sbt._

object Dependencies {
  private val target = RuntimeTarget.current

  val sparkV = target.sparkVersion
  val deltaV = target.deltaVersion
  val antlrV = target.antlrVersion
  // DuckDB JDBC pinned to track openivm's bundled DuckDB v1.5.x.
  // The .duckdb_extension binary is built from openivm@OPENIVM_COMMIT inside
  // the spark-ext Docker image.  The compiler module uses the CLI at
  // /opt/openivm/duckdb (same ABI version as the extension) rather than JDBC.
  val duckdbV = "1.5.2.1"
  val scalaTV = "3.2.18"

  val sparkCore = "org.apache.spark" %% "spark-core"         % sparkV   % Provided
  val sparkSql  = "org.apache.spark" %% "spark-sql"          % sparkV   % Provided
  val sparkHive = "org.apache.spark" %% "spark-hive"         % sparkV   % Provided
  val sparkCat  = "org.apache.spark" %% "spark-catalyst"     % sparkV   % Provided
  val delta     = "io.delta"         %% target.deltaArtifact % deltaV   % Provided
  val duckdb    = "org.duckdb"        % "duckdb_jdbc"        % duckdbV
  val rocksdb   = "org.rocksdb"       % "rocksdbjni"         % target.rocksDbVersion
  val antlr     = "org.antlr"         % "antlr4-runtime"     % antlrV
  val slf4j     = "org.slf4j"         % "slf4j-api"          % "2.0.12" % Provided
  val collectionCompat =
    "org.scala-lang.modules" %% "scala-collection-compat" % "2.13.0"
  val scalaTest = "org.scalatest" %% "scalatest" % scalaTV % Test
  val scalaMock = "org.scalamock" %% "scalamock" % "5.2.0" % Test

  val executor = Seq(sparkCore, sparkSql, sparkCat, delta, slf4j, collectionCompat, scalaTest, scalaMock)
  val common =
    Seq(sparkCore, sparkSql, sparkCat, delta, rocksdb, slf4j, collectionCompat, scalaTest, scalaMock)
  val compiler = Seq(sparkSql, sparkCat, slf4j, collectionCompat, scalaTest, scalaMock)
  val extension =
    Seq(sparkCore, sparkSql, sparkCat, sparkHive, delta, antlr, slf4j, collectionCompat, scalaTest, scalaMock)
  val it = Seq(sparkCore, sparkSql, sparkCat, sparkHive, delta, collectionCompat, scalaTest)
}
