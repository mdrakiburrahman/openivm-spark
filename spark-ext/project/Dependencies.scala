import sbt._

object Dependencies {
  val sparkV = "3.5.1"
  val deltaV = "3.2.0"
  val antlrV = "4.9.3"
  // DuckDB JDBC pinned to track openivm's bundled DuckDB v1.5.x.
  // The .duckdb_extension binary is built from openivm@OPENIVM_COMMIT inside
  // the spark-ext Docker image.  The compiler module uses the CLI at
  // /opt/openivm/duckdb (same ABI version as the extension) rather than JDBC.
  val duckdbV = "1.5.2.1"
  val scalaTV = "3.2.18"

  val sparkCore         = "org.apache.spark"              %% "spark-core"              % sparkV   % Provided
  val sparkSql          = "org.apache.spark"              %% "spark-sql"               % sparkV   % Provided
  val sparkHive         = "org.apache.spark"              %% "spark-hive"              % sparkV   % Provided
  val sparkCat          = "org.apache.spark"              %% "spark-catalyst"          % sparkV   % Provided
  val delta             = "io.delta"                      %% "delta-spark"             % deltaV   % Provided
  val duckdb            = "org.duckdb"                     % "duckdb_jdbc"             % duckdbV
  val rocksdb           = "org.rocksdb"                    % "rocksdbjni"              % "8.3.2"
  val antlr             = "org.antlr"                      % "antlr4-runtime"          % antlrV
  val slf4j             = "org.slf4j"                      % "slf4j-api"               % "2.0.12" % Provided
  val scalaTest         = "org.scalatest"                 %% "scalatest"               % scalaTV  % Test
  val scalaMock         = "org.scalamock"                 %% "scalamock"               % "5.2.0"  % Test

  val executor  = Seq(sparkCore, sparkSql, sparkCat, delta, slf4j, scalaTest, scalaMock)
  val common    = Seq(sparkCore, sparkSql, sparkCat, delta, rocksdb, slf4j, scalaTest, scalaMock)
  val compiler  = Seq(sparkSql, sparkCat, slf4j, scalaTest, scalaMock)
  val extension = Seq(sparkCore, sparkSql, sparkCat, sparkHive, delta, antlr, slf4j, scalaTest, scalaMock)
  val it        = Seq(sparkCore, sparkSql, sparkCat, sparkHive, delta, scalaTest)
}
