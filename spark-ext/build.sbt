// build.sbt — root project aggregates 5 sub-modules.
//
// Module dependency graph:
//   ivm-it -> ivm-extension -> ivm-compiler -> ivm-common -> ivm-executor
//
// Cross-module test inheritance via `compile->compile;test->test`.
//
// All Spark / Delta / Hive deps are `% provided` so the assembled jar is
// runtime-pluggable into a spark-shell (`--jars openivm-spark-assembly.jar
// --conf spark.sql.extensions=org.openivm.spark.OpenIvmSparkExtensions`).

import Dependencies._
import Settings._

val openIvmExtensionPath =
  sys.env.getOrElse("OPENIVM_EXTENSION_PATH", "/opt/openivm/openivm.duckdb_extension")
val openIvmCliPath = sys.env.getOrElse("OPENIVM_CLI_PATH", "/opt/openivm/duckdb")

ThisBuild / scalaVersion := "2.12.17"
ThisBuild / version      := "0.1.0-SNAPSHOT"
ThisBuild / organization := "org.openivm"

ThisBuild / javacOptions ++= Seq("--release", "11")
ThisBuild / Test / fork := true
ThisBuild / Test / javaOptions ++= jvmModuleOpts
ThisBuild / Test / parallelExecution := false
ThisBuild / Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oDF")

// Cap parallel forked JVMs. Override with `-Dopenivm.test.forks=N`.
Global / concurrentRestrictions := Seq(
  Tags.limit(Tags.ForkedTestGroup, sys.props.getOrElse("openivm.test.forks", "32").toInt)
)

lazy val root = (project in file("."))
  .aggregate(ivmExecutor, ivmCommon, ivmCompiler, ivmExtension, ivmIt)
  .settings(
    name           := "openivm-spark",
    publish / skip := true
  )

lazy val ivmExecutor = (project in file("ivm-executor"))
  .settings(commonSettings: _*)
  .settings(libraryDependencies ++= Dependencies.executor)

lazy val ivmCommon = (project in file("ivm-common"))
  .dependsOn(ivmExecutor % "compile->compile;test->test")
  .settings(commonSettings: _*)
  .settings(libraryDependencies ++= Dependencies.common)

lazy val ivmCompiler = (project in file("ivm-compiler"))
  .dependsOn(ivmCommon % "compile->compile;test->test")
  .settings(commonSettings: _*)
  .settings(libraryDependencies ++= Dependencies.compiler)

lazy val ivmExtension = (project in file("ivm-extension"))
  .dependsOn(ivmCompiler % "compile->compile;test->test")
  .enablePlugins(Antlr4Plugin)
  .settings(commonSettings: _*)
  .settings(assemblySettings: _*)
  .settings(libraryDependencies ++= Dependencies.extension)
  .settings(
    Antlr4 / antlr4PackageName := Some("org.openivm.spark.parser.gen"),
    Antlr4 / antlr4Version     := antlrV,
    Antlr4 / antlr4GenListener := true,
    Antlr4 / antlr4GenVisitor  := true
  )
  .settings(
    Test / envVars ++= Map(
      "OPENIVM_EXTENSION_PATH" -> openIvmExtensionPath,
      "OPENIVM_CLI_PATH"       -> openIvmCliPath
    )
  )
  .settings(
    // Bake the DuckDB CLI + OpenIVM extension into the assembly JAR.
    Compile / resourceGenerators += Def.task {
      sys.env.get("OPENIVM_NATIVE_DIR").filter(_.nonEmpty) match {
        case None => Seq.empty[File]
        case Some(dir) =>
          val outDir = (Compile / resourceManaged).value / "openivm-native"
          IO.createDirectory(outDir)
          Seq("duckdb", "openivm.duckdb_extension").flatMap { name =>
            val src = file(dir) / name
            if (!src.exists())
              sys.error(s"OPENIVM_NATIVE_DIR set but missing native binary: $src")
            val dst = outDir / name
            IO.copyFile(src, dst, preserveLastModified = true)
            Seq(dst)
          }
      }
    }.taskValue
  )

lazy val ivmIt = (project in file("ivm-it"))
  .dependsOn(ivmExtension % "compile->compile;test->test")
  .settings(commonSettings: _*)
  .settings(parallelForkSettings: _*)
  .settings(libraryDependencies ++= Dependencies.it)
  .settings(
    Test / test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-l", "org.openivm.spark.tags.MicroBenchmark")
  )
  .settings(publish / skip := true)
