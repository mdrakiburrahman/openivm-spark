package org.openivm.spark.compiler

import java.nio.file.Files
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CompilerRuntimeProbeSpec extends AnyFunSuite with Matchers {
  private def withCli(body: String)(check: OpenIvmCompiler => Unit): Unit = {
    val dir = Files.createTempDirectory("compiler-probe-test")
    val cli = dir.resolve("duckdb")
    Files.write(cli, ("#!/bin/sh\ncat >/dev/null\n" + body + "\n").getBytes("UTF-8"))
    cli.toFile.setExecutable(true) shouldBe true
    val compiler = OpenIvmCompiler.build(extensionPath = cli.toString, cliPath = cli.toString)
    try check(compiler)
    finally {
      compiler.close()
      Files.deleteIfExists(cli)
      Files.deleteIfExists(dir)
    }
  }

  test("loader failure is fatal and preserves stderr instead of enabling full refresh") {
    withCli("echo 'libstdc++.so.6: GLIBCXX_3.4.32 not found' >&2; exit 1") { compiler =>
      val error = intercept[IllegalStateException](compiler.verifyRuntime())
      error should not be a[OpenIvmCompileException]
      error.getMessage should include("GLIBCXX_3.4.32")
      error.getMessage should include("refusing full-refresh fallback")
    }
  }

  test("a compiler that returns an incremental projection passes the probe") {
    withCli(
      """echo '{"refresh_type":2,"refresh_type_name":"SIMPLE_PROJECTION","sql":"SELECT id FROM delta_source"}'"""
    ) {
      _.verifyRuntime()
    }
  }

  test("a compiler returning full refresh for the projection fails the probe") {
    withCli("""echo '{"refresh_type":4,"refresh_type_name":"FULL_REFRESH","sql":"SELECT id FROM source"}'""") {
      compiler =>
        intercept[IllegalStateException](compiler.verifyRuntime()).getMessage should include("incremental projection")
    }
  }
}
