package org.openivm.spark.commands

import java.io.ByteArrayInputStream
import java.nio.file.Files
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class BundledCompilerAssetsSpec extends AnyFunSuite with Matchers {
  private val binaries = Map("duckdb" -> "cli", "openivm.duckdb_extension" -> "extension")

  private def extract(assets: Map[String, String])(check: ((String, String, Option[String])) => Unit): Unit = {
    val dir = Files.createTempDirectory("bundled-compiler-test").toFile
    try {
      val result = OpenIvmCompilers.extractBundledAssets(
        dir,
        name =>
          assets
            .get(name.stripPrefix("/openivm-native/"))
            .map(text => new ByteArrayInputStream(text.getBytes("UTF-8")))
            .orNull
      )
      check(result)
    } finally {
      Option(dir.listFiles()).toSeq.flatten.foreach(_.delete())
      dir.delete()
    }
  }

  test("extract the matching C++ and GCC runtimes beside the executable") {
    extract(binaries ++ Map("libstdc++.so.6" -> "cxx", "libgcc_s.so.1" -> "gcc")) { result =>
      new java.io.File(result._2).canExecute shouldBe true
      val dir = result._3.get
      new String(Files.readAllBytes(java.nio.file.Paths.get(dir, "libstdc++.so.6")), "UTF-8") shouldBe "cxx"
      new String(Files.readAllBytes(java.nio.file.Paths.get(dir, "libgcc_s.so.1")), "UTF-8") shouldBe "gcc"
    }
  }

  test("older bundles without runtime libraries retain the host loader") {
    extract(binaries)(result => result._3 shouldBe None)
  }

  test("an incomplete runtime bundle fails rather than mixing C++ runtimes") {
    intercept[IllegalStateException] {
      extract(binaries + ("libstdc++.so.6" -> "cxx"))(_ => fail("accepted incomplete runtime"))
    }.getMessage should include("libgcc_s.so.1")
  }
}
