package org.openivm.spark.common.rocksdb

import java.io.{File, FileOutputStream, IOException}
import java.nio.file.Files
import java.util.UUID
import java.util.zip.{ZipEntry, ZipOutputStream}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class OpenIvmSnapshotArchiveSpec extends AnyFunSpec with Matchers {
  private def withDirectory(body: File => Unit): Unit = {
    val root = new File(s"target/test-state-archive-${UUID.randomUUID()}")
    root.mkdirs()
    def delete(file: File): Unit = {
      if (file.isDirectory && !Files.isSymbolicLink(file.toPath))
        Option(file.listFiles()).foreach(_.foreach(delete))
      Files.deleteIfExists(file.toPath)
    }
    try body(root)
    finally delete(root)
  }

  private def writeZip(file: File, name: String, contents: Array[Byte]): Unit = {
    val zip = new ZipOutputStream(new FileOutputStream(file))
    try {
      zip.putNextEntry(new ZipEntry(name))
      zip.write(contents)
      zip.closeEntry()
    } finally zip.close()
  }

  describe("packed snapshot archive validation") {
    it("computes exact component identities while packing changed files") {
      withDirectory { root =>
        val source = new File(root, "source")
        source.mkdirs()
        val files = Seq("state-a", "telemetry", "state-b").zipWithIndex.map { case (name, index) =>
          val file = new File(source, name)
          Files.write(file.toPath, Array.fill[Byte](70000 + index)((index + 1).toByte))
          file
        }
        val classify: String => String = name => if (name == "telemetry") "telemetry" else "state"
        val hashes =
          OpenIvmSnapshotArchive.pack(source, files.reverse, new File(root, "snapshot.zip"), _ => (), Some(classify))
        hashes shouldBe files.groupBy(file => classify(file.getName)).map { case (name, members) =>
          name -> OpenIvmSnapshotArchive.contentHash(source, members)
        }
      }
    }

    it("hashes content and names rather than file modification times") {
      withDirectory { root =>
        val file = new File(root, "state")
        Files.write(file.toPath, Array[Byte](1, 2, 3))
        val first = OpenIvmSnapshotArchive.contentHash(root, Seq(file))
        file.setLastModified(123456789000L) shouldBe true
        OpenIvmSnapshotArchive.contentHash(root, Seq(file)) shouldBe first
        Files.write(file.toPath, Array[Byte](4, 5, 6))
        file.setLastModified(123456789000L) shouldBe true
        OpenIvmSnapshotArchive.contentHash(root, Seq(file)) should not be first
        val beforeRename = OpenIvmSnapshotArchive.contentHash(root, Seq(file))
        val moved        = new File(root, "renamed")
        Files.move(file.toPath, moved.toPath)
        val renamed = OpenIvmSnapshotArchive.contentHash(root, Seq(moved))
        renamed should not be beforeRename
      }
    }

    it("extracts only selected components and enforces the shared restore budget") {
      withDirectory { root =>
        val source = new File(root, "source")
        source.mkdirs()
        val state     = new File(source, "state")
        val telemetry = new File(source, "telemetry")
        Files.write(state.toPath, Array[Byte](1, 2))
        Files.write(telemetry.toPath, Array[Byte](3, 4, 5))
        val archive = new File(root, "snapshot.zip")
        OpenIvmSnapshotArchive.pack(source, Seq(state, telemetry), archive, _ => ())
        val restored = new File(root, "restored")
        val used     = OpenIvmSnapshotArchive.extract(archive, restored, _ == "state", 1, 2L)
        used shouldBe (1 -> 2L)
        new File(restored, "telemetry").exists() shouldBe false
        intercept[IOException] {
          OpenIvmSnapshotArchive.extract(archive, restored, _ == "telemetry", 1 - used._1, 5L - used._2)
        }.getMessage should include("entry limit")
        intercept[IOException] {
          OpenIvmSnapshotArchive.extract(archive, restored, _ == "telemetry", 1, 1L)
        }.getMessage should include("size")
      }
    }

    it("round trips state bytes and CURRENT newlines") {
      withDirectory { root =>
        val source    = new File(root, "state")
        val directory = new File(source, "mvs/a/rocksdb")
        directory.mkdirs()
        val file = new File(directory, "CURRENT")
        Files.write(file.toPath, "MANIFEST-1\n".getBytes("UTF-8"))
        val archive = new File(root, "snapshot.zip")
        OpenIvmSnapshotArchive.pack(source, Seq(file), archive, _ => ())
        OpenIvmSnapshotArchive.sha256(archive).length shouldBe 64
        val restored = new File(root, "restored")
        restored.mkdirs()
        OpenIvmSnapshotArchive.extract(archive, restored)
        new String(Files.readAllBytes(new File(restored, "mvs/a/rocksdb/CURRENT").toPath), "UTF-8") shouldBe
          "MANIFEST-1\n"
      }
    }

    it("rejects traversal absolute paths backslashes and directory entries before extraction") {
      withDirectory { root =>
        val archive     = new File(root, "snapshot.zip")
        val destination = new File(root, "restored")
        destination.mkdirs()
        Seq("../escaped", "/absolute", "a/../../escaped", "a\\\\b", "drive:entry", "a//b", "directory/")
          .foreach { name =>
            writeZip(archive, name, Array[Byte](1))
            intercept[IOException](OpenIvmSnapshotArchive.extract(archive, destination))
            intercept[IOException](OpenIvmSnapshotArchive.extract(archive, destination, _ => false))
            destination.listFiles() shouldBe empty
          }
        new File(root, "escaped").exists() shouldBe false
      }
    }

    it("rejects symbolic source files and entry count overflow") {
      withDirectory { root =>
        val actual = new File(root, "actual")
        Files.write(actual.toPath, Array[Byte](1))
        val link = new File(root, "link")
        Files.createSymbolicLink(link.toPath, actual.toPath.toAbsolutePath)
        val archive = new File(root, "snapshot.zip")
        intercept[IOException](
          OpenIvmSnapshotArchive.pack(root, Seq(link), archive, _ => ())
        ).getMessage should include("symbolic")
        intercept[IOException](
          OpenIvmSnapshotArchive.pack(root, Seq.fill(OpenIvmSnapshotArchive.MaxEntries + 1)(actual), archive, _ => ())
        ).getMessage should include("entry limit")
      }
    }

    it("rejects corrupt entry bytes by CRC even when the ZIP directory is readable") {
      withDirectory { root =>
        val archive = new File(root, "snapshot.zip")
        val value   = Array[Byte](11, 22, 33)
        val crc     = new java.util.zip.CRC32()
        crc.update(value)
        val zip = new ZipOutputStream(new FileOutputStream(archive))
        try {
          val entry = new ZipEntry("CURRENT")
          entry.setMethod(ZipEntry.STORED)
          entry.setSize(value.length)
          entry.setCompressedSize(value.length)
          entry.setCrc(crc.getValue)
          zip.putNextEntry(entry)
          zip.write(value)
          zip.closeEntry()
        } finally zip.close()
        val bytes  = Files.readAllBytes(archive.toPath)
        val offset = 30 + "CURRENT".length
        bytes(offset) = (bytes(offset) ^ 1).toByte
        Files.write(archive.toPath, bytes)
        val destination = new File(root, "restored")
        destination.mkdirs()
        intercept[IOException](OpenIvmSnapshotArchive.extract(archive, destination)).getMessage should include(
          "corrupt"
        )
      }
    }

    it("fails interruption without clearing the thread interrupt flag") {
      withDirectory { root =>
        Thread.currentThread().interrupt()
        try {
          intercept[java.io.InterruptedIOException] {
            OpenIvmSnapshotArchive.pack(root, Seq(new File(root, "CURRENT")), new File(root, "snapshot.zip"), _ => ())
          }
          Thread.currentThread().isInterrupted shouldBe true
        } finally Thread.interrupted()
      }
    }
  }
}
