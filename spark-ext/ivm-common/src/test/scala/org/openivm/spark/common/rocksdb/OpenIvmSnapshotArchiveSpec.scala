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
