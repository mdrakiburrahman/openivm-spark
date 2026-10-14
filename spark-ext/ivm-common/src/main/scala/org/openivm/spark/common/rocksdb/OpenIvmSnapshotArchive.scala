package org.openivm.spark.common.rocksdb

import java.io.{BufferedInputStream, BufferedOutputStream, File, FileInputStream, FileOutputStream, IOException}
import java.nio.file.Files
import java.security.MessageDigest
import java.util.zip.{CRC32, ZipEntry, ZipFile, ZipOutputStream}
import scala.jdk.CollectionConverters._

/** Bounded immutable state archives, verified before recovery is published. */
private[rocksdb] object OpenIvmSnapshotArchive {
  val MaxEntries: Int = 100000
  val MaxBytes: Long  = 64L * 1024L * 1024L * 1024L

  def checkInterrupted(): Unit =
    if (Thread.currentThread().isInterrupted)
      throw new java.io.InterruptedIOException("openivm state-sync: interrupted archive operation")

  private def safeName(name: String): Unit =
    if (
      name.isEmpty || name.startsWith("/") || name.contains("\\") ||
      name.contains(":") || name.split("/", -1).exists(part => part.isEmpty || part == "." || part == "..")
    )
      throw new IOException(s"openivm state-sync: unsafe archive entry '$name'")

  def pack(root: File, files: Seq[File], archive: File, beforeCopy: File => Unit): Unit = {
    if (files.size > MaxEntries) throw new IOException("openivm state-sync: archive entry limit exceeded")
    val output = new ZipOutputStream(new BufferedOutputStream(new FileOutputStream(archive)))
    var total  = 0L
    try
      files.sortBy(_.toString).foreach { file =>
        checkInterrupted()
        beforeCopy(file)
        if (Files.isSymbolicLink(file.toPath)) throw new IOException("openivm state-sync: symbolic state file")
        val name = root.toPath.relativize(file.toPath).iterator().asScala.map(_.toString).mkString("/")
        safeName(name)
        output.putNextEntry(new ZipEntry(name))
        val input  = new BufferedInputStream(new FileInputStream(file))
        val buffer = new Array[Byte](65536)
        try {
          var size = input.read(buffer)
          while (size >= 0) {
            checkInterrupted()
            total += size
            if (total > MaxBytes) throw new IOException("openivm state-sync: archive size limit exceeded")
            output.write(buffer, 0, size)
            size = input.read(buffer)
          }
        } finally input.close()
        output.closeEntry()
      }
    finally output.close()
    if (archive.length() > MaxBytes) throw new IOException("openivm state-sync: archive size limit exceeded")
  }

  def sha256(file: File): String = {
    val digest = MessageDigest.getInstance("SHA-256")
    val input  = new BufferedInputStream(new FileInputStream(file))
    val buffer = new Array[Byte](65536)
    try {
      var size = input.read(buffer)
      while (size >= 0) {
        checkInterrupted()
        digest.update(buffer, 0, size)
        size = input.read(buffer)
      }
    } finally input.close()
    digest.digest().map(byte => f"${byte & 0xff}%02x").mkString
  }

  def extract(archive: File, destination: File): Unit = {
    if (archive.length() > MaxBytes) throw new IOException("openivm state-sync: archive size limit exceeded")
    val zip = new ZipFile(archive)
    try {
      val entries = zip.entries().asScala.take(MaxEntries + 1).toVector
      if (entries.size > MaxEntries) throw new IOException("openivm state-sync: archive entry limit exceeded")
      val names    = scala.collection.mutable.Set.empty[String]
      var declared = 0L
      entries.foreach { entry =>
        safeName(entry.getName)
        if (
          entry.isDirectory || !names.add(entry.getName) || entry.getSize < 0 ||
          entry.getSize > MaxBytes - declared
        )
          throw new IOException("openivm state-sync: invalid archive entries or size")
        declared += entry.getSize
      }
      var total = 0L
      entries.foreach { entry =>
        val file = new File(destination, entry.getName)
        if (!file.toPath.normalize().startsWith(destination.toPath.normalize()))
          throw new IOException("openivm state-sync: archive path escapes destination")
        Files.createDirectories(file.getParentFile.toPath)
        val input    = zip.getInputStream(entry)
        val output   = new BufferedOutputStream(new FileOutputStream(file))
        val checksum = new CRC32()
        val buffer   = new Array[Byte](65536)
        var copied   = 0L
        try {
          var size = input.read(buffer)
          while (size >= 0) {
            checkInterrupted()
            copied += size
            total += size
            if (total > MaxBytes || copied > entry.getSize)
              throw new IOException("openivm state-sync: expanded archive size limit exceeded")
            checksum.update(buffer, 0, size)
            output.write(buffer, 0, size)
            size = input.read(buffer)
          }
        } finally {
          try output.close()
          finally input.close()
        }
        if (copied != entry.getSize || checksum.getValue != entry.getCrc)
          throw new IOException("openivm state-sync: corrupt archive entry")
      }
    } finally zip.close()
  }
}
