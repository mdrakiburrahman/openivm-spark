package org.openivm.spark.common.rocksdb

import java.io.{BufferedInputStream, BufferedOutputStream, File, FileInputStream, FileOutputStream, IOException}
import java.nio.file.Files
import java.nio.ByteBuffer
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

  private def hashHeader(digest: MessageDigest, name: String, length: Long): Unit = {
    val bytes = name.getBytes(java.nio.charset.StandardCharsets.UTF_8)
    digest.update(ByteBuffer.allocate(4 + bytes.length + 8).putInt(bytes.length).put(bytes).putLong(length).array())
  }

  private def hex(digest: MessageDigest): String = digest.digest().map(byte => f"${byte & 0xff}%02x").mkString

  def pack(
      root: File,
      files: Seq[File],
      archive: File,
      beforeCopy: File => Unit,
      componentOf: Option[String => String] = None
  ): Map[String, String] = {
    if (files.size > MaxEntries) throw new IOException("openivm state-sync: archive entry limit exceeded")
    val output = new ZipOutputStream(new BufferedOutputStream(new FileOutputStream(archive)))
    val hashes = scala.collection.mutable.Map.empty[String, MessageDigest]
    val buffer = new Array[Byte](65536)
    var total  = 0L
    try
      files.sortBy(_.toString).foreach { file =>
        checkInterrupted()
        beforeCopy(file)
        if (Files.isSymbolicLink(file.toPath)) throw new IOException("openivm state-sync: symbolic state file")
        val name = root.toPath.relativize(file.toPath).iterator().asScala.map(_.toString).mkString("/")
        safeName(name)
        val digest = componentOf.map { classify =>
          val hash = hashes.getOrElseUpdate(classify(name), MessageDigest.getInstance("SHA-256"))
          hashHeader(hash, name, file.length())
          hash
        }
        val entry = new ZipEntry(name)
        entry.setTime(0L)
        output.putNextEntry(entry)
        val input = new BufferedInputStream(new FileInputStream(file))
        try {
          var size = input.read(buffer)
          while (size >= 0) {
            checkInterrupted()
            total += size
            if (total > MaxBytes) throw new IOException("openivm state-sync: archive size limit exceeded")
            output.write(buffer, 0, size)
            digest.foreach(_.update(buffer, 0, size))
            size = input.read(buffer)
          }
        } finally input.close()
        output.closeEntry()
      }
    finally output.close()
    if (archive.length() > MaxBytes) throw new IOException("openivm state-sync: archive size limit exceeded")
    hashes.iterator.map { case (name, digest) => name -> hex(digest) }.toMap
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
    hex(digest)
  }

  /** Hash state content independently of file timestamps and ZIP layout. */
  def contentHash(root: File, files: Seq[File]): String = {
    if (files.size > MaxEntries) throw new IOException("openivm state-sync: archive entry limit exceeded")
    val digest = MessageDigest.getInstance("SHA-256")
    val buffer = new Array[Byte](65536)
    var total  = 0L
    files.sortBy(_.toString).foreach { file =>
      checkInterrupted()
      if (Files.isSymbolicLink(file.toPath)) throw new IOException("openivm state-sync: symbolic state file")
      val name = root.toPath.relativize(file.toPath).iterator().asScala.map(_.toString).mkString("/")
      safeName(name)
      hashHeader(digest, name, file.length())
      val input = new BufferedInputStream(new FileInputStream(file))
      try {
        var size = input.read(buffer)
        while (size >= 0) {
          checkInterrupted()
          total += size
          if (total > MaxBytes) throw new IOException("openivm state-sync: archive size limit exceeded")
          digest.update(buffer, 0, size)
          size = input.read(buffer)
        }
      } finally input.close()
    }
    hex(digest)
  }

  def extract(
      archive: File,
      destination: File,
      include: String => Boolean = _ => true,
      remainingEntries: Int = MaxEntries,
      remainingBytes: Long = MaxBytes
  ): (Int, Long) = {
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
      val selected = entries.filter(entry => include(entry.getName))
      if (selected.size > remainingEntries)
        throw new IOException("openivm state-sync: archive entry limit exceeded")
      var total  = 0L
      val buffer = new Array[Byte](65536)
      selected.foreach { entry =>
        val file = new File(destination, entry.getName)
        if (!file.toPath.normalize().startsWith(destination.toPath.normalize()))
          throw new IOException("openivm state-sync: archive path escapes destination")
        Files.createDirectories(file.getParentFile.toPath)
        val input    = zip.getInputStream(entry)
        val output   = new BufferedOutputStream(new FileOutputStream(file))
        val checksum = new CRC32()
        var copied   = 0L
        try {
          var size = input.read(buffer)
          while (size >= 0) {
            checkInterrupted()
            copied += size
            total += size
            if (total > remainingBytes || copied > entry.getSize)
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
      selected.size -> total
    } finally zip.close()
  }
}
