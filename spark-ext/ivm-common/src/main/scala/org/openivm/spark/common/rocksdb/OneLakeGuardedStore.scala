package org.openivm.spark.common.rocksdb

import com.fasterxml.jackson.databind.ObjectMapper
import java.io.{ByteArrayInputStream, File, IOException, InputStream}
import java.net.URI
import java.net.http.HttpResponse
import java.nio.charset.StandardCharsets.US_ASCII
import java.nio.file.{Files, StandardOpenOption}
import java.security.MessageDigest
import java.util.{Arrays, UUID}
import scala.collection.mutable
import scala.jdk.CollectionConverters._

private[rocksdb] final class OneLakeGuardedStore(paths: OneLakeGuardedPaths)
    extends GuardedSnapshotDecisions.ArchiveStore {
  import GuardedSnapshotDecisions._
  import OneLakeGuardedPaths._
  import OneLakeGuardedRecords._

  val canonicalRoot: String = paths.canonicalRoot
  private val prefix        = URI.create(canonicalRoot).getPath.stripPrefix("/").split("/", 2)(1) + "/"
  private val mapper        = new ObjectMapper()
  private val id            = "[0-9]{19}-[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}"
  private val allowedName   = s"$id\\.(zip|upload|finished|decision|pending-[0-9a-f-]{36})"

  override def verifyNamespace(): Unit = {
    val marker = use(paths.readMarker()) { r =>
      check(r, 200, "read namespace marker")
      bounded(r.body(), MaxRecordBytes)
    }
    if (!Arrays.equals(marker, namespaceMarker(canonicalRoot)))
      throw new IOException("guarded checkpoint namespace marker mismatch")
    val _ = listDecisionGenerations()
  }

  override def listDecisionGenerations(): Seq[Generation] = {
    val names         = mutable.HashSet.empty[String]
    val continuations = mutable.HashSet.empty[String]
    var next          = Option.empty[String]
    do {
      next = use(paths.list(next)) { r =>
        check(r, 200, "list namespace")
        val entries = mapper.readTree(bounded(r.body(), 512 * 1024)).path("paths")
        if (!entries.isArray) throw new IOException("invalid guarded checkpoint listing")
        entries.elements().asScala.foreach { entry =>
          val full      = entry.path("name").asText()
          val name      = full.stripPrefix(prefix)
          val directory = entry.path("isDirectory")
          if (
            full == name || !(name == "format" || name.matches(allowedName)) ||
            (!directory.isMissingNode && directory.asText() != "false") || !names.add(name)
          )
            throw new IOException("unexpected or non-file member in guarded checkpoint namespace")
        }
        val value = r.headers().firstValue("x-ms-continuation")
        if (value.isPresent && value.get().nonEmpty) Some(value.get()) else None
      }
      if (next.exists(n => !continuations.add(n)) || continuations.size >= 64)
        throw new IOException("guarded checkpoint namespace listing is cyclic or exceeds its bound")
    } while (next.nonEmpty)
    names.iterator.filter(_.endsWith(".decision")).map(n => Generation(n.stripSuffix(".decision"))).toVector
  }

  override def readFinished(g: Generation): Option[Finished] =
    readRecord(g, Receipt).map(receipt(_, canonicalRoot, g))

  override def readDecision(g: Generation): Option[Decision] =
    readRecord(g, Terminal).map(decision(_, canonicalRoot, g))

  override def installIfAbsent(d: Decision): Unit = {
    val g = d.artifact.generation
    installBytes(g, Terminal, decisionBytes(d))
  }

  override def sealUpload(g: Generation, file: File, bytes: Long, sha256: String): Finished = {
    // The permanent reservation prevents a delayed second uploader from recreating a retired archive path.
    val reservation = s"OPENIVM_GUARDED_CHECKPOINT_V1\nUPLOAD\n$canonicalRoot\n${g.value}\n${UUID.randomUUID()}\n"
      .getBytes(US_ASCII)
    installBytes(g, Upload, reservation)
    if (!readRecord(g, Upload).exists(Arrays.equals(_, reservation)))
      throw new IOException("guarded archive upload reservation was not confirmed")
    val input = Files.newInputStream(file.toPath)
    try stageAndInstall(g, Archive, input, bytes)
    finally input.close()
    val artifact = Finished(canonicalRoot, g, bytes, sha256, archiveVersion(g, bytes))
    withExactArchive(artifact)(_ => ())
    installBytes(g, Receipt, receiptBytes(artifact))
    if (!readFinished(g).contains(artifact)) throw new IOException("guarded archive seal was not confirmed")
    artifact
  }

  override def requireArchiveVersion(a: Finished): Unit =
    if (archiveVersion(a.generation, a.bytes) != a.version)
      throw new IOException("guarded checkpoint archive version changed")

  override def copyExactArchive(a: Finished, destination: File): Unit = {
    val output = Files.newOutputStream(destination.toPath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)
    try withExactArchive(a)(chunk => output.write(chunk))
    finally output.close()
  }

  override def deleteExactArchive(a: Finished): Deletion = use(paths.deleteArchive(a.generation, a.version)) { r =>
    if (missing(r)) AlreadyAbsent
    else
      (r.statusCode(), error(r)) match {
        case (200, _)                 => Deleted
        case (412, "ConditionNotMet") => VersionChanged
        case _                        => throw failure(r, "delete exact retired archive")
      }
  }

  private def readRecord(g: Generation, slot: RecordSlot): Option[Array[Byte]] =
    use(paths.readRecord(g, slot)) { r =>
      if (missing(r)) None
      else {
        check(r, 200, "read guarded record")
        Some(bounded(r.body(), MaxRecordBytes))
      }
    }

  private def installBytes(g: Generation, slot: RecordSlot, body: Array[Byte]): Unit = {
    val failure =
      try {
        stageAndInstall(g, slot, new ByteArrayInputStream(body), body.length.toLong)
        None
      } catch { case e: IOException => Some(e) }
    if (!readRecord(g, slot).exists(Arrays.equals(_, body)))
      throw new IOException("guarded checkpoint record install not confirmed", failure.orNull)
  }

  private def stageAndInstall(g: Generation, slot: InstallSlot, input: InputStream, bytes: Long): Unit = {
    if (bytes <= 0 || bytes > OpenIvmSnapshotArchive.MaxBytes)
      throw new IOException("invalid guarded checkpoint upload length")
    val nonce = UUID.randomUUID()
    use(paths.createPending(g, nonce))(check(_, 201, "create pending file"))
    var written = 0L
    while (written < bytes) {
      OpenIvmSnapshotArchive.checkInterrupted()
      val count = math.min(bytes - written, ChunkBytes.toLong).toInt
      val chunk = input.readNBytes(count)
      if (chunk.length != count) throw new IOException("truncated guarded checkpoint upload")
      use(paths.appendPending(g, nonce, written, chunk))(check(_, 202, "append pending file"))
      written += count
    }
    if (input.read() != -1) throw new IOException("guarded checkpoint upload exceeds declared length")
    val flushed = use(paths.closePending(g, nonce, bytes)) { r =>
      check(r, 200, "close pending file")
      etag(r)
    }
    use(paths.install(g, nonce, slot, flushed))(check(_, 201, "conditionally install guarded file"))
  }

  private def archiveVersion(g: Generation, bytes: Long): String = use(paths.archiveStatus(g)) { r =>
    check(r, 200, "check archive identity")
    if (r.headers().firstValueAsLong("Content-Length").orElse(-1L) != bytes)
      throw new IOException("guarded checkpoint archive length changed")
    etag(r)
  }

  private def withExactArchive(a: Finished)(consume: Array[Byte] => Unit): Unit =
    use(paths.readArchive(a.generation, a.version)) { r =>
      check(r, 200, "read exact archive")
      if (etag(r) != a.version || r.headers().firstValueAsLong("Content-Length").orElse(-1L) != a.bytes)
        throw new IOException("guarded archive response did not bind the sealed object")
      val digest    = MessageDigest.getInstance("SHA-256")
      var remaining = a.bytes
      while (remaining > 0) {
        OpenIvmSnapshotArchive.checkInterrupted()
        val want  = math.min(remaining, ChunkBytes.toLong).toInt
        val chunk = r.body().readNBytes(want)
        if (chunk.length != want) throw new IOException("truncated guarded archive download")
        digest.update(chunk)
        consume(chunk)
        remaining -= want
      }
      if (r.body().read() != -1 || java.util.HexFormat.of().formatHex(digest.digest()) != a.sha256)
        throw new IOException("guarded archive digest or length mismatch")
    }

  private def use[A](r: HttpResponse[InputStream])(f: HttpResponse[InputStream] => A): A =
    try f(r)
    finally r.body().close()

  private def bounded(in: InputStream, limit: Int): Array[Byte] = {
    val content = in.readNBytes(limit + 1)
    if (content.length > limit) throw new IOException("guarded checkpoint response exceeds its limit")
    content
  }

  private def etag(r: HttpResponse[InputStream]): String =
    exactVersion(r.headers().firstValue("ETag").orElse(""))

  private def error(r: HttpResponse[InputStream]): String =
    r.headers().firstValue("x-ms-error-code").orElse("")

  private def missing(r: HttpResponse[InputStream]): Boolean =
    r.statusCode() == 404 && error(r) == "PathNotFound"

  private def check(r: HttpResponse[InputStream], expected: Int, action: String): Unit =
    if (r.statusCode() != expected) throw failure(r, action)

  private def failure(r: HttpResponse[InputStream], action: String): IOException =
    new IOException(s"guarded checkpoint could not $action (HTTP ${r.statusCode()})")
}
