package org.openivm.spark.common.rocksdb

import java.io.{IOException, InputStream}
import java.net.{URI, URLEncoder}
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.charset.StandardCharsets.UTF_8
import java.time.Duration
import java.util.UUID

/** Driver-only DFS transport for guarded checkpoint slots, not a general path client. */
private[rocksdb] final class OneLakeGuardedPaths private (
    val canonicalRoot: String,
    token: () => String,
    exchange: HttpRequest => HttpResponse[InputStream]
) {
  import GuardedSnapshotDecisions.Generation
  import OneLakeGuardedPaths._
  import HttpRequest.BodyPublishers.{noBody, ofByteArray}

  private def file(generation: Generation, slot: Slot): URI =
    URI.create(s"$canonicalRoot/${generation.value}.${slot.suffix}")

  private def request(uri: URI): HttpRequest.Builder =
    HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(60)).header("x-ms-version", ApiVersion)

  private[rocksdb] def installRequest(
      generation: Generation,
      nonce: UUID,
      target: InstallSlot,
      sourceVersion: String
  ): HttpRequest.Builder = {
    val source = file(generation, Pending(nonce))
    request(URI.create(s"${file(generation, target)}?mode=posix"))
      .header("x-ms-rename-source", source.getRawPath)
      .header("x-ms-source-if-match", exactVersion(sourceVersion))
      .header("If-None-Match", "*")
      .PUT(noBody())
  }

  private[rocksdb] def readArchiveRequest(generation: Generation, version: String): HttpRequest.Builder =
    request(file(generation, Archive)).header("If-Match", exactVersion(version)).GET()

  private[rocksdb] def deleteArchiveRequest(generation: Generation, version: String): HttpRequest.Builder =
    request(URI.create(s"${file(generation, Archive)}?recursive=false"))
      .header("If-Match", exactVersion(version))
      .DELETE()

  private[rocksdb] def createPendingRequest(generation: Generation, nonce: UUID): HttpRequest.Builder =
    request(URI.create(s"${file(generation, Pending(nonce))}?resource=file"))
      .header("If-None-Match", "*")
      .PUT(noBody())

  private[rocksdb] def appendPendingRequest(
      generation: Generation,
      nonce: UUID,
      position: Long,
      data: Array[Byte]
  ): HttpRequest.Builder = {
    require(position >= 0 && data.nonEmpty && data.length <= ChunkBytes, "invalid guarded upload chunk")
    request(URI.create(s"${file(generation, Pending(nonce))}?action=append&position=$position"))
      .header("Content-Type", "application/octet-stream")
      .method("PATCH", ofByteArray(data))
  }

  private[rocksdb] def flushPendingRequest(
      generation: Generation,
      nonce: UUID,
      bytes: Long
  ): HttpRequest.Builder = {
    require(bytes > 0 && bytes <= OpenIvmSnapshotArchive.MaxBytes, "invalid guarded upload length")
    request(URI.create(s"${file(generation, Pending(nonce))}?action=flush&position=$bytes&close=true"))
      .method("PATCH", noBody())
  }

  private[rocksdb] def listRequest(continuation: Option[String]): HttpRequest.Builder = {
    val path      = URI.create(canonicalRoot).getRawPath.stripPrefix("/")
    val parts     = path.split("/", 2)
    val directory = URLEncoder.encode(parts(1), UTF_8.name())
    val suffix = continuation
      .map { next =>
        require(next.nonEmpty && next.length <= 2048, "invalid guarded listing continuation")
        s"&continuation=${URLEncoder.encode(next, UTF_8.name())}"
      }
      .getOrElse("")
    request(
      URI.create(
        s"https://${URI.create(canonicalRoot).getHost}/${parts(0)}?resource=filesystem&directory=$directory&recursive=false&maxResults=256$suffix"
      )
    )
      .GET()
  }

  // Keep the supplier on the calling driver. The caller must reconcile conditional writes by readback.
  def install(generation: Generation, nonce: UUID, target: InstallSlot, version: String): HttpResponse[InputStream] =
    send(installRequest(generation, nonce, target, version))

  def createPending(generation: Generation, nonce: UUID): HttpResponse[InputStream] =
    send(createPendingRequest(generation, nonce))

  def appendPending(generation: Generation, nonce: UUID, position: Long, data: Array[Byte]): HttpResponse[InputStream] =
    send(appendPendingRequest(generation, nonce, position, data))

  def closePending(generation: Generation, nonce: UUID, bytes: Long): HttpResponse[InputStream] =
    send(flushPendingRequest(generation, nonce, bytes))

  def list(continuation: Option[String]): HttpResponse[InputStream] =
    send(listRequest(continuation))

  def readMarker(): HttpResponse[InputStream] =
    send(request(URI.create(s"$canonicalRoot/format")).GET())

  def readRecord(generation: Generation, slot: RecordSlot): HttpResponse[InputStream] =
    send(request(file(generation, slot)).GET())

  def archiveStatus(generation: Generation): HttpResponse[InputStream] =
    send(request(file(generation, Archive)).method("HEAD", noBody()))

  def readArchive(generation: Generation, version: String): HttpResponse[InputStream] =
    send(readArchiveRequest(generation, version))

  def deleteArchive(generation: Generation, version: String): HttpResponse[InputStream] =
    send(deleteArchiveRequest(generation, version))

  private def send(request: HttpRequest.Builder): HttpResponse[InputStream] = {
    val bearer = token()
    if (bearer == null || !bearer.matches("[A-Za-z0-9._~+/-]+=*"))
      throw new IOException("invalid OneLake storage credential")
    exchange(request.setHeader("Authorization", s"Bearer $bearer").build())
  }
}

private[rocksdb] object OneLakeGuardedPaths {
  private val Hosts      = Seq("onelake.dfs.fabric.microsoft.com", "msit-onelake.dfs.fabric.microsoft.com")
  private val ApiVersion = "2021-06-08"
  private val LowerUuid  = "[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}"
  val ChunkBytes         = 4 * 1024 * 1024
  private val Root =
    s"https://(?:${Hosts.map(java.util.regex.Pattern.quote).mkString("|")})" +
      s"/$LowerUuid/$LowerUuid/Files/openivm-checkpoints-v1/$LowerUuid"

  sealed abstract class Slot(val suffix: String)
  sealed abstract class InstallSlot(suffix: String) extends Slot(suffix)
  sealed abstract class RecordSlot(suffix: String)  extends InstallSlot(suffix)
  case object Receipt                               extends RecordSlot("finished")
  case object Terminal                              extends RecordSlot("decision")
  case object Upload                                extends RecordSlot("upload")
  case object Archive                               extends InstallSlot("zip")
  private final case class Pending(nonce: UUID)     extends Slot(s"pending-$nonce")

  def apply(root: String, token: () => String): OneLakeGuardedPaths = {
    requireCanonicalRoot(root)
    val http = HttpClient
      .newBuilder()
      .connectTimeout(Duration.ofSeconds(15))
      .followRedirects(HttpClient.Redirect.NEVER)
      .build()
    new OneLakeGuardedPaths(root, token, request => http.send(request, HttpResponse.BodyHandlers.ofInputStream()))
  }

  private[rocksdb] def forTesting(
      root: String,
      exchange: HttpRequest => HttpResponse[InputStream]
  ): OneLakeGuardedPaths = {
    requireCanonicalRoot(root)
    new OneLakeGuardedPaths(root, () => "synthetic-storage-token", exchange)
  }

  private[rocksdb] def requireCanonicalRoot(root: String): Unit =
    if (!root.matches(Root))
      throw new IOException("guarded checkpoint root must use canonical OneLake GUID paths")

  private[rocksdb] def exactVersion(value: String): String = {
    if (!value.matches("\"[A-Za-z0-9._:-]{1,128}\""))
      throw new IOException("guarded checkpoint requires one strong quoted object ETag")
    value
  }
}
