package org.openivm.spark.common.rocksdb

import java.io.{IOException, InputStream}
import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.time.Duration
import java.util.UUID

/** Driver-only DFS transport for guarded checkpoint slots, not a general path client. */
private[rocksdb] final class OneLakeGuardedPaths private (
    val canonicalRoot: String,
    token: () => String
) {
  import GuardedSnapshotDecisions.Generation
  import OneLakeGuardedPaths._
  import HttpRequest.BodyPublishers.noBody

  private val http = HttpClient
    .newBuilder()
    .connectTimeout(Duration.ofSeconds(15))
    .followRedirects(HttpClient.Redirect.NEVER)
    .build()

  private def file(generation: Generation, slot: Slot): URI =
    URI.create(s"$canonicalRoot/${generation.value}.${slot.suffix}")

  private def request(uri: URI): HttpRequest.Builder =
    HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(60)).header("x-ms-version", ApiVersion)

  private[rocksdb] def installRequest(
      generation: Generation,
      nonce: UUID,
      target: RecordSlot,
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

  // Keep the supplier on the calling driver. The caller must reconcile conditional writes by readback.
  def install(generation: Generation, nonce: UUID, target: RecordSlot, version: String): HttpResponse[InputStream] =
    send(installRequest(generation, nonce, target, version))

  def readArchive(generation: Generation, version: String): HttpResponse[InputStream] =
    send(readArchiveRequest(generation, version))

  def deleteArchive(generation: Generation, version: String): HttpResponse[InputStream] =
    send(deleteArchiveRequest(generation, version))

  private def send(request: HttpRequest.Builder): HttpResponse[InputStream] = {
    val bearer = token()
    if (bearer == null || !bearer.matches("[A-Za-z0-9._~+/-]+=*"))
      throw new IOException("invalid OneLake storage credential")
    http.send(request.setHeader("Authorization", s"Bearer $bearer").build(), HttpResponse.BodyHandlers.ofInputStream())
  }
}

private[rocksdb] object OneLakeGuardedPaths {
  private val Host       = "onelake.dfs.fabric.microsoft.com"
  private val ApiVersion = "2021-06-08"
  private val LowerUuid  = "[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}"
  private val Root =
    s"https://${java.util.regex.Pattern.quote(Host)}/$LowerUuid/$LowerUuid/Files/openivm-checkpoints-v1/$LowerUuid"

  sealed abstract class Slot(val suffix: String)
  sealed abstract class RecordSlot(suffix: String) extends Slot(suffix)
  case object Receipt                              extends RecordSlot("finished")
  case object Terminal                             extends RecordSlot("decision")
  private case object Archive                      extends Slot("zip")
  private final case class Pending(nonce: UUID)    extends Slot(s"pending-$nonce")

  def apply(root: String, token: () => String): OneLakeGuardedPaths = {
    requireCanonicalRoot(root)
    new OneLakeGuardedPaths(root, token)
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
