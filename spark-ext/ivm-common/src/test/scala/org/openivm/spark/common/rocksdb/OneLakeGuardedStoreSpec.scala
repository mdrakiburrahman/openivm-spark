package org.openivm.spark.common.rocksdb

import java.io.{ByteArrayInputStream, IOException, InputStream}
import java.net.URI
import java.net.http.{HttpClient, HttpHeaders, HttpRequest, HttpResponse}
import java.nio.charset.StandardCharsets.UTF_8
import java.util.Optional
import javax.net.ssl.SSLSession
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import scala.jdk.CollectionConverters._

class OneLakeGuardedStoreSpec extends AnyFunSpec with Matchers {
  import GuardedSnapshotDecisions._
  import OneLakeGuardedRecords._

  private val workspace  = "11111111-1111-1111-1111-111111111111"
  private val lakehouse  = "22222222-2222-2222-2222-222222222222"
  private val namespace  = "33333333-3333-3333-3333-333333333333"
  private val dir        = s"$lakehouse/Files/openivm-checkpoints-v1/$namespace"
  private val root       = s"https://onelake.dfs.fabric.microsoft.com/$workspace/$dir"
  private val generation = Generation("0000000000000000001-01234567-89ab-cdef-0123-456789abcdef")
  private val artifact   = Finished(root, generation, 17, "e" * 64, "\"0x4321\"")

  private def response(
      req: HttpRequest,
      status: Int,
      bytes: Array[Byte] = Array.emptyByteArray,
      fields: Map[String, String] = Map.empty
  ): HttpResponse[InputStream] =
    new HttpResponse[InputStream] {
      def body(): InputStream                                     = input
      private val input                                           = new ByteArrayInputStream(bytes)
      def statusCode(): Int                                       = status
      def request(): HttpRequest                                  = req
      def previousResponse(): Optional[HttpResponse[InputStream]] = Optional.empty()
      def headers(): HttpHeaders =
        HttpHeaders.of(fields.map { case (k, v) => k -> java.util.Collections.singletonList(v) }.asJava, (_, _) => true)
      def sslSession(): Optional[SSLSession] = Optional.empty()
      def uri(): URI                         = req.uri()
      def version(): HttpClient.Version      = HttpClient.Version.HTTP_1_1
    }

  it("deletes only after reading back its retirement when the conditional install response is lost") {
    var installed = false
    var deleted   = false
    var deletes   = 0
    val paths = OneLakeGuardedPaths.forTesting(
      root,
      req => {
        val q    = Option(req.uri().getRawQuery).getOrElse("")
        val path = req.uri().getPath
        if (q.startsWith("resource=filesystem")) {
          val members = Seq("format", s"${generation.value}.finished") ++
            (if (installed) Seq(s"${generation.value}.decision") else Nil)
          val entries = members.map(name => s"""{"name":"$dir/$name","isDirectory":false}""").mkString(",")
          response(req, 200, s"""{"paths":[$entries]}""".getBytes(UTF_8))
        } else if (path.endsWith("/format")) response(req, 200, namespaceMarker(root))
        else if (path.endsWith(".finished")) response(req, 200, receiptBytes(artifact))
        else if (path.endsWith(".decision") && q == "mode=posix") {
          req.headers().firstValue("If-None-Match").get() shouldBe "*"
          req.headers().firstValue("x-ms-source-if-match").get() shouldBe "\"0x1234\""
          installed = true
          throw new IOException("lost terminal install response")
        } else if (path.endsWith(".decision")) {
          if (installed) response(req, 200, decisionBytes(Retired(artifact)))
          else response(req, 404, fields = Map("x-ms-error-code" -> "PathNotFound"))
        } else if (q == "resource=file") response(req, 201)
        else if (q.startsWith("action=append")) response(req, 202)
        else if (q.startsWith("action=flush")) response(req, 200, fields = Map("ETag" -> "\"0x1234\""))
        else if (req.method() == "DELETE") {
          installed shouldBe true
          q shouldBe "recursive=false"
          req.headers().firstValue("If-Match").get() shouldBe artifact.version
          deletes += 1
          if (deleted) response(req, 404, fields = Map("x-ms-error-code" -> "PathNotFound"))
          else {
            deleted = true
            throw new IOException("lost delete response")
          }
        } else fail(s"unexpected fixture request ${req.method()} $path")
      }
    )
    val protocol = new GuardedSnapshotDecisions(new OneLakeGuardedStore(paths))
    intercept[IOException](protocol.retire(artifact)).getMessage shouldBe "lost delete response"
    protocol.retire(artifact) shouldBe true
    protocol.latestPublished() shouldBe None
    deletes shouldBe 2
  }
}
