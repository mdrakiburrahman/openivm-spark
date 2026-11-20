package org.openivm.spark.common.rocksdb

import java.io.IOException
import java.util.UUID
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class OneLakeGuardedPathsSpec extends AnyFunSpec with Matchers {
  private val workspace = "11111111-1111-1111-1111-111111111111"
  private val lakehouse = "22222222-2222-2222-2222-222222222222"
  private val namespace = "33333333-3333-3333-3333-333333333333"
  private val path      = s"/$workspace/$lakehouse/Files/openivm-checkpoints-v1/$namespace"
  private val root      = s"https://onelake.dfs.fabric.microsoft.com$path"
  private val generation =
    GuardedSnapshotDecisions.Generation("0000000000000000001-01234567-89ab-cdef-0123-456789abcdef")
  private val nonce = UUID.fromString("44444444-4444-4444-4444-444444444444")
  private val etag  = "\"0x1234\""

  it("uses the same single-file conditional POSIX rename for either terminal outcome") {
    val paths    = OneLakeGuardedPaths(root, () => fail("credentials are not required to construct a request"))
    val terminal = paths.installRequest(generation, nonce, OneLakeGuardedPaths.Terminal, etag).build()
    terminal.method() shouldBe "PUT"
    terminal.uri().toString shouldBe s"$root/${generation.value}.decision?mode=posix"
    terminal.headers().firstValue("x-ms-rename-source").get() shouldBe s"$path/${generation.value}.pending-$nonce"
    terminal.headers().firstValue("x-ms-source-if-match").get() shouldBe etag
    terminal.headers().firstValue("If-None-Match").get() shouldBe "*"
    terminal.headers().firstValue("x-ms-version").get() shouldBe "2021-06-08"
  }

  it("pins archive reads and nonrecursive deletion to the exact sealed object") {
    val paths  = OneLakeGuardedPaths(root, () => fail("credentials are not required to construct a request"))
    val read   = paths.readArchiveRequest(generation, etag).build()
    val delete = paths.deleteArchiveRequest(generation, etag).build()
    read.method() shouldBe "GET"
    read.uri().toString shouldBe s"$root/${generation.value}.zip"
    delete.method() shouldBe "DELETE"
    delete.uri().toString shouldBe s"${read.uri()}?recursive=false"
    for (req     <- Seq(read, delete)) req.headers().firstValue("If-Match").get() shouldBe etag
    for (version <- Seq("*", "0x1234", "W/\"0x1234\"", "\"a\", \"b\""))
      intercept[IOException](paths.deleteArchiveRequest(generation, version))
  }

  it("binds both pages of a listing and record requests to the exact MSIT endpoint") {
    val host  = "msit-onelake.dfs.fabric.microsoft.com"
    val msit  = root.replace("onelake.dfs.fabric.microsoft.com", host)
    val paths = OneLakeGuardedPaths(msit, () => fail("credentials are not required to construct a request"))
    val requests = Seq(
      paths.listRequest(None),
      paths.listRequest(Some("next+page")),
      paths.installRequest(generation, nonce, OneLakeGuardedPaths.Terminal, etag),
      paths.readArchiveRequest(generation, etag),
      paths.deleteArchiveRequest(generation, etag)
    )
    requests.foreach(_.build().uri().getRawAuthority shouldBe host)
    paths.listRequest(Some("next+page")).build().uri().getRawQuery should include("continuation=next%2Bpage")
    for (authority <- Seq(s"$host.example.com", s"$host:443", s"$host@onelake.dfs.fabric.microsoft.com"))
      intercept[IOException](
        OneLakeGuardedPaths(msit.replace(host, authority), () => fail("unexpected credential read"))
      )
  }

  it("rejects aliases and URLs outside the dedicated GUID-rooted namespace") {
    for (
      alias <- Seq(
        root + "/",
        root + "/../other",
        root + "?other",
        root.replace("https:", "http:"),
        root.replace("onelake.dfs.fabric.microsoft.com", "onelake.dfs.fabric.microsoft.com.example.com"),
        root.replace(workspace, "workspace"),
        root.replace("Files", "%46iles")
      )
    ) intercept[IOException](OneLakeGuardedPaths(alias, () => fail("credentials must not be accessed")))
  }

  it("binds complete terminal records to the exact root, slot, and strong archive ETag") {
    import GuardedSnapshotDecisions._
    import OneLakeGuardedRecords._
    val finished  = Finished(root, generation, 17, "e" * 64, etag)
    val published = decisionBytes(Published(finished))
    val retired   = decisionBytes(Retired(finished))
    decision(published, root, generation) shouldBe Published(finished)
    decision(retired, root, generation) shouldBe Retired(finished)
    receipt(receiptBytes(finished), root, generation) shouldBe finished
    intercept[IOException](decision(published, root.replace(namespace, workspace), generation))
    intercept[IOException](decision(published, root, Generation(generation.value.updated(0, '1'))))
    intercept[IOException](decision(published.dropRight(1), root, generation))
    intercept[IOException](decision(retired ++ published, root, generation))
    intercept[IOException](decision(receiptBytes(finished), root, generation))
    intercept[IOException](receipt(retired, root, generation))
    intercept[IOException](receiptBytes(finished.copy(version = "W/\"stale\"")))
  }
}
