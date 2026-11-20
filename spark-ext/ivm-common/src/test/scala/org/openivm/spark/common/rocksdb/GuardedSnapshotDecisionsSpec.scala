package org.openivm.spark.common.rocksdb

import java.io.IOException
import java.nio.channels.FileChannel
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths, StandardCopyOption, StandardOpenOption}
import java.util.concurrent.TimeUnit
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import scala.jdk.CollectionConverters._

private[rocksdb] object GuardedSnapshotDecisionProbe {
  import GuardedSnapshotDecisions._

  def generation(n: Int): Generation = Generation(f"$n%019d-01234567-89ab-cdef-0123-456789abcdef")

  // Cooperative local test store, not a filesystem capability advertised to Spark.
  final class LocalStore(
      root: Path,
      loseInstall: Boolean = false,
      loseDelete: Boolean = false,
      beforeInstall: () => Unit = () => ()
  ) extends Store {
    val canonicalRoot: String = root.toRealPath().toString
    private def slot(g: Generation, suffix: String): Path = root.resolve(g.value + suffix)
    def archive(g: Generation): Path = slot(g, ".zip")
    private def version(g: Generation): String =
      Files.getAttribute(archive(g), "unix:ino").toString
    private def fields(f: Finished): Seq[String] =
      Seq(f.root, f.generation.value, f.bytes.toString, f.sha256, f.version)
    private def decode(values: Seq[String]): Finished =
      Finished(values(0), Generation(values(1)), values(2).toLong, values(3), values(4))
    private def read(path: Path): Option[Seq[String]] =
      if (Files.notExists(path)) None else Some(Files.readAllLines(path, UTF_8).asScala.toVector)
    private def write(path: Path, lines: Seq[String]): Unit =
      Files.write(path, lines.asJava, UTF_8, StandardOpenOption.CREATE_NEW)
    private def locked[A](g: Generation)(body: => A): A = {
      val channel = FileChannel.open(slot(g, ".lock"), StandardOpenOption.CREATE, StandardOpenOption.WRITE)
      try {
        val lock = channel.lock()
        try body finally lock.release()
      } finally channel.close()
    }

    def seal(g: Generation): Finished = {
      val path = archive(g)
      val receipt = Finished(canonicalRoot, g, Files.size(path), OpenIvmSnapshotArchive.sha256(path.toFile), version(g))
      write(slot(g, ".finished"), fields(receipt))
      receipt
    }

    override def verifyNamespace(): Unit =
      if (root.toRealPath().toString != canonicalRoot || !read(root.resolve("format")).contains(Seq("guarded-v1")) ||
          !Files.notExists(root.resolve(".openivm-state-sync")))
        throw new IOException("unverified local guarded namespace")
    override def readFinished(g: Generation): Option[Finished] = read(slot(g, ".finished")).map(decode)
    override def readDecision(g: Generation): Option[Decision] = read(slot(g, ".decision")).map {
      case values if values.size == 6 && values.head == "publish" => Published(decode(values.tail))
      case values if values.size == 6 && values.head == "retire" => Retired(decode(values.tail))
      case _ => throw new IOException("invalid terminal decision")
    }
    override def listDecisionGenerations(): Seq[Generation] = {
      val stream = Files.list(root)
      try stream.iterator().asScala.map(_.getFileName.toString).filter(_.endsWith(".decision"))
        .map(name => Generation(name.stripSuffix(".decision"))).toVector
      finally stream.close()
    }
    override def installIfAbsent(decision: Decision): Unit = {
      val temp = Files.createTempFile(root, "terminal-", ".tmp")
      try {
        val state = decision match {
          case Published(_) => "publish"
          case Retired(_) => "retire"
        }
        Files.write(temp, (state +: fields(decision.artifact)).asJava, UTF_8)
        beforeInstall()
        Files.createLink(slot(decision.artifact.generation, ".decision"), temp)
        if (loseInstall) throw new IOException("injected lost terminal install response")
      } finally Files.delete(temp)
    }
    override def requireArchiveVersion(artifact: Finished): Unit =
      if (version(artifact.generation) != artifact.version) throw new IOException("archive version changed")
    override def deleteExactArchive(artifact: Finished): Deletion = locked(artifact.generation) {
      if (Files.notExists(archive(artifact.generation))) AlreadyAbsent
      else if (version(artifact.generation) != artifact.version) VersionChanged
      else {
        Files.delete(archive(artifact.generation))
        if (loseDelete) throw new IOException("injected lost delete response")
        Deleted
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val root = Paths.get(args(0))
    val role = args(1)
    val store = new LocalStore(
      root,
      loseInstall = true,
      loseDelete = role == "retire-lost-delete",
      beforeInstall = () => if (role.startsWith("race-")) {
        Files.createFile(root.resolve(s"$role.ready"))
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20)
        while (Files.notExists(root.resolve("race-release")) && System.nanoTime() < deadline) Thread.sleep(10)
        require(Files.exists(root.resolve("race-release")), "race was not released")
      }
    )
    val protocol = new GuardedSnapshotDecisions(store)
    val receipt = store.readFinished(generation(args(2).toInt)).get
    role match {
      case "publish" => protocol.publish(receipt)
      case "race-publish" =>
        try protocol.publish(receipt)
        catch { case e: IOException if e.getMessage.contains("was retired") => () }
      case "race-retire" => protocol.retire(receipt)
      case "publish-declined" =>
        try {
          protocol.publish(receipt)
          sys.error("retired generation became published")
        } catch { case e: IOException if e.getMessage.contains("was retired") => () }
      case "retire-lost-delete" =>
        try {
          protocol.retire(receipt)
          sys.error("lost delete response was acknowledged")
        } catch { case e: IOException if e.getMessage.contains("lost delete") => () }
      case "retire" => require(protocol.retire(receipt))
      case "restore" =>
        val selected = protocol.latestPublished().get
        require(selected == receipt)
        val source = store.archive(selected.generation).toFile
        require(source.length() == selected.bytes && OpenIvmSnapshotArchive.sha256(source) == selected.sha256)
        val restored = Files.createDirectory(root.resolve("restored"))
        OpenIvmSnapshotArchive.extract(source, restored.toFile)
        require(Files.readAllLines(restored.resolve("state"), UTF_8).asScala == Seq("generation-1"))
      case other => sys.error(s"unexpected probe operation: $other")
    }
  }
}

class GuardedSnapshotDecisionsSpec extends AnyFunSpec with Matchers {
  import GuardedSnapshotDecisionProbe._

  it("reconciles lost decisions across processes without exposing a retired archive on restore") {
    val root = Files.createTempDirectory(Paths.get("target").toAbsolutePath, "guarded-decisions-")
    Files.write(root.resolve("format"), Seq("guarded-v1").asJava, UTF_8)
    val store = new LocalStore(root)
    val protocol = new GuardedSnapshotDecisions(store)
    def prepare(n: Int): GuardedSnapshotDecisions.Finished = {
      val stage = Files.createDirectory(root.resolve(s"stage-$n"))
      val file = Files.write(stage.resolve("state"), Seq(s"generation-$n").asJava, UTF_8)
      OpenIvmSnapshotArchive.pack(stage.toFile, Seq(file.toFile), store.archive(generation(n)).toFile, _ => ())
      store.seal(generation(n))
    }
    def start(role: String, n: Int): Process = {
      val java = Paths.get(System.getProperty("java.home"), "bin", "java").toString
      new ProcessBuilder(java, "-cp", System.getProperty("java.class.path"),
        "org.openivm.spark.common.rocksdb.GuardedSnapshotDecisionProbe", root.toString, role, n.toString)
        .redirectErrorStream(true).redirectOutput(root.resolve(s"$role-$n.log").toFile).start()
    }

    def waitForChild(process: Process): Unit = {
      try {
        process.waitFor(45, TimeUnit.SECONDS) shouldBe true
        process.exitValue() shouldBe 0
      } finally if (process.isAlive) process.destroyForcibly()
    }

    def child(role: String, n: Int): Unit = waitForChild(start(role, n))

    val published = prepare(1)
    prepare(2)
    protocol.latestPublished() shouldBe None
    child("publish", 1)
    protocol.retire(published) shouldBe false
    child("retire-lost-delete", 2)
    child("retire", 2)
    child("publish-declined", 2)
    child("restore", 1)
    Files.exists(store.archive(generation(2))) shouldBe false
    val replaced = prepare(3)
    val old = store.archive(generation(3))
    Files.move(old, root.resolve("old-inode"), StandardCopyOption.ATOMIC_MOVE)
    Files.write(old, Seq("replacement").asJava, UTF_8)
    intercept[IOException](protocol.retire(replaced)).getMessage should include("version changed")
    Files.readAllLines(old, UTF_8).asScala shouldBe Seq("replacement")

    val raced = prepare(4)
    val racers = Seq("race-publish", "race-retire")
    val processes = racers.map(start(_, 4))
    try {
      val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15)
      def bothReady: Boolean = racers.forall(role => Files.exists(root.resolve(s"$role.ready")))
      while (!bothReady && System.nanoTime() < deadline) Thread.sleep(10)
      bothReady shouldBe true
      Files.createFile(root.resolve("race-release"))
      processes.foreach(waitForChild)
      store.readDecision(raced.generation) match {
        case Some(GuardedSnapshotDecisions.Published(`raced`)) =>
          protocol.latestPublished() shouldBe Some(raced)
          protocol.retire(raced) shouldBe false
        case Some(GuardedSnapshotDecisions.Retired(`raced`)) =>
          protocol.latestPublished() shouldBe Some(published)
          Files.exists(store.archive(raced.generation)) shouldBe false
        case other => fail(s"invalid terminal state: $other")
      }
    } finally processes.filter(_.isAlive).foreach(_.destroyForcibly())

    Files.createDirectory(root.resolve(".openivm-state-sync"))
    intercept[IOException](protocol.latestPublished()).getMessage should include("unverified")
  }
}
