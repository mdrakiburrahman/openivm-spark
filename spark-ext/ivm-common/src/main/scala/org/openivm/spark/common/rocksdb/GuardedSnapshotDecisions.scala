package org.openivm.spark.common.rocksdb

import java.io.{File, IOException}

/** Storage contract for a fresh, exclusively versioned checkpoint namespace. No Hadoop fallback. */
private[rocksdb] object GuardedSnapshotDecisions {
  final case class Generation(value: String) {
    require(value.matches("[0-9]{19}-[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}"), "invalid generation")
  }

  final case class Finished(root: String, generation: Generation, bytes: Long, sha256: String, version: String) {
    require(bytes > 0 && bytes <= OpenIvmSnapshotArchive.MaxBytes, "invalid archive size")
    require(sha256.matches("[0-9a-f]{64}"), "invalid archive checksum")
    require(version.matches("[!-~]{1,256}") && version != "*", "an exact object version is required")
  }

  sealed trait Decision { def artifact: Finished }
  final case class Published(artifact: Finished) extends Decision
  final case class Retired(artifact: Finished)   extends Decision

  sealed trait Deletion
  case object Deleted        extends Deletion
  case object AlreadyAbsent  extends Deletion
  case object VersionChanged extends Deletion

  trait Store {
    val canonicalRoot: String

    // Must reject aliases, mixed formats, nonexclusive namespace layout, and unsupported atomic operations.
    def verifyNamespace(): Unit
    // Returned only for a sealed upload receipt, not inferred from listing or object contents.
    def readFinished(generation: Generation): Option[Finished]
    def readDecision(generation: Generation): Option[Decision]
    def listDecisionGenerations(): Seq[Generation]
    // Atomically installs a complete immutable record into the same slot for both terminal states.
    // A conflicting record must never be replaced; an ambiguous response may throw IOException.
    def installIfAbsent(decision: Decision): Unit
    def requireArchiveVersion(artifact: Finished): Unit
    // Missing must be distinguished from a replacement. Identity check and delete are one operation.
    def deleteExactArchive(artifact: Finished): Deletion
  }

  trait ArchiveStore extends Store {
    // A create-only archive upload followed by an immutable, complete receipt for that version.
    def sealUpload(generation: Generation, file: File, bytes: Long, sha256: String): Finished
    // The downloaded body must be from the requested object version, not a second unpinned GET.
    def copyExactArchive(artifact: Finished, destination: File): Unit
  }
}

private[rocksdb] final class GuardedSnapshotDecisions(store: GuardedSnapshotDecisions.Store) {
  import GuardedSnapshotDecisions._

  def publish(receipt: Finished): Unit = {
    store.verifyNamespace()
    requireReceipt(receipt)
    if (store.readDecision(receipt.generation).contains(Retired(receipt)))
      throw new IOException("checkpoint generation was retired")
    store.requireArchiveVersion(receipt)
    choose(Published(receipt)) match {
      case Published(_) => store.requireArchiveVersion(receipt)
      case Retired(_)   => throw new IOException("checkpoint generation was retired")
    }
  }

  def retire(receipt: Finished): Boolean = {
    store.verifyNamespace()
    requireReceipt(receipt)
    choose(Retired(receipt)) match {
      case Published(_) => false
      case Retired(_) =>
        store.deleteExactArchive(receipt) match {
          case Deleted | AlreadyAbsent => true
          case VersionChanged          => throw new IOException("retired checkpoint archive version changed")
        }
    }
  }

  def latestPublished(): Option[Finished] = {
    store.verifyNamespace()
    store
      .listDecisionGenerations()
      .map { generation =>
        val decision =
          store.readDecision(generation).getOrElse(throw new IOException("checkpoint decision disappeared"))
        if (decision.artifact.generation != generation) throw new IOException("checkpoint generation mismatch")
        requireReceipt(decision.artifact)
        decision
      }
      .collect { case Published(receipt) => receipt }
      .sortBy(_.generation.value)
      .lastOption
      .map { receipt =>
        store.requireArchiveVersion(receipt)
        receipt
      }
  }

  private def requireReceipt(receipt: Finished): Unit =
    if (receipt.root != store.canonicalRoot || !store.readFinished(receipt.generation).contains(receipt))
      throw new IOException("checkpoint upload receipt does not match the namespace and generation")

  private def choose(wanted: Decision): Decision = {
    val generation = wanted.artifact.generation
    val observed = store.readDecision(generation).orElse {
      val failure =
        try {
          store.installIfAbsent(wanted)
          None
        } catch { case error: IOException => Some(error) }
      store.readDecision(generation) match {
        case result @ Some(_) => result
        case None             => throw new IOException("checkpoint decision was not confirmed", failure.orNull)
      }
    }
    observed match {
      case Some(value) if value.artifact == wanted.artifact => value
      case _ => throw new IOException("checkpoint decision does not bind the exact upload receipt")
    }
  }
}
