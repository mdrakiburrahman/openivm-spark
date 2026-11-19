package org.openivm.spark.common

import java.util.UUID

/** Shared identity for monotonic timestamps captured within this JVM process. */
private[openivm] object MonotonicClockDomain {
  val id: String = UUID.randomUUID().toString
}
