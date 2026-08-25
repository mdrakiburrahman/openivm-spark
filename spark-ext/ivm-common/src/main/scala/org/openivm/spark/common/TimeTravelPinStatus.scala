package org.openivm.spark.common

import java.util.Locale

/** Telemetry contract for a user-authored snapshot pin (`… VERSION AS OF 366`,
  * `… TIMESTAMP AS OF '2024-01-01'`) on a materialized view's source.
  *
  * Downstream consumers (the campaign/source integration's scratch historical
  * MV probe and its hydrate guards) gate on this status, so it is a CONTRACT,
  * not a debug hint:
  *
  *   - [[Applied]]        — the view carries at least one user pin, every pin
  *                          resolved to exactly one tracked source, and the
  *                          engine froze that source at the pinned snapshot.
  *                          Reported for CREATE and for every subsequent
  *                          REFRESH of that view.
  *   - [[NotApplicable]]  — the view body carries no user pin at all.
  *   - [[CompileFailed]]  — the body carries a pin OpenIVM cannot honor
  *                          incrementally (two versions of one source, a moving
  *                          value, a pin that binds to no tracked source, …).
  *                          Those bodies are refused by the compile bridge and
  *                          demoted to `FULL_REFRESH`.
  *
  * The status is always derived from the USER's `querySql` plus the view's
  * tracked sources — never from compiled or generated SQL text, whose delta
  * statements legitimately carry no temporal clause at all.
  */
object TimeTravelPinStatus {

  val Applied: String       = "APPLIED"
  val NotApplicable: String = "NOT_APPLICABLE"
  val CompileFailed: String = "COMPILE_FAILED"

  val All: Seq[String] = Seq(Applied, NotApplicable, CompileFailed)

  /** True only for the exact [[Applied]] contract value. Guards must call this
    * rather than pattern-matching on compiled SQL text.
    */
  def isApplied(status: String): Boolean = status == Applied

  /** Canonical form of a persisted / logged status, or `None` when the value is
    * absent, blank or not part of the contract. An unrecognised value is
    * deliberately NOT coerced to [[NotApplicable]]: a guard must see it as
    * missing rather than as a silent "no pin here".
    */
  def normalize(raw: String): Option[String] =
    Option(raw)
      .map(_.trim.toUpperCase(Locale.ROOT))
      .filter(All.contains)
}
