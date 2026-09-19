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
    * deliberately NOT coerced to [[NotApplicable]]: a caller must see it as
    * missing rather than as a silent "no pin here".
    *
    * Use [[normalizeOrRefuse]] wherever the value is about to be REPORTED or
    * TRUSTED; this raw form only answers "is this a contract value".
    */
  def normalize(raw: String): Option[String] =
    Option(raw)
      .map(_.trim.toUpperCase(Locale.ROOT))
      .filter(All.contains)

  /** Fail-closed normalization for a value that was persisted or handed to the
    * telemetry: a present-but-unrecognised status becomes [[CompileFailed]],
    * never [[NotApplicable]] and never silently dropped.
    *
    * Dropping it would let a corrupt `_ivm_time_travel_pin_status` property (or
    * a caller passing a typo) look like "this view was created before the
    * contract existed", which re-derives the status from the body and hides the
    * corruption. Reporting [[CompileFailed]] instead makes a downstream guard
    * reject the view loudly. Absent/blank stays `None` — that is a genuinely
    * missing value, which guards reject on coverage grounds.
    */
  def normalizeOrRefuse(raw: String): Option[String] =
    Option(raw)
      .map(_.trim)
      .filter(_.nonEmpty)
      .map(value => normalize(value).getOrElse(CompileFailed))
}

/** Stable, machine-comparable explanations for a [[TimeTravelPinStatus]].
  *
  * Emitted as `time_travel_pin_reason` next to every `time_travel_pin_status`
  * on both the `[openivm-mv]` classification lines and
  * `OPENIVM_EXECUTION_SPAN`, so a consumer can tell WHY a pin was refused
  * without parsing the prose `cause=` of a compile failure.
  *
  * Every token is lower_snake_case and free of quotes/whitespace so it survives
  * the single-quoted `key='value'` log format unchanged (see
  * [[org.openivm.spark.telemetry.KvLogValue]]).
  */
object TimeTravelPinReason {

  /** [[TimeTravelPinStatus.Applied]]: every user pin bound to exactly one
    * tracked source and that source is frozen at the pinned snapshot.
    */
  val PinsResolved: String = "pins_resolved"

  /** [[TimeTravelPinStatus.NotApplicable]]: the body carries no user pin. */
  val NoUserPin: String = "no_user_pin"

  /** Refused: the same source is read at two versions, pinned in one place and
    * live in another, or pinned to a non-literal/moving value.
    */
  val UnsupportedPinShape: String = "unsupported_pin_shape"

  /** Refused: a pin binds to zero or to several tracked sources. */
  val PinNotResolvedToSingleSource: String = "pin_not_resolved_to_single_source"

  /** Refused: the body carries a pin but the view tracks no source at all, so
    * no relation can be proven frozen.
    */
  val NoTrackedSources: String = "no_tracked_sources"

  /** Refused: a persisted/recorded status was not a contract value. */
  val UnknownPinStatus: String = "unknown_pin_status"

  val All: Seq[String] = Seq(
    PinsResolved,
    NoUserPin,
    UnsupportedPinShape,
    PinNotResolvedToSingleSource,
    NoTrackedSources,
    UnknownPinStatus
  )

  /** Canonical form of a persisted / logged reason, or `None` when it is
    * absent, blank or not part of the vocabulary. Unlike the status, an
    * unknown reason is only a diagnostic and is dropped rather than escalated —
    * the status it accompanies is what guards act on.
    */
  def normalize(raw: String): Option[String] =
    Option(raw)
      .map(_.trim.toLowerCase(Locale.ROOT))
      .filter(All.contains)
}
