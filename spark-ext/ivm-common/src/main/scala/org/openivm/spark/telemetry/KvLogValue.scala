package org.openivm.spark.telemetry

/** Renders a value into the single-quoted `key='value'` shape the
  * `[openivm-mv]` classification lines use.
  *
  * Those lines are NOT JSON. Downstream readers tokenize them with a
  * `(\w+)='([^']*)'`-style scanner, so a value that itself contains `'` ends
  * the field early and the reader silently keeps a TRUNCATED value: a
  * `TIMESTAMP AS OF '2024-01-01'` pin rendered verbatim reads back as
  * `t=TIMESTAMP AS OF `, dropping the pinned value a pin audit exists to
  * check. A newline is just as fatal for a line-oriented reader.
  *
  * Sanitizing here keeps the log parseable; the value persisted in MV metadata
  * is NOT sanitized, because the fail-closed CREATE-vs-REFRESH comparison must
  * see the clause exactly as the user wrote it.
  */
object KvLogValue {

  /** `'` is replaced (not doubled/escaped) because the reader's character class
    * excludes the quote character itself — any surviving `'` truncates the
    * field regardless of what precedes it.
    */
  private val QuoteReplacement: Char = '"'

  /** Log-safe form of `value`: single quotes become double quotes and every
    * control character (newlines, carriage returns, tabs) becomes a space.
    */
  def sanitize(value: String): String =
    if (value == null) ""
    else
      value.map {
        case '\''             => QuoteReplacement
        case c if c.isControl => ' '
        case c                => c
      }

  /** True when `value` survives [[sanitize]] unchanged, i.e. it can be logged
    * verbatim. Vocabulary constants are asserted against this.
    */
  def isSafe(value: String): Boolean = value != null && sanitize(value) == value

  /** `key='sanitized value'`. */
  def render(key: String, value: String): String = s"$key='${sanitize(value)}'"

  /** `render`, or `""` when there is nothing to report — keeps optional fields
    * off the line entirely instead of emitting an empty-looking value.
    */
  def renderIfPresent(key: String, value: String): String =
    if (value == null || value.trim.isEmpty) "" else s" ${render(key, value)}"
}
