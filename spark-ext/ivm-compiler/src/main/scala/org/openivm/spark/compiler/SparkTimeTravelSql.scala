package org.openivm.spark.compiler

import java.util.Locale

import org.apache.spark.sql.catalyst.analysis.{RelationTimeTravel, UnresolvedRelation}
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.openivm.spark.common.{TimeTravelPinReason, TimeTravelPinStatus}

/** Splits Spark/Delta snapshot pins (`… VERSION AS OF <v>` / `… TIMESTAMP AS OF
  * <ts>`, a.k.a. Spark's `temporalClause`) out of a materialized-view body.
  *
  * ## Why this exists
  *
  * A snapshot pin is a Spark/Delta STORAGE concern: it selects which committed
  * version of a Delta table a relation reads. It carries no information the
  * OpenIVM/LPTS compiler can use — the compile bridge registers schema-only,
  * row-less DuckDB tables ([[OpenIvmCompiler.compile]]), so there is no
  * snapshot history on the DuckDB side to pin to. DuckDB rejects Spark's
  * spelling outright at parse time:
  *
  * {{{
  * Parser Error: syntax error at or near "as"
  * LINE 1: … FROM billing_meter_dim VERSION AS OF 366 GROUP BY region;
  *                                          ^
  * }}}
  *
  * and its own DuckLake spelling (`AT (VERSION => 366)`) parses but then fails
  * to bind (`Binder Error: Catalog type does not support time travel`) against
  * the bridge's plain in-memory tables. Either way the whole view is demoted
  * `COMPILE_FAILED -> FULL_REFRESH`, so every refresh — including a refresh
  * with an empty delta — re-executes the entire view body.
  *
  * The pin is therefore stripped from the compile-bridge COPY of the body only.
  * It is NOT stripped from anything Spark executes:
  *   - `MvMetadata.querySql` keeps the user's SQL verbatim, so the FULL_REFRESH
  *     `INSERT OVERWRITE` path stays pinned;
  *   - [[OpenIvmCompiler.parseInitialLoadSql]] re-applies the pin to the
  *     initial-load SELECT openivm emits, so the CTAS at CREATE loads the
  *     pinned snapshot rather than live data;
  *   - `SparkRefreshRewriter` re-applies the pin to every live-source read in
  *     the compiled refresh program (`memory.main.<source>`), so a mixed
  *     pinned/live join still reads the pinned side at its pinned version.
  *
  * ## Correctness of the split
  *
  * The clause is located with a Spark-dialect, quote/comment-aware scanner —
  * this object deliberately does NOT re-implement a SQL parser. Spark's own
  * parser ([[CatalystSqlParser]]) is the AUTHORITY on what was pinned; the
  * scanner only supplies the text surgery. Every split must satisfy:
  *
  *   1. the de-pinned SQL must parse,
  *   2. it must contain no remaining `RelationTimeTravel` node,
  *   3. its multiset of relation identifiers must be identical to the original's
  *      (source-table identity is preserved — nothing is renamed or dropped),
  *   4. the lifted pins must correspond ONE FOR ONE to the original's
  *      `RelationTimeTravel` nodes: same relation identifiers, same frozen
  *      values, same count. A pin the scanner bound to the wrong token (a word
  *      inside a comment, say) or a pin the scanner failed to see therefore
  *      fails the split instead of silently resolving to no source.
  *
  * If any check fails the ORIGINAL SQL is returned unchanged, so the compile
  * bridge fails loudly exactly as it does today rather than silently compiling
  * something that is not what the user wrote.
  */
object SparkTimeTravelSql {

  /** One snapshot pin: the relation reference exactly as the user wrote it
    * (`billing_meter_dim`, `arc_sql_db_bi.billing_meter_dim`, `` `db`.`t` ``)
    * plus the temporal clause text (`VERSION AS OF 366`).
    */
  final case class SnapshotPin(tableRef: String, clause: String) {

    /** Last segment of [[tableRef]], unquoted and lower-cased. */
    def shortName: String = SparkTimeTravelSql.identifierSegments(tableRef).last

    /** Unquoted, lower-cased segments of [[tableRef]]. */
    def segments: Seq[String] = SparkTimeTravelSql.identifierSegments(tableRef)
  }

  /** Result of [[split]]: the de-pinned SQL plus every pin removed from it.
    * `pins` is empty exactly when `sql` is the untouched input.
    */
  final case class Split(sql: String, pins: Seq[SnapshotPin])

  /** A refused pin shape: the stable
    * [[org.openivm.spark.common.TimeTravelPinReason]] token consumers gate on,
    * plus the operator-facing prose that names the offending relation.
    */
  final case class PinRefusal(reason: String, detail: String)

  /** The snapshot-pin telemetry contract for one view body.
    *
    * @param status  [[org.openivm.spark.common.TimeTravelPinStatus]] value.
    * @param pins    `<qualified source>=<clause>` identity of every frozen
    *                relation, sorted; empty unless `status` is `APPLIED`.
    * @param reason  [[org.openivm.spark.common.TimeTravelPinReason]] token.
    * @param detail  operator-facing prose, present only for a refusal.
    */
  final case class PinTelemetry(
      status: String,
      pins: Seq[String],
      reason: String,
      detail: Option[String]
  )

  /** Validated replacement of every immutable VERSION pin in a view body. */
  final case class VersionRepin(
      querySql: String,
      currentVersions: Map[String, Long],
      targetVersions: Map[String, Long],
      pins: Seq[String]
  )

  /** Test-only pin-resolution contract; production callers do not yet use it. */
  private[compiler] final case class SourceIdentity(
      alias: String,
      deltaLogDataPath: String,
      deltaTableMetadataId: String
  ) {
    def matchesAlias(candidate: String): Boolean = alias == candidate
  }

  private[compiler] final case class ResolvedSnapshotPin(
      pin: SnapshotPin,
      sqlVisibleSource: SourceIdentity,
      operationalSource: SourceIdentity
  ) {
    def emitsResolved: String = operationalSource.alias
  }

  private[compiler] sealed trait PinIdentityOperation

  private[compiler] object PinIdentityOperation {
    case object Create           extends PinIdentityOperation
    case object IdempotentCreate extends PinIdentityOperation
    case object Refresh          extends PinIdentityOperation
    case object Advance          extends PinIdentityOperation
    case object DryCompile       extends PinIdentityOperation
    case object DryRewrite       extends PinIdentityOperation
  }

  private[compiler] sealed trait PinBindingCheckpoint

  private[compiler] object PinBindingCheckpoint {
    case object CreateBeforeWrite              extends PinBindingCheckpoint
    case object CreateAfterWrite               extends PinBindingCheckpoint
    case object RefreshBeforeApply             extends PinBindingCheckpoint
    case object RefreshAfterStagingBeforeMerge extends PinBindingCheckpoint
    case object RefreshAfterApply              extends PinBindingCheckpoint
  }

  private[compiler] final case class PinBindingCheckpointFailure(
      detail: String,
      createArtifactsCleaned: Boolean,
      refreshRestored: Boolean,
      watermarksUnchanged: Boolean,
      consumedChangesUnchanged: Boolean,
      ctasRetried: Boolean,
      preVersionUnchanged: Boolean,
      createPostCheckOutsideRetry: Boolean,
      refreshMarkersUnchanged: Boolean
  )

  private[compiler] sealed trait PinRewriteSurface

  private[compiler] object PinRewriteSurface {
    case object UserFullQuery               extends PinRewriteSurface
    case object CompilerInitialLoad         extends PinRewriteSurface
    case object SparkRefreshRewriterEmitted extends PinRewriteSurface
  }

  private[compiler] final case class PathBoundRewrite(
      sql: String,
      pinnedOccurrenceCount: Int
  )

  private[compiler] sealed trait BindingSite

  private[compiler] object BindingSite {
    case object CreateCompileRequest               extends BindingSite
    case object DryCompileCompileRequest           extends BindingSite
    case object CreateInitialLoad                  extends BindingSite
    case object DryCompileInitialLoad              extends BindingSite
    case object RefreshCompileRequest              extends BindingSite
    case object RefreshFreshSchemasShortToQual     extends BindingSite
    case object CreatePinTelemetry                 extends BindingSite
    case object CreatePersistedIdentity            extends BindingSite
    case object RefreshPinTelemetry                extends BindingSite
    case object RefreshFrozenSourceSelection       extends BindingSite
    case object RefreshRewriterSnapshotPins        extends BindingSite
    case object AdvanceExternalSourceIdentifiers   extends BindingSite
    case object AdvancePersistedIdentityValidation extends BindingSite

    val CompilerSites: Seq[BindingSite] = Seq(
      CreateCompileRequest,
      DryCompileCompileRequest,
      CreateInitialLoad,
      DryCompileInitialLoad,
      RefreshCompileRequest,
      RefreshFreshSchemasShortToQual
    )

    val PinSites: Seq[BindingSite] = Seq(
      CreatePinTelemetry,
      CreatePersistedIdentity,
      RefreshPinTelemetry,
      RefreshFrozenSourceSelection,
      RefreshRewriterSnapshotPins,
      AdvanceExternalSourceIdentifiers,
      AdvancePersistedIdentityValidation
    )
  }

  /** Test-only pin-binding contract; production callers do not yet use it. */
  private[compiler] final case class PinResolutionFailure(
      pin: SnapshotPin,
      detail: String
  )

  private[compiler] final case class ResolvedSnapshotPinBindings(
      sourceTables: Seq[String],
      sourceIdentities: Seq[SourceIdentity],
      pins: Seq[ResolvedSnapshotPin],
      resolutionFailures: Seq[PinResolutionFailure] = Seq.empty
  )

  /** A pin as the scanner lifted it, plus the parsed clause KIND and VALUE.
    * Those two are what binds the pin to the `RelationTimeTravel` node Spark's
    * parser produced for the same relation — the clause TEXT is user-facing and
    * deliberately not normalised beyond comment/whitespace cleanup.
    */
  private final case class PinnedRef(
      pin: SnapshotPin,
      kind: String,
      value: String,
      clauseStart: Int,
      clauseEnd: Int
  ) {
    def identity: String = identityKey(pin.segments, kind, value)
  }

  private final case class Scanned(sql: String, refs: Seq[PinnedRef])

  /** A temporal clause as located in the source text. */
  private final case class ParsedClause(start: Int, end: Int, text: String, kind: String, value: String)

  private val VersionKind   = "version"
  private val TimestampKind = "timestamp"

  /** Relation + frozen value, the identity a lifted pin and a parsed
    * `RelationTimeTravel` node must agree on.
    */
  private def identityKey(segments: Seq[String], kind: String, value: String): String =
    s"${segments.mkString(".")}@$kind:$value"

  /** Cheap pre-filter so the scanner + parser round-trip only runs for bodies
    * that plausibly contain a temporal clause. Matches inside string literals
    * too — that is fine, it only gates the precise pass below. Comments count
    * as trivia between the keywords, exactly as they do for Spark's lexer.
    */
  private val Trivia = """(?:\s|--[^\n\r]*+|/\*(?:[^*]|\*(?!/))*+\*/)"""

  private val TemporalClauseGuard =
    s"""(?i)\\b(?:SYSTEM_VERSION|VERSION|SYSTEM_TIME|TIMESTAMP)$Trivia++AS$Trivia++OF\\b""".r

  private val VersionKeywords   = Seq("SYSTEM_VERSION", "VERSION")
  private val TimestampKeywords = Seq("SYSTEM_TIME", "TIMESTAMP")

  /** Keywords that can never start a temporal-clause VALUE. Guards against
    * mis-reading a query like `SELECT timestamp AS of FROM t` (where `of` is a
    * column alias) as a pin.
    */
  private val ValueStopKeywords: Set[String] =
    Set(
      "from",
      "where",
      "group",
      "order",
      "having",
      "limit",
      "join",
      "inner",
      "outer",
      "left",
      "right",
      "full",
      "cross",
      "semi",
      "anti",
      "on",
      "using",
      "union",
      "intersect",
      "except",
      "select",
      "with",
      "as",
      "and",
      "or",
      "when",
      "then",
      "else",
      "end",
      "window",
      "qualify",
      "cluster",
      "distribute",
      "sort",
      "lateral",
      "natural",
      "tablesample",
      "pivot",
      "unpivot"
    )

  /** True when `sql` plausibly carries a Spark snapshot pin. */
  def hasSnapshotPin(sql: String): Boolean =
    sql != null && sql.nonEmpty && TemporalClauseGuard.findFirstIn(sql).isDefined && split(sql).pins.nonEmpty

  /** True when `sql` pins a source to a snapshot in a shape the bridge refuses
    * to lift out. See [[unsupportedSnapshotPinReason]], which also explains why
    * this must not be left to a downstream parser.
    */
  def hasUnsupportedSnapshotPin(sql: String): Boolean = unsupportedSnapshotPinReason(sql, Nil).isDefined

  /** Why `sql` carries a snapshot pin OpenIVM cannot maintain incrementally, or
    * `None` when every pin (if any) is one it can honor. Prose form of
    * [[pinRefusal]] — see there for the full rule set.
    */
  def unsupportedSnapshotPinReason(
      sql: String,
      qualifiedSources: Seq[String],
      requireTrackedSources: Boolean = false
  ): Option[String] =
    pinRefusal(sql, qualifiedSources, requireTrackedSources).map(_.detail)

  /** Structured refusal for `sql`, or `None` when every pin (if any) is one
    * OpenIVM can honor.
    *
    * OpenIVM re-applies a pin per SOURCE, so it cannot honor:
    *
    *   - the same source read at two different versions, or pinned in one place
    *     and read live in another (including through a CTE that shadows the
    *     pinned name) — whichever single clause it picked would freeze or
    *     unfreeze the other read;
    *   - a MOVING pin value (`TIMESTAMP AS OF current_timestamp()`) — the
    *     relation would be frozen at compile time and then maintained against a
    *     different snapshot on every refresh;
    *   - a pin the scanner and Spark's parser do not agree on;
    *   - a pin that does not resolve to exactly one tracked source of the view
    *     (`qualifiedSources`) — a pin that resolves to none would silently
    *     maintain a frozen relation from live rows, and one that resolves to
    *     several would freeze relations the user did not pin;
    *   - with `requireTrackedSources`, a pinned body whose view tracks NO
    *     source at all: nothing can be proven frozen, so reporting the pin as
    *     honored would be an unbacked claim. Callers that only ask about the
    *     pin SHAPE (no source list to check against) leave it `false`.
    *
    * Historically DuckDB's parser caught the un-liftable shapes for us — the
    * un-split body still carried `VERSION AS OF`, so the compile aborted and the
    * view fell back to FULL_REFRESH, which re-executes the user's pinned body
    * verbatim and is therefore correct. An LPTS front-end that ACCEPTS Spark's
    * `temporalClause` removes that accident: the compile would succeed, no pin
    * would be registered, and the incremental program would silently read live
    * rows for a frozen relation. The bridge refuses these bodies itself so the
    * fallback does not depend on a downstream parser rejecting them.
    *
    * Bodies `CatalystSqlParser` cannot parse are NOT refused here: without a
    * parse there is no evidence of a real pin, so they are passed through and
    * DuckDB decides, exactly as before.
    */
  def pinRefusal(
      sql: String,
      qualifiedSources: Seq[String],
      requireTrackedSources: Boolean = false
  ): Option[PinRefusal] = {
    if (sql == null || sql.isEmpty) return None
    if (TemporalClauseGuard.findFirstIn(sql).isEmpty) return None
    val pins = split(sql).pins
    if (pins.isEmpty)
      if (parsePlan(sql).exists(plan => timeTravelCount(plan) > 0))
        Some(
          PinRefusal(
            TimeTravelPinReason.UnsupportedPinShape,
            "a source is read at two different versions, pinned in one place and read live in another, " +
              "or pinned to a value that is not a stable literal"
          )
        )
      else None
    else if (qualifiedSources.isEmpty)
      if (requireTrackedSources)
        Some(
          PinRefusal(
            TimeTravelPinReason.NoTrackedSources,
            "the view pins a source to a snapshot but tracks no source at all, so no relation can be " +
              "proven frozen at the pinned snapshot"
          )
        )
      else None
    else
      unresolvedPins(pins, qualifiedSources).headOption.map { pin =>
        PinRefusal(
          TimeTravelPinReason.PinNotResolvedToSingleSource,
          s"the pin on '${pin.tableRef}' does not resolve to exactly one tracked source " +
            s"(sources: ${qualifiedSources.distinct.sorted.mkString(", ")})"
        )
      }
  }

  /** Split every snapshot pin out of `sql`.
    *
    * Returns `Split(sql, Nil)` unchanged when there is no pin, when the scanner
    * cannot recognise the clause shape, when the same source is read at more
    * than one version (or both pinned and live), or when the parser cross-check
    * rejects the rewrite. Those bodies are reported by
    * [[unsupportedSnapshotPinReason]] and refused by the compile bridge, because
    * re-applying one clause to every read of that source would silently produce
    * wrong rows; `COMPILE_FAILED -> FULL_REFRESH` re-executes the pinned body
    * verbatim and stays correct.
    */
  def split(sql: String): Split = {
    if (sql == null || sql.isEmpty) return Split(sql, Nil)
    if (TemporalClauseGuard.findFirstIn(sql).isEmpty) return Split(sql, Nil)
    val scanned = scan(sql)
    if (scanned.refs.isEmpty) return Split(sql, Nil)
    if (!pinsAreUnambiguous(scanned.refs)) return Split(sql, Nil)
    if (!verifiesAgainstSparkParser(sql, scanned.sql, scanned.refs)) Split(sql, Nil)
    else Split(scanned.sql, scanned.refs.map(_.pin))
  }

  /** False when one source carries two different pins (a cross-version
    * self-join): the Spark side re-applies a pin per SOURCE, so there is no
    * single version to freeze that relation at. Two spellings of the SAME
    * frozen value (`VERSION AS OF 2` / `version as of '2'`) are one pin.
    */
  private def pinsAreUnambiguous(refs: Seq[PinnedRef]): Boolean =
    refs.groupBy(_.pin.segments).forall { case (_, group) => group.map(_.identity).distinct.size == 1 }

  /** De-pinned copy of `sql` for the DuckDB compile bridge. */
  def stripSnapshotPins(sql: String): String = split(sql).sql

  /** Resolve the pins in `sql` against a view's tracked source tables.
    *
    * A pin and a source name identify the same relation when one's segment
    * chain is a suffix of the other's: the tracker may hold a source as
    * `db.table` while the body wrote a bare `table` (the sharp edge documented
    * in `MaterializedViewCommands.postRefreshCleanup`), or the compile request
    * may hold the bare name while the body qualified it. Disagreeing qualifiers
    * (`other_db.src` vs `default.src`) never match.
    *
    * Callers that act on the result must first check [[unresolvedPins]]: a pin
    * that matches no source (or several) silently drops out of this map, which
    * on the refresh path means maintaining a frozen relation from live rows.
    *
    * @return qualified source table name → temporal clause text
    */
  def pinsByQualifiedSource(sql: String, qualifiedSources: Seq[String]): Map[String, String] = {
    val pins = split(sql).pins
    if (pins.isEmpty) return Map.empty
    qualifiedSources.flatMap { qualified =>
      pins
        .find(pin => referToSameRelation(pin, qualified))
        .map(pin => qualified -> pin.clause)
    }.toMap
  }

  /** Pins in `sql` that do NOT resolve to exactly one of `qualifiedSources`. */
  def unresolvedPins(sql: String, qualifiedSources: Seq[String]): Seq[SnapshotPin] =
    unresolvedPins(split(sql).pins, qualifiedSources)

  /** Telemetry of the user-authored pins in `sql`, evaluated against the view's
    * tracked sources.
    *
    * This is the ONLY sanctioned way to derive
    * [[org.openivm.spark.common.TimeTravelPinStatus]], and it is deliberately
    * OPERATION-INVARIANT: CREATE and REFRESH call it with the same two inputs
    * (the user's body — `MvMetadata.querySql` — and the view's tracked sources),
    * so the same view cannot report one status at CREATE and another at
    * REFRESH. It never reads compiled or generated SQL, whose delta statements
    * carry no temporal clause even when every source read in them is re-pinned.
    *
    *   - `COMPILE_FAILED` when a pin is present but un-maintainable
    *     ([[pinRefusal]] — checked first, because those bodies deliberately lift
    *     NO pin, and with `requireTrackedSources` so a pinned body with no
    *     tracked source can never be reported as honored);
    *   - `APPLIED` when every pin lifted and resolved to exactly one source, so
    *     the engine freezes that source at the pinned snapshot;
    *   - `NOT_APPLICABLE` when the body carries no pin at all.
    *
    * A refused body reports NO pin identity: nothing was frozen, so persisting
    * `<source>=<clause>` entries for it would claim otherwise.
    */
  def pinTelemetry(sql: String, qualifiedSources: Seq[String]): PinTelemetry =
    pinRefusal(sql, qualifiedSources, requireTrackedSources = true) match {
      case Some(refusal) =>
        PinTelemetry(TimeTravelPinStatus.CompileFailed, Seq.empty, refusal.reason, Some(refusal.detail))
      case None if hasSnapshotPin(sql) =>
        PinTelemetry(
          TimeTravelPinStatus.Applied,
          pinIdentity(sql, qualifiedSources),
          TimeTravelPinReason.PinsResolved,
          None
        )
      case None =>
        PinTelemetry(TimeTravelPinStatus.NotApplicable, Seq.empty, TimeTravelPinReason.NoUserPin, None)
    }

  /** Test-only observation seam for resolving already-validated snapshot pins
    * to operational source identities. Resolution is intentionally not
    * implemented yet.
    */
  private[compiler] def pinTelemetry(
      sql: String,
      operationalSources: Seq[String],
      resolvedPins: Seq[ResolvedSnapshotPin]
  ): PinTelemetry =
    pinTelemetry(sql, operationalSources)

  private[compiler] def pinTelemetry(
      sql: String,
      operationalSources: Seq[String],
      resolvedPins: Seq[ResolvedSnapshotPin],
      persistedPins: Seq[ResolvedSnapshotPin]
  ): PinTelemetry =
    pinTelemetry(sql, operationalSources)

  private[compiler] def pinTelemetry(
      sql: String,
      bindings: ResolvedSnapshotPinBindings,
      persistedPins: Seq[ResolvedSnapshotPin]
  ): PinTelemetry =
    pinTelemetry(sql, bindings.sourceTables)

  /** Test-only validation seam; physical-identity checks are intentionally not
    * implemented yet.
    */
  private[compiler] def validateResolvedSnapshotPins(
      operation: PinIdentityOperation,
      bindings: ResolvedSnapshotPinBindings,
      persistedPins: Option[Seq[ResolvedSnapshotPin]]
  ): Either[String, Unit] =
    Right(())

  /** Test-only structured-property seam; persistence is intentionally not
    * implemented yet.
    */
  private[compiler] val PinnedSourceIdentitiesPropertyKey: String =
    "_ivm_pinned_source_identities"
  private[compiler] val MaxPinnedSourceIdentitiesPropertyBytes: Int = 65536

  private[compiler] def pinnedSourceIdentityProperties(
      bindings: ResolvedSnapshotPinBindings
  ): Map[String, String] =
    Map.empty

  private[compiler] def readPinnedSourceIdentityProperties(
      properties: Map[String, String],
      currentBindings: ResolvedSnapshotPinBindings
  ): Either[String, Seq[ResolvedSnapshotPin]] =
    Left("pinned source identities are not implemented")

  private[compiler] def bindingFor(
      bindings: ResolvedSnapshotPinBindings,
      site: BindingSite
  ): ResolvedSnapshotPinBindings =
    bindings

  private[compiler] def rewriteSnapshotPinsByDataPath(
      sql: String,
      bindings: ResolvedSnapshotPinBindings,
      persistedPins: Seq[ResolvedSnapshotPin]
  ): Either[String, String] =
    Left("path-bound snapshot pin rewriting is not implemented")

  private[compiler] def rewritePinnedReadSurface(
      surface: PinRewriteSurface,
      sql: String,
      bindings: ResolvedSnapshotPinBindings,
      persistedPins: Seq[ResolvedSnapshotPin]
  ): Either[String, PathBoundRewrite] =
    Left("path-bound surface rewriting is not implemented")

  private[compiler] def rewriteEmittedSnapshotPinsByDataPath(
      emittedSql: Seq[String],
      bindings: ResolvedSnapshotPinBindings,
      persistedPins: Seq[ResolvedSnapshotPin]
  ): Either[String, Seq[String]] =
    Left("path-bound emitted SQL rewriting is not implemented")

  private[compiler] def verifySnapshotPinBindingsAt(
      operation: PinIdentityOperation,
      checkpoint: PinBindingCheckpoint,
      bindings: ResolvedSnapshotPinBindings,
      persistedPins: Seq[ResolvedSnapshotPin]
  ): Either[PinBindingCheckpointFailure, Unit] =
    Right(())

  /** Status component of [[pinTelemetry]]. */
  def pinStatus(sql: String, qualifiedSources: Seq[String]): String =
    pinTelemetry(sql, qualifiedSources).status

  /** Identity of every resolved pin as `<qualified source>=<clause>`, sorted by
    * source. Persisted at CREATE so REFRESH can prove the status it reports
    * still describes the same frozen relations at the same frozen values.
    * Sorted on the rendered entry so it matches the persisted property byte for
    * byte.
    */
  def pinIdentity(sql: String, qualifiedSources: Seq[String]): Seq[String] =
    pinsByQualifiedSource(sql, qualifiedSources).toSeq.map { case (source, clause) => s"$source=$clause" }.sorted

  /** Replace every resolved immutable VERSION pin with the caller's exact
    * source-version map.
    *
    * The map must resolve one-to-one to all and only pinned sources. Targets
    * may equal their current value so retrying a successful command is a
    * no-op, but they may never move backwards. TIMESTAMP pins are deliberately
    * rejected: their source snapshot cannot be advanced by a numeric Delta
    * version without changing the user's pin kind.
    */
  def repinVersions(
      sql: String,
      qualifiedSources: Seq[String],
      requestedVersions: Map[String, Long]
  ): Either[String, VersionRepin] = {
    val validatedSplit = split(sql)
    if (validatedSplit.pins.isEmpty)
      return Left("the materialized-view query has no supported immutable VERSION AS OF source pins")

    val scanned = scan(sql)
    if (
      scanned.refs.isEmpty ||
      !pinsAreUnambiguous(scanned.refs) ||
      !verifiesAgainstSparkParser(sql, scanned.sql, scanned.refs)
    )
      return Left("the materialized-view query contains an unsupported or ambiguous source pin shape")

    if (scanned.refs.exists(_.kind != VersionKind))
      return Left("only VERSION AS OF pins can be advanced; TIMESTAMP AS OF pins are not supported")

    val resolvedRefs = scanned.refs.map { ref =>
      val matches = qualifiedSources.distinct.filter(source => referToSameRelation(ref.pin, source))
      if (matches.size != 1)
        return Left(
          s"the pin on '${ref.pin.tableRef}' resolves to ${matches.size} tracked sources; expected exactly one"
        )
      val version =
        scala.util.Try(ref.value.toLong).toOption.filter(_ >= 0L).getOrElse {
          return Left(s"the VERSION AS OF value '${ref.value}' on '${ref.pin.tableRef}' is not a non-negative integer")
        }
      (ref, matches.head, version)
    }

    val currentVersions = resolvedRefs
      .groupBy(_._2)
      .map { case (source, refs) =>
        val values = refs.map(_._3).distinct
        if (values.size != 1)
          return Left(s"the tracked source '$source' is read at multiple VERSION AS OF values")
        source -> values.head
      }

    val resolvedRequested = requestedVersions.toSeq.map { case (requestedSource, version) =>
      if (version < 0L)
        return Left(s"the requested version for '$requestedSource' must be non-negative")
      val requestedSegments = identifierSegments(requestedSource)
      val matches = qualifiedSources.distinct.filter { source =>
        val sourceSegments = identifierSegments(source)
        sourceSegments.endsWith(requestedSegments) || requestedSegments.endsWith(sourceSegments)
      }
      if (matches.size != 1)
        return Left(
          s"the requested source '$requestedSource' resolves to ${matches.size} tracked sources; expected exactly one"
        )
      matches.head -> version
    }
    val duplicateRequested = resolvedRequested.groupBy(_._1).collectFirst {
      case (source, entries) if entries.size > 1 =>
        source
    }
    duplicateRequested.foreach(source => return Left(s"the requested version map names '$source' more than once"))
    val targetVersions = resolvedRequested.toMap

    val missing = currentVersions.keySet -- targetVersions.keySet
    val extra   = targetVersions.keySet -- currentVersions.keySet
    if (missing.nonEmpty || extra.nonEmpty)
      return Left(
        s"the requested version map must cover all and only pinned sources " +
          s"(missing: ${missing.toSeq.sorted.mkString(", ")}; extra: ${extra.toSeq.sorted.mkString(", ")})"
      )

    currentVersions.toSeq.sortBy(_._1).foreach { case (source, current) =>
      val target = targetVersions(source)
      if (target < current)
        return Left(s"the requested version for '$source' moves backwards from $current to $target")
    }

    val rewritten = new StringBuilder(sql)
    resolvedRefs.sortBy(_._1.clauseStart).reverse.foreach { case (ref, source, _) =>
      rewritten.replace(ref.clauseStart, ref.clauseEnd, s"VERSION AS OF ${targetVersions(source)}")
    }
    val querySql  = rewritten.toString
    val telemetry = pinTelemetry(querySql, qualifiedSources)
    if (telemetry.status != TimeTravelPinStatus.Applied)
      Left(
        s"the rewritten query did not preserve a valid source-pin contract: " +
          telemetry.detail.getOrElse(telemetry.reason)
      )
    else
      Right(
        VersionRepin(
          querySql = querySql,
          currentVersions = currentVersions,
          targetVersions = targetVersions,
          pins = telemetry.pins
        )
      )
  }

  /** Test-only observation seam paired with [[pinTelemetry]] for ADVANCE SOURCE
    * VERSIONS.
    */
  private[compiler] def repinVersions(
      sql: String,
      operationalSources: Seq[String],
      resolvedPins: Seq[ResolvedSnapshotPin],
      requestedVersions: Map[String, Long]
  ): Either[String, VersionRepin] =
    repinVersions(sql, operationalSources, requestedVersions)

  private[compiler] def repinVersions(
      sql: String,
      bindings: ResolvedSnapshotPinBindings,
      persistedPins: Seq[ResolvedSnapshotPin],
      requestedVersions: Map[String, Long]
  ): Either[String, VersionRepin] =
    repinVersions(sql, bindings.sourceTables, requestedVersions)

  private[compiler] def repinVersions(
      sql: String,
      operationalSources: Seq[String],
      resolvedPins: Seq[ResolvedSnapshotPin],
      persistedPins: Seq[ResolvedSnapshotPin],
      requestedVersions: Map[String, Long]
  ): Either[String, VersionRepin] =
    repinVersions(sql, operationalSources, requestedVersions)

  private def unresolvedPins(pins: Seq[SnapshotPin], qualifiedSources: Seq[String]): Seq[SnapshotPin] = {
    val sources = qualifiedSources.distinct
    pins.filter(pin => sources.count(source => referToSameRelation(pin, source)) != 1)
  }

  private def referToSameRelation(pin: SnapshotPin, qualifiedSource: String): Boolean = {
    val sourceSegments = identifierSegments(qualifiedSource)
    sourceSegments.endsWith(pin.segments) || pin.segments.endsWith(sourceSegments)
  }

  /** Same as [[pinsByQualifiedSource]] but keyed by the short (last-segment)
    * table name — the key space `SparkRefreshRewriter` uses when it expands
    * openivm's `memory.main.<source>` references.
    */
  def pinsByShortSource(sql: String, qualifiedSources: Seq[String]): Map[String, String] =
    pinsByQualifiedSource(sql, qualifiedSources).map { case (qualified, clause) =>
      identifierSegments(qualified).last -> clause
    }

  /** Unquoted, lower-cased dot-separated segments of a (possibly backtick- or
    * double-quote-quoted) table reference.
    */
  private[compiler] def identifierSegments(tableRef: String): Seq[String] = {
    val segments = scala.collection.mutable.ArrayBuffer.empty[String]
    val current  = new StringBuilder
    var i        = 0
    var quote    = '\u0000'
    while (i < tableRef.length) {
      val c = tableRef.charAt(i)
      if (quote != '\u0000') {
        if (c == quote) {
          if (i + 1 < tableRef.length && tableRef.charAt(i + 1) == quote) { current += c; i += 2 }
          else { quote = '\u0000'; i += 1 }
        } else { current += c; i += 1 }
      } else if (c == '`' || c == '"') { quote = c; i += 1 }
      else if (c == '.') { segments += current.toString; current.setLength(0); i += 1 }
      else { current += c; i += 1 }
    }
    segments += current.toString
    segments.map(_.trim.toLowerCase(Locale.ROOT)).toVector
  }

  // ── Scanner ────────────────────────────────────────────────────────────────

  /** Locate and elide every temporal clause outside string literals, quoted
    * identifiers and comments. Pure text surgery — validated by [[split]].
    */
  private def scan(sql: String): Scanned = {
    val out  = new StringBuilder(sql.length)
    val refs = scala.collection.mutable.ArrayBuffer.empty[PinnedRef]
    var i    = 0
    // Length of `out` up to and including the last emitted TOKEN character.
    // Comments (and the whitespace after them) are trivia: Spark allows them
    // between a relation and its temporal clause, so a word inside one
    // (`FROM t -- freeze at load time\n VERSION AS OF 4`) must never be taken
    // for the pinned relation.
    var refEnd = 0
    while (i < sql.length) {
      val commentEnd = consumeComment(sql, i)
      if (commentEnd > i) {
        out.append(sql.substring(i, commentEnd))
        i = commentEnd
      } else {
        val quotedEnd = consumeQuoted(sql, i)
        if (quotedEnd > i) {
          out.append(sql.substring(i, quotedEnd))
          refEnd = out.length
          i = quotedEnd
        } else {
          parseTemporalClause(sql, i) match {
            case Some(clause) =>
              trailingTableRef(out, refEnd) match {
                case Some(tableRef) =>
                  refs += PinnedRef(
                    SnapshotPin(tableRef, clause.text),
                    clause.kind,
                    clause.value,
                    clause.start,
                    clause.end
                  )
                  elideClause(out, sql, clause.end)
                  refEnd = math.min(refEnd, out.length)
                  i = clause.end
                case None =>
                  refEnd = appendChar(out, sql.charAt(i), refEnd)
                  i += 1
              }
            case None =>
              refEnd = appendChar(out, sql.charAt(i), refEnd)
              i += 1
          }
        }
      }
    }
    if (refs.isEmpty) Scanned(sql, Nil) else Scanned(out.toString, refs.toVector)
  }

  /** Append one character, returning the updated end-of-last-token marker. */
  private def appendChar(out: StringBuilder, c: Char, refEnd: Int): Int = {
    out.append(c)
    if (c.isWhitespace) refEnd else out.length
  }

  /** Drop the elided clause together with the inline whitespace that preceded
    * it, re-inserting a single separator only when the next token needs one.
    * A trailing newline is never removed: it may terminate a line comment.
    */
  private def elideClause(out: StringBuilder, sql: String, clauseEnd: Int): Unit = {
    var trimmed = out.length
    while (trimmed > 0 && isInlineWhitespace(out.charAt(trimmed - 1))) trimmed -= 1
    out.setLength(trimmed)
    val endsWithNewline = trimmed > 0 && (out.charAt(trimmed - 1) == '\n' || out.charAt(trimmed - 1) == '\r')
    val next            = if (clauseEnd < sql.length) sql.charAt(clauseEnd) else ' '
    if (!endsWithNewline && !next.isWhitespace && next != ')' && next != ',' && next != ';') out.append(' ')
  }

  private def isInlineWhitespace(c: Char): Boolean = c.isWhitespace && c != '\n' && c != '\r'

  /** End index (exclusive) of a string literal or quoted identifier starting at
    * `i` — a real token — or `i` itself.
    */
  private def consumeQuoted(sql: String, i: Int): Int = sql.charAt(i) match {
    case '\'' => SparkFunctionShimSql.consumeSparkSingleQuoted(sql, i)
    case '"'  => consumeSparkDoubleQuoted(sql, i)
    case '`'  => consumeBacktickQuoted(sql, i)
    case _    => i
  }

  /** End index (exclusive) of a line or block comment starting at `i` — trivia,
    * never part of a relation reference — or `i` itself.
    */
  private def consumeComment(sql: String, i: Int): Int = sql.charAt(i) match {
    case '-' if i + 1 < sql.length && sql.charAt(i + 1) == '-' =>
      var j = i + 2
      while (j < sql.length && sql.charAt(j) != '\n' && sql.charAt(j) != '\r') j += 1
      j
    case '/' if i + 1 < sql.length && sql.charAt(i + 1) == '*' =>
      var j = i + 2
      while (j + 1 < sql.length && !(sql.charAt(j) == '*' && sql.charAt(j + 1) == '/')) j += 1
      if (j + 1 < sql.length) j + 2 else sql.length
    case _ => i
  }

  private def consumeSparkDoubleQuoted(sql: String, start: Int): Int = {
    var i = start + 1
    while (i < sql.length) {
      sql.charAt(i) match {
        case '\\'                                                  => i += 2
        case '"' if i + 1 < sql.length && sql.charAt(i + 1) == '"' => i += 2
        case '"'                                                   => return i + 1
        case _                                                     => i += 1
      }
    }
    sql.length
  }

  private def consumeBacktickQuoted(sql: String, start: Int): Int = {
    var i = start + 1
    while (i < sql.length) {
      if (sql.charAt(i) == '`') {
        if (i + 1 < sql.length && sql.charAt(i + 1) == '`') i += 2 else return i + 1
      } else i += 1
    }
    sql.length
  }

  /** Match Spark's `temporalClause` at `start`.
    *
    * {{{
    * temporalClause
    *   : FOR? (SYSTEM_VERSION | VERSION)  AS OF version=(INTEGER_VALUE | STRING)
    *   | FOR? (SYSTEM_TIME | TIMESTAMP)   AS OF timestamp=valueExpression
    * }}}
    *
    * @return the clause, or `None` when `start` does not begin one.
    */
  private def parseTemporalClause(sql: String, start: Int): Option[ParsedClause] = {
    if (!isIdentifierStart(sql.charAt(start)) || !hasLeftIdentifierBoundary(sql, start)) return None

    var pos = start
    if (isKeywordAt(sql, pos, "FOR")) pos = skipTrivia(sql, pos + "FOR".length)

    val kindKeyword =
      (VersionKeywords ++ TimestampKeywords).find(isKeywordAt(sql, pos, _)).getOrElse(return None)
    // `SYSTEM_VERSION`/`SYSTEM_TIME` must win over their `VERSION`/`TIME`
    // suffixes; `isKeywordAt` already enforces identifier boundaries, so the
    // first hit in the ordered list is the full keyword.
    pos = skipTrivia(sql, pos + kindKeyword.length)
    if (!isKeywordAt(sql, pos, "AS")) return None
    pos = skipTrivia(sql, pos + "AS".length)
    if (!isKeywordAt(sql, pos, "OF")) return None
    val valueStart = skipTrivia(sql, pos + "OF".length)

    val isVersion = VersionKeywords.contains(kindKeyword)
    val valueEnd =
      if (isVersion) parseVersionValueEnd(sql, valueStart) else parseTimestampValueEnd(sql, valueStart)

    valueEnd.map { end =>
      ParsedClause(
        start = start,
        end = end,
        text = clauseText(sql, start, end),
        kind = if (isVersion) VersionKind else TimestampKind,
        value = unquoteLiteral(sql.substring(valueStart, end).trim)
      )
    }
  }

  /** The clause exactly as the user wrote it, minus comments and with runs of
    * whitespace OUTSIDE string literals collapsed to one space. The result is
    * re-emitted verbatim into the SQL Spark executes, so a line comment inside
    * the clause (`VERSION -- pinned\n AS OF 4`) must not survive the collapse
    * and comment the rest of the statement out.
    */
  private def clauseText(sql: String, start: Int, end: Int): String = {
    val out = new StringBuilder(end - start)
    var i   = start
    while (i < end) {
      val commentEnd = consumeComment(sql, i)
      if (commentEnd > i) {
        appendSeparator(out)
        i = commentEnd
      } else {
        val quotedEnd = consumeQuoted(sql, i)
        if (quotedEnd > i) {
          out.append(sql.substring(i, math.min(quotedEnd, end)))
          i = quotedEnd
        } else if (sql.charAt(i).isWhitespace) {
          appendSeparator(out)
          i += 1
        } else {
          out.append(sql.charAt(i))
          i += 1
        }
      }
    }
    out.toString.trim
  }

  private def appendSeparator(out: StringBuilder): Unit =
    if (out.nonEmpty && out.charAt(out.length - 1) != ' ') out.append(' ')

  /** Strip the quotes of a single-quoted literal and undo its escapes, so a
    * scanned value can be compared with the one Spark's parser produced.
    */
  private def unquoteLiteral(text: String): String = {
    if (text.length < 2 || text.charAt(0) != '\'' || text.charAt(text.length - 1) != '\'') return text
    val out = new StringBuilder(text.length)
    var i   = 1
    val end = text.length - 1
    while (i < end) {
      val c = text.charAt(i)
      if (c == '\\' && i + 1 < end) { out.append(text.charAt(i + 1)); i += 2 }
      else if (c == '\'' && i + 1 < end && text.charAt(i + 1) == '\'') { out.append('\''); i += 2 }
      else { out.append(c); i += 1 }
    }
    out.toString
  }

  /** `INTEGER_VALUE | STRING` — the only two forms Spark's grammar allows after
    * `VERSION AS OF`.
    */
  private def parseVersionValueEnd(sql: String, start: Int): Option[Int] = {
    if (start >= sql.length) return None
    val c = sql.charAt(start)
    if (c == '\'') Some(SparkFunctionShimSql.consumeSparkSingleQuoted(sql, start))
    else if (c.isDigit) {
      var i = start + 1
      while (i < sql.length && sql.charAt(i).isDigit) i += 1
      Some(i)
    } else None
  }

  /** `valueExpression` after `TIMESTAMP AS OF`, restricted to the STABLE
    * literal forms a frozen relation can be defined by: a string literal or a
    * number.
    *
    * Anything else — `current_timestamp()`, `date_sub(current_date(), 1)`, an
    * arithmetic expression — is a MOVING target. OpenIVM freezes a relation ONCE
    * (the pin it re-applies is fixed text, and staged deltas for a frozen source
    * are dropped), so a moving value would pick some snapshot at CREATE and then
    * maintain the view against a different one at every refresh. Those bodies
    * are deliberately not lifted: the bridge refuses them and the view falls
    * back to FULL_REFRESH, which re-evaluates the user's expression each run.
    */
  private def parseTimestampValueEnd(sql: String, start: Int): Option[Int] = {
    if (start >= sql.length) return None
    val c = sql.charAt(start)
    if (c == '\'') return Some(SparkFunctionShimSql.consumeSparkSingleQuoted(sql, start))
    if (!c.isDigit) return None
    var i = start + 1
    while (i < sql.length && (sql.charAt(i).isDigit || sql.charAt(i) == '.')) i += 1
    Some(i)
  }

  /** Trailing dot-separated identifier chain already emitted into `out`, up to
    * `limit` (the end of the last real token — comments are excluded) — the
    * relation the temporal clause pins. `None` when the preceding token is not
    * a plausible relation reference (e.g. a comma, an operator, or a SQL
    * keyword), which makes the caller leave the text untouched.
    *
    * This is a HINT, not the verdict: [[split]] only keeps the pin if Spark's
    * own parser agrees the same relation carries the same clause.
    */
  private def trailingTableRef(out: StringBuilder, limit: Int): Option[String] = {
    var end = math.min(limit, out.length)
    while (end > 0 && out.charAt(end - 1).isWhitespace) end -= 1
    if (end == 0) return None
    var start = end
    var ok    = true
    while (ok && start > 0) {
      val c = out.charAt(start - 1)
      if (c == '`') {
        var open = start - 2
        while (open >= 0 && out.charAt(open) != '`') open -= 1
        if (open < 0) { ok = false }
        else start = open
      } else if (isIdentifierChar(c) || c == '.') start -= 1
      else ok = false
    }
    if (start >= end) return None
    val ref = out.substring(start, end)
    if (ref.startsWith(".") || ref.endsWith(".")) return None
    val segments = identifierSegments(ref)
    if (segments.exists(_.isEmpty)) return None
    if (ValueStopKeywords.contains(segments.last) || segments.last.forall(_.isDigit)) return None
    Some(ref)
  }

  private def skipTrivia(sql: String, start: Int): Int = {
    var i    = start
    var more = true
    while (more && i < sql.length) {
      if (sql.charAt(i).isWhitespace) i += 1
      else {
        val end = consumeComment(sql, i)
        if (end > i) i = end else more = false
      }
    }
    i
  }

  private def isKeywordAt(sql: String, start: Int, keyword: String): Boolean =
    start >= 0 && start + keyword.length <= sql.length &&
      sql.regionMatches(true, start, keyword, 0, keyword.length) &&
      hasLeftIdentifierBoundary(sql, start) &&
      hasRightIdentifierBoundary(sql, start + keyword.length)

  private def isIdentifierStart(c: Char): Boolean = c.isLetter || c == '_'

  private def isIdentifierChar(c: Char): Boolean = c.isLetterOrDigit || c == '_'

  private def hasLeftIdentifierBoundary(sql: String, idx: Int): Boolean =
    idx <= 0 || (!isIdentifierChar(sql.charAt(idx - 1)) && sql.charAt(idx - 1) != '.')

  private def hasRightIdentifierBoundary(sql: String, idx: Int): Boolean =
    idx >= sql.length || !isIdentifierChar(sql.charAt(idx))

  // ── Spark-parser cross-check ───────────────────────────────────────────────

  /** The de-pinned SQL must parse, carry no residual time-travel node,
    * reference exactly the same relations as the original, and account for
    * every pin Spark's parser saw. A source that is pinned somewhere and read
    * live elsewhere is rejected: the Spark side re-pins per source, which would
    * freeze the live read too.
    */
  private def verifiesAgainstSparkParser(original: String, depinned: String, refs: Seq[PinnedRef]): Boolean =
    (parsePlan(original), parsePlan(depinned)) match {
      case (Some(before), Some(after)) =>
        timeTravelCount(before) > 0 &&
        timeTravelCount(after) == 0 &&
        relationIdentifiers(before) == relationIdentifiers(after) &&
        bindsExactlyToParsedPins(before, refs) &&
        !readsPinnedSourceLive(before, refs.map(_.pin))
      case _ => false
    }

  /** Spark's parser — not the scanner — decides WHICH relation a temporal
    * clause pins and WHAT it is frozen at. The lifted pins must therefore be
    * the same multiset of `(relation, kind, value)` as the `RelationTimeTravel`
    * nodes of the parsed original.
    *
    * This is what stops a pin from silently binding to the wrong token (a word
    * inside a comment that sits between the relation and its clause) or from
    * going unnoticed (a clause shape the scanner does not lift, such as a
    * moving `TIMESTAMP AS OF current_timestamp()`): either way the association
    * is not exact, the split is refused, and the view falls back to a
    * FULL_REFRESH of the user's pinned body.
    */
  private def bindsExactlyToParsedPins(plan: LogicalPlan, refs: Seq[PinnedRef]): Boolean =
    parsedPinIdentities(plan).exists(_.sorted == refs.map(_.identity).sorted)

  /** Identity of every `RelationTimeTravel` in `plan`, or `None` when any of
    * them pins something other than a named relation or is frozen at anything
    * other than a literal value (`current_timestamp()` and friends).
    */
  private def parsedPinIdentities(plan: LogicalPlan): Option[Seq[String]] = {
    val identities = collectPlans(plan).collect { case tt: RelationTimeTravel => parsedPinIdentity(tt) }
    if (identities.forall(_.isDefined)) Some(identities.map(_.get)) else None
  }

  private def parsedPinIdentity(travel: RelationTimeTravel): Option[String] = travel.relation match {
    case relation: UnresolvedRelation =>
      val segments = relation.multipartIdentifier.map(_.toLowerCase(Locale.ROOT))
      (travel.version, travel.timestamp) match {
        case (Some(version), None) => Some(identityKey(segments, VersionKind, unquoteLiteral(version)))
        case (None, Some(literal: Literal)) if literal.value != null =>
          Some(identityKey(segments, TimestampKind, literal.value.toString))
        case _ => None
      }
    case _ => None
  }

  /** True when a relation reference outside any `RelationTimeTravel` resolves
    * to one of the pinned sources. CTE names are compared too, so a CTE that
    * shadows a pinned source name conservatively refuses the split.
    */
  private def readsPinnedSourceLive(plan: LogicalPlan, pins: Seq[SnapshotPin]): Boolean = {
    val pinnedSegments  = pins.map(_.segments).distinct
    val nodes           = collectPlans(plan)
    val pinnedRelations = nodes.collect { case tt: RelationTimeTravel => tt.relation }
    nodes
      .collect { case r: UnresolvedRelation if !pinnedRelations.exists(_ eq r) => r }
      .exists { relation =>
        val id = relation.multipartIdentifier.map(_.toLowerCase(Locale.ROOT))
        pinnedSegments.exists(segments => id.endsWith(segments) || segments.endsWith(id))
      }
  }

  private def parsePlan(sql: String): Option[LogicalPlan] =
    try Some(CatalystSqlParser.parsePlan(sql))
    catch { case _: Throwable => None }

  private def timeTravelCount(plan: LogicalPlan): Int =
    collectPlans(plan).count(_.isInstanceOf[RelationTimeTravel])

  private def relationIdentifiers(plan: LogicalPlan): Seq[Seq[String]] =
    collectPlans(plan)
      .collect { case r: UnresolvedRelation => r.multipartIdentifier.map(_.toLowerCase(Locale.ROOT)) }
      .sortBy(_.mkString("."))

  /** Every plan node in `plan`, including nodes Spark's own `collect` does not
    * reach: the relation wrapped by a `RelationTimeTravel` (an
    * `UnresolvedLeafNode`, so its relation is not a plan child) and
    * `innerChildren` such as the CTE definitions of an unresolved `WITH` and the
    * plans of subquery expressions.
    */
  private def collectPlans(plan: LogicalPlan): Seq[LogicalPlan] = {
    val out = scala.collection.mutable.ArrayBuffer.empty[LogicalPlan]
    def visit(p: LogicalPlan): Unit = {
      out += p
      val pinned = p match {
        case tt: RelationTimeTravel => Seq(tt.relation)
        case _                      => Seq.empty[LogicalPlan]
      }
      val inner = p.innerChildren.collect { case lp: LogicalPlan => lp }
      (p.children ++ inner ++ pinned).foreach(visit)
    }
    visit(plan)
    out.toVector
  }
}
