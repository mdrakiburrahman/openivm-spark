package org.openivm.spark.parity.base

import org.openivm.spark.common.ChangeFeedMode

/** Test scaffolding contract for the per-mode mixins.
  *
  * Each parity spec is split into:
  *  - `<Name>Scenarios extends IvmParitySpecBase` — the actual
  *    `describe` / `it` bodies, written against the abstract `sql(...)`
  *    helper so they work under both modes.
  *  - `<Name>Spec extends <Name>Scenarios with InterceptMode` — the
  *    historical behaviour, kept verbatim for backwards-compat.
  *  - `<Name>CdfSpec extends <Name>Scenarios with CdfMode` — exercises
  *    the same `it` blocks against the CDF change-propagation path.
  */
trait IvmParityMode {

  /** Value of `spark.openivm.changeFeed.mode` for this run. */
  def changeFeedMode: ChangeFeedMode

  /** Extra TBLPROPERTIES (without the surrounding parens) injected into
    * every `CREATE TABLE ... USING DELTA` statement that the test runs.
    * Empty string for intercept mode.  For CDF mode this is
    * `'delta.enableChangeDataFeed' = 'true'` so base tables expose change
    * feed rows for the trait under test.
    */
  def cdfTblProps: String

  /** Short label, included in warehouse-dir suffix + Spark app name so two
    * sibling specs (Intercept + Cdf) are easy to disambiguate in logs.
    */
  def modeLabel: String
}

trait InterceptMode extends IvmParityMode {
  override def changeFeedMode: ChangeFeedMode = ChangeFeedMode.Intercept
  override def cdfTblProps: String            = ""
  override def modeLabel: String              = "intercept"
}

trait CdfMode extends IvmParityMode {
  override def changeFeedMode: ChangeFeedMode = ChangeFeedMode.Cdf
  override def cdfTblProps: String            = "'delta.enableChangeDataFeed' = 'true'"
  override def modeLabel: String              = "cdf"
}
