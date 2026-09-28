/*
 * Bridge into Spark's `private[sql]` API surface.
 *
 * Used by RefreshMaterializedViewCommand to give
 * every refresh its own SessionState (in particular, its own temp-view
 * namespace) without sharing those session-local objects with sibling
 * refreshes running in parallel waves.  The clone shares SparkContext
 * and the table-cache manager, so the cost is dominated by a copy of
 * SQLConf and the temp-catalog map.
 */
package org.apache.spark.sql.openivm

import org.apache.spark.sql.SparkSession

object SparkSessionAccess {

  /** Returns a cloned SparkSession that shares this one's SparkContext and
    * table cache while copying its SQLConf, registered temp views, UDFs,
    * and session-state catalog.
    */
  def cloneSession(spark: SparkSession): SparkSession = {
    val currentCatalog  = spark.catalog.currentCatalog()
    val currentDatabase = spark.catalog.currentDatabase
    val cloned          = SparkSessionCloneCompatibility.cloneSession(spark)
    cloned.catalog.setCurrentCatalog(currentCatalog)
    cloned.catalog.setCurrentDatabase(currentDatabase)
    cloned
  }

  /** Run `body` with `spark` installed as the thread-local active session,
    * restoring the caller's prior active session afterwards.
    *
    * We intentionally do NOT touch Spark's default session: the active
    * session is thread-local and therefore request-scoped, while mutating the
    * process-wide default session would leak across concurrent Privy workers.
    */
  def withActiveSession[A](spark: SparkSession)(body: => A): A = {
    val previous = SparkSession.getActiveSession
    SparkSession.setActiveSession(spark)
    try body
    finally
      previous match {
        case Some(session) => SparkSession.setActiveSession(session)
        case None          => SparkSession.clearActiveSession()
      }
  }

  /** Clone `spark`, install the clone as the active session for this thread,
    * and run `body` with that isolated session.
    */
  def withIsolatedSession[A](spark: SparkSession)(body: SparkSession => A): A = {
    val cloned = cloneSession(spark)
    withActiveSession(cloned) {
      body(cloned)
    }
  }
}
