/*
 * Bridge into Spark's `private[sql]` API surface.
 *
 * Lives in the `org.apache.spark.sql` package family so it can call
 * `SparkSession.cloneSession()`, which is package-private to
 * `org.apache.spark.sql`.  Used by RefreshMaterializedViewCommand to give
 * every refresh its own SessionState (in particular, its own temp-view
 * namespace) without sharing those session-local objects with sibling
 * refreshes running in parallel waves.  The clone shares SparkContext
 * and the table-cache manager, so the cost is dominated by a copy of
 * SQLConf and the temp-catalog map.
 */
package org.apache.spark.sql.openivm

import org.apache.spark.sql.SparkSession

object SparkSessionAccess {

  /** Returns a new SparkSession that shares this one's SparkContext and
    * table cache but has its own SQLConf, registered temp views, UDFs,
    * and session-state catalog. Wraps the package-private
    * `SparkSession.cloneSession()` so it is callable from
    * `org.openivm.*`.
    */
  def cloneSession(spark: SparkSession): SparkSession = spark.cloneSession()
}
