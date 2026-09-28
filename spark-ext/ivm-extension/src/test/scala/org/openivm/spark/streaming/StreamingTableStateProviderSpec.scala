package org.openivm.spark.streaming

import org.apache.hadoop.fs.Path
import org.scalatest.funspec.AnyFunSpec

class StreamingTableStateProviderSpec extends AnyFunSpec with StreamingTableTestFixture {

  private val StateProviderKey = "spark.sql.streaming.stateStore.providerClass"
  private val RocksDbProvider =
    "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider"

  describe("native state-store provider delegation") {
    it("uses the caller-configured RocksDB provider and restores state from the native checkpoint") {
      withStateProvider(Some(RocksDbProvider)) {
        val source = "strt_rocks_source"
        val target = "strt_rocks_target"
        createSource(source)
        val query = readStream(source).groupBy("part").count()

        val first = createStreaming(
          target,
          query,
          s"SELECT part, count(*) AS count FROM STREAM $source GROUP BY part",
          options = Map("outputMode" -> "complete")
        )
        appendRows(source, "(1, 'one', 'east'), (2, 'two', 'east'), (3, 'three', 'west')")
        process(first)

        val state = new Path(targetPath(target), "_openivm-checkpoint/state")
        val fs    = state.getFileSystem(spark.sessionState.newHadoopConf())
        spark.conf.getOption(StateProviderKey) shouldBe Some(RocksDbProvider)
        fs.exists(state) shouldBe true
        assertBagEqual(
          target,
          "SELECT 'east' AS part, CAST(2 AS BIGINT) AS count UNION ALL " +
            "SELECT 'west', CAST(1 AS BIGINT)"
        )

        StreamingTableManager.stop(spark, Seq(target))
        val resumed = createStreaming(
          target,
          query,
          s"SELECT part, count(*) AS count FROM STREAM $source GROUP BY part",
          options = Map("outputMode" -> "complete")
        )
        appendRows(source, "(4, 'four', 'east')")
        process(resumed)
        assertBagEqual(
          target,
          "SELECT 'east' AS part, CAST(3 AS BIGINT) AS count UNION ALL " +
            "SELECT 'west', CAST(1 AS BIGINT)"
        )
      }
    }

    it("uses Spark's default state provider when the caller leaves it unset") {
      withStateProvider(None) {
        val source = "strt_default_state_source"
        val target = "strt_default_state_target"
        createSource(source)
        val query                  = readStream(source).groupBy("part").count()
        val explicitProviderBefore = spark.conf.getAll.get(StateProviderKey)

        val status = createStreaming(
          target,
          query,
          s"SELECT part, count(*) AS count FROM STREAM $source GROUP BY part",
          options = Map("outputMode" -> "complete")
        )
        appendRows(source, "(1, 'one', 'north'), (2, 'two', 'north')")
        process(status)

        val state = new Path(targetPath(target), "_openivm-checkpoint/state")
        val fs    = state.getFileSystem(spark.sessionState.newHadoopConf())
        spark.conf.getAll.get(StateProviderKey) shouldBe explicitProviderBefore
        fs.exists(state) shouldBe true
        assertBagEqual(target, "SELECT 'north' AS part, CAST(2 AS BIGINT) AS count")
      }
    }
  }

  private def withStateProvider[A](provider: Option[String])(body: => A): A = {
    val previous = spark.conf.getOption(StateProviderKey)
    provider match {
      case Some(value) => spark.conf.set(StateProviderKey, value)
      case None        => spark.conf.unset(StateProviderKey)
    }
    try body
    finally
      previous match {
        case Some(value) => spark.conf.set(StateProviderKey, value)
        case None        => spark.conf.unset(StateProviderKey)
      }
  }
}
