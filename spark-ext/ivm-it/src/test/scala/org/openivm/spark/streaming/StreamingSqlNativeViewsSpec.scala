package org.openivm.spark.streaming

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, View}
import org.apache.spark.sql.catalyst.rules.Rule
import org.scalatest.funspec.AnyFunSpec

/** Models a native catalog analyzer that exposes a resolved view without a View wrapper. */
class NativeViewExpansionTestExtensions extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit =
    extensions.injectPostHocResolutionRule { _ =>
      new Rule[LogicalPlan] {
        override def apply(plan: LogicalPlan): LogicalPlan =
          plan.transformUp { case view: View if view.resolved => view.child }
      }
    }
}

class StreamingSqlNativeViewsSpec extends AnyFunSpec with StreamingSqlTestSupport {
  override protected def sessionExtensions: String =
    super.sessionExtensions + ",org.openivm.spark.streaming.NativeViewExpansionTestExtensions"

  describe("native catalog view expansion") {
    it("streams resolved wrapper-free UNION ALL views using catalog metadata and stable leaf order") {
      val source = "strsql_native_views_source"
      val view   = "strsql_native_views_view"
      val target = "strsql_native_views_target"
      createDeltaSource(source, "id INT, value STRING, part STRING")
      insertRows(source, "(1, 'one', 'p'), (2, 'two', 'p')")
      spark
        .sql(
          s"CREATE VIEW $view AS SELECT CAST(id AS BIGINT) AS id, value, part FROM $source WHERE id <= 2 UNION ALL SELECT CAST(id AS BIGINT), upper(value), part FROM $source WHERE id >= 2"
        )
        .collect()
      spark.sql(s"SELECT * FROM $view").queryExecution.analyzed.exists(_.isInstanceOf[View]) shouldBe false
      spark.catalog.getTable(view).tableType shouldBe "VIEW"
      val declaration =
        s"CREATE STREAMING TABLE $target AS SELECT * FROM STREAM($view) WITH ('maxFilesPerTrigger' = '1')"
      val first = createStreamingSql(declaration)
      process(first)
      assertBagEqual(target, s"SELECT * FROM $view")
      spark.sql(s"ALTER STREAMING TABLE $target STOP").collect()
      val resumed = createStreamingSql(declaration)
      resumed.queryId shouldBe first.queryId
      resumed.definitionHash shouldBe first.definitionHash
      resumed.checkpointLocation shouldBe first.checkpointLocation
      insertRows(source, "(3, 'three', 'p')")
      process(resumed)
      assertBagEqual(target, s"SELECT * FROM $view")
    }
  }
}
