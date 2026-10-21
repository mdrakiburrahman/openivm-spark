package org.apache.spark.sql.execution.openivm

import java.util.IdentityHashMap
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.spark.scheduler._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.{ReusedSubqueryExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.execution.ui.{SparkListenerSQLExecutionEnd, SparkListenerSQLExecutionStart}
import scala.collection.mutable

/** Test-only listener; no query/state mutations and no production dependency. */
final class FrequencyProbeTrace(spark: SparkSession) extends SparkListener {
  private val mapper = new ObjectMapper()
  private val jobs = mutable.LinkedHashMap.empty[Int, ObjectNode]
  private val stages = mutable.LinkedHashMap.empty[Int, ObjectNode]
  private val sql = mutable.LinkedHashMap.empty[Long, ObjectNode]
  private val sqlGroups = mutable.Map.empty[Long, String]
  spark.sparkContext.addSparkListener(this)

  private def stage(id: Int): ObjectNode = stages.getOrElseUpdate(id, {
    val node = mapper.createObjectNode()
    node.put("stage_id", id)
    node.putArray("tasks")
    node
  })
  override def onJobStart(event: SparkListenerJobStart): Unit = synchronized {
    val group = Option(event.properties).flatMap(p => Option(p.getProperty("spark.jobGroup.id")))
    group.filter(_.startsWith("frequency-")).foreach { g =>
      val node = mapper.createObjectNode()
      node.put("group", g); node.put("job_id", event.jobId); node.put("start_ms", event.time)
      val ids = node.putArray("stage_ids")
      event.stageIds.foreach(ids.add)
      Option(event.properties.getProperty("spark.sql.execution.id")).foreach { id =>
        node.put("sql_execution_id", id.toLong)
        sqlGroups.update(id.toLong, g)
      }
      jobs.update(event.jobId, node)
    }
  }
  override def onJobEnd(event: SparkListenerJobEnd): Unit = synchronized {
    jobs.get(event.jobId).foreach(_.put("end_ms", event.time))
  }
  override def onStageCompleted(event: SparkListenerStageCompleted): Unit = synchronized {
    val info = event.stageInfo
    val node = stage(info.stageId)
    node.put("name", info.name); node.put("num_tasks", info.numTasks)
    info.submissionTime.foreach(node.put("start_ms", _))
    info.completionTime.foreach(node.put("end_ms", _))
    val parents = node.putArray("parent_ids"); info.parentIds.foreach(parents.add)
    val rdds = node.putArray("rdd_names"); info.rddInfos.foreach(r => rdds.add(r.name))
  }
  override def onTaskEnd(event: SparkListenerTaskEnd): Unit = synchronized {
    if (event.taskMetrics != null) {
      val node = stage(event.stageId).withArray("tasks").addObject()
      val m = event.taskMetrics
      node.put("stage_attempt", event.stageAttemptId); node.put("partition_id", event.taskInfo.partitionId)
      node.put("task_type", event.taskType); node.put("failed", event.taskInfo.failed)
      node.put("launch_ms", event.taskInfo.launchTime); node.put("finish_ms", event.taskInfo.finishTime)
      node.put("executor_run_ms", m.executorRunTime); node.put("executor_cpu_ns", m.executorCpuTime)
      node.put("deserialize_ms", m.executorDeserializeTime); node.put("gc_ms", m.jvmGCTime)
      node.put("input_bytes", m.inputMetrics.bytesRead); node.put("input_records", m.inputMetrics.recordsRead)
      node.put("shuffle_read_bytes", m.shuffleReadMetrics.totalBytesRead)
      node.put("shuffle_write_bytes", m.shuffleWriteMetrics.bytesWritten)
      node.put("memory_spill_bytes", m.memoryBytesSpilled); node.put("disk_spill_bytes", m.diskBytesSpilled)
    }
  }
  override def onOtherEvent(event: SparkListenerEvent): Unit = synchronized {
    event match {
      case start: SparkListenerSQLExecutionStart =>
        val node = mapper.createObjectNode()
        node.put("execution_id", start.executionId); node.put("start_ms", start.time)
        node.put("initial_plan", start.physicalPlanDescription)
        sql.update(start.executionId, node)
      case end: SparkListenerSQLExecutionEnd =>
        sql.get(end.executionId).foreach { node =>
          node.put("end_ms", end.time); node.put("sql_execution_ns", end.duration)
          node.put("execution_name", end.executionName.getOrElse("unavailable"))
          if (sqlGroups.contains(end.executionId) && end.qe != null) {
            val plan = end.qe.executedPlan
            node.put("final_plan", plan.toString)
            val metrics = node.putArray("operators")
            val seen = new IdentityHashMap[SparkPlan, java.lang.Boolean]()
            def visit(p: SparkPlan, path: String): Unit = {
              if (!seen.containsKey(p)) {
                seen.put(p, java.lang.Boolean.TRUE)
                val op = metrics.addObject()
                op.put("path", path); op.put("node", p.nodeName)
                val values = op.putArray("metrics")
                p.metrics.toVector.sortBy(_._1).foreach { case (key, m) =>
                  val value = values.addObject()
                  value.put("key", key); value.put("name", m.name.getOrElse(key))
                  value.put("type", m.metricType); value.put("value", m.value)
                }
                val children = p match {
                  case a: AdaptiveSparkPlanExec => Seq(a.executedPlan)
                  case q: QueryStageExec => Seq(q.plan)
                  case r: ReusedExchangeExec => Seq(r.child)
                  case r: ReusedSubqueryExec => Seq(r.child)
                  case _ => p.children ++ p.subqueries
                }
                children.zipWithIndex.foreach { case (child, i) => visit(child, path + "." + i) }
              }
            }
            visit(plan, "0")
          }
          if (!sqlGroups.contains(end.executionId)) sql.remove(end.executionId)
        }
      case _ => ()
    }
  }
  def snapshot(group: String, expectedJobs: Int): ObjectNode = {
    // Listener draining is outside the timed prepare call. No timing sleeps or
    // best-effort snapshots: missing events fail the diagnostic instead.
    spark.sparkContext.listenerBus.waitUntilEmpty(30000L)
    synchronized {
      val selected = jobs.values.filter(_.get("group").asText() == group).toVector
      require(selected.size == expectedJobs, "diagnostic missed job events")
      require(selected.forall(_.has("end_ms")), "diagnostic saw unfinished jobs")
      val out = mapper.createObjectNode()
      val outputJobs = out.putArray("jobs"); selected.foreach(outputJobs.add)
      val stageIds = selected.flatMap { job =>
        import scala.jdk.CollectionConverters._
        job.withArray("stage_ids").elements().asScala.map(_.asInt()).toVector
      }.distinct
      val outputStages = out.putArray("stages"); stageIds.flatMap(stages.get).foreach(outputStages.add)
      val executions = selected.filter(_.has("sql_execution_id")).map(_.get("sql_execution_id").asLong()).distinct
      require(executions.nonEmpty, "diagnostic missed SQL job bindings")
      val outputSql = out.putArray("sql")
      executions.foreach { id =>
        val node = sql(id)
        require(node.has("end_ms"), "diagnostic missed SQL completion")
        outputSql.add(node)
      }
      require(executions.exists(id => sql(id).has("operators")), "diagnostic missed executed plans")
      out
    }
  }
  def close(): Unit = spark.sparkContext.removeSparkListener(this)
}
