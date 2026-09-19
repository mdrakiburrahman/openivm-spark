package org.openivm.spark.common

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.execution.command.LeafRunnableCommand

import java.lang.reflect.{InvocationHandler, InvocationTargetException, Method, Proxy}
import java.util.Properties
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch, TimeUnit}

/** Synthetic driver-side lifecycle; no compiler or application SQL is involved. */
object ScopedSqlBridgeFixture {
  @volatile var source: String              = _
  @volatile var entered: CountDownLatch     = new CountDownLatch(0)
  @volatile var proceed: CountDownLatch     = new CountDownLatch(0)
  @volatile var taskEntered: CountDownLatch = new CountDownLatch(0)
  @volatile var taskProceed: CountDownLatch = new CountDownLatch(0)
  val threads                               = new ConcurrentHashMap[String, Long]()
  val observed                              = new ConcurrentHashMap[String, String]()

  def awaitCancellation(): Int = {
    taskEntered.countDown()
    require(taskProceed.await(45, TimeUnit.SECONDS), "cancellation fixture timed out")
    1
  }

  def parser(delegate: ParserInterface): ParserInterface =
    Proxy
      .newProxyInstance(
        classOf[ParserInterface].getClassLoader,
        Array(classOf[ParserInterface]),
        new InvocationHandler {
          override def invoke(proxy: Any, method: Method, args: Array[AnyRef]): AnyRef =
            if (method.getName == "parsePlan" && args(0).toString.startsWith("SCOPED FIXTURE ")) {
              val parts = args(0).toString.split(" ")
              ScopedSqlProbeCommand(parts(2), parts.lift(3).contains("fail"))
            } else
              try method.invoke(delegate, args: _*)
              catch { case error: InvocationTargetException => throw error.getCause }
        }
      )
      .asInstanceOf[ParserInterface]

  def properties(spark: SparkSession): String =
    Seq(
      QueryLogExport.RequestIdProperty,
      "spark.jobGroup.id",
      "spark.job.description",
      "spark.scheduler.pool",
      "spark.job.interruptOnCancel"
    ).map(key => Option(spark.sparkContext.getLocalProperty(key)).getOrElse("<null>")).mkString("|")
}

case class ScopedSqlProbeCommand(request: String, fail: Boolean) extends LeafRunnableCommand {
  override def run(spark: SparkSession): Seq[Row] = {
    import ScopedSqlBridgeFixture._
    threads.put(request, Thread.currentThread().getId)
    observed.put(request, properties(spark))
    val invocation =
      QueryLogExport.startInvocation(spark, s"native-$request", s"default.scoped_$request", "refresh").get
    try {
      entered.countDown()
      require(proceed.await(45, TimeUnit.SECONDS), "fixture overlap timed out")
      spark.read.format("delta").load(source).collect()
      if (fail) throw new IllegalStateException("synthetic SQL failure")
      invocation.finish("no_pending_deltas")
      Seq.empty
    } catch {
      case error: Throwable =>
        invocation.finish("synthetic_failure")
        throw error
    }
  }
}

/** Entry point for a real pooled JavaGateway. Every method is one independent RPC. */
class ScopedSqlGatewayFixture(val spark: SparkSession) {
  private val blocked = new CountDownLatch(1)
  private val unblock = new CountDownLatch(1)

  def legacySet(request: String): Long = {
    spark.sparkContext.setLocalProperty(QueryLogExport.RequestIdProperty, request)
    Thread.currentThread().getId
  }

  def legacyRead(): String =
    s"${Thread.currentThread().getId}:${spark.sparkContext.getLocalProperty(QueryLogExport.RequestIdProperty)}"

  def holdConnection(): Unit = {
    blocked.countDown()
    require(unblock.await(45, TimeUnit.SECONDS), "legacy fixture timed out")
  }

  def awaitBlocked(): Unit      = require(blocked.await(45, TimeUnit.SECONDS), "legacy fixture did not enter")
  def releaseConnection(): Unit = unblock.countDown()

  def prepare(count: Int): Unit = {
    ScopedSqlBridgeFixture.entered = new CountDownLatch(count)
    ScopedSqlBridgeFixture.proceed = new CountDownLatch(1)
  }

  def awaitOverlap(): Unit =
    require(ScopedSqlBridgeFixture.entered.await(45, TimeUnit.SECONDS), "bridge serialized or did not enter")

  def releaseOverlap(): Unit = ScopedSqlBridgeFixture.proceed.countDown()

  def executeAndVerifyRestore(request: String, fail: Boolean): Long = {
    val context = spark.sparkContext
    val prior   = new Properties()
    prior.setProperty("fixture.unrelated", request)
    prior.setProperty(QueryLogExport.RequestIdProperty, "prior-" + request)
    prior.setProperty("spark.jobGroup.id", "prior-group")
    ScopedSqlTestProperties.set(context, prior)
    try {
      ScopedSqlBridge.execute(
        spark,
        s"SCOPED FIXTURE $request ${if (fail) "fail" else "ok"}",
        request,
        "group-" + request,
        "description-" + request,
        "pool-" + request,
        true
      )
    } finally {
      require(ScopedSqlTestProperties.get(context) == prior, "prior values were not restored")
      require(ScopedSqlTestProperties.get(context) ne prior, "restoration must use a cloned snapshot")
      require(prior.getProperty(QueryLogExport.RequestIdProperty) == "prior-" + request, "prior object mutated")
      require(prior.getProperty("spark.scheduler.pool") == null, "prior object acquired scoped pool")
      ScopedSqlTestProperties.set(context, new Properties())
    }
    Thread.currentThread().getId
  }

  def snapshot(request: String): String = QueryLogExport.snapshotJson(spark, request, 100, 65536)
}
