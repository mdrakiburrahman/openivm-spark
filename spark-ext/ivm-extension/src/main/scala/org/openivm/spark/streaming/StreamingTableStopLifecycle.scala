package org.openivm.spark.streaming

private[streaming] object StreamingTableStopLifecycle {

  def afterStops[A](stops: Seq[() => Unit])(mutation: => A): A = {
    stops.foreach(stop => stop())
    mutation
  }
}
