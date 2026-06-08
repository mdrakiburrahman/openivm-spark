package org.openivm.spark.common

import java.sql.Timestamp

sealed trait ChangeWatermark {

  def encode: String
}

object ChangeWatermark {

  final case class TxnTs(timestamp: Timestamp) extends ChangeWatermark {
    override def encode: String = timestamp.toString
  }

  final case class DeltaVersion(version: Long) extends ChangeWatermark {
    override def encode: String = s"v:$version"
  }

  def decodeTxnTs(raw: String): Option[TxnTs] =
    scala.util.Try(Timestamp.valueOf(raw)).toOption.map(TxnTs.apply)

  def decodeDeltaVersion(raw: String): Option[DeltaVersion] =
    raw match {
      case s if s.startsWith("v:") => scala.util.Try(s.substring(2).toLong).toOption.map(DeltaVersion.apply)
      case _                       => None
    }
}
