package org.openivm.spark.insights

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.JsonNodeFactory

import scala.collection.Map

/** Jackson-backed details JSON builder used by insight emitters. */
object OpenIvmInsightJson {
  private val Mapper = new ObjectMapper()

  def obj(fields: (String, Any)*): String = {
    val root = JsonNodeFactory.instance.objectNode()
    fields.foreach { case (name, value) =>
      root.set[JsonNode](name, node(value))
    }
    Mapper.writeValueAsString(root)
  }

  private def node(value: Any): JsonNode =
    value match {
      case null              => JsonNodeFactory.instance.nullNode()
      case json: JsonNode    => json
      case value: String     => JsonNodeFactory.instance.textNode(value)
      case value: Boolean    => JsonNodeFactory.instance.booleanNode(value)
      case value: Byte       => JsonNodeFactory.instance.numberNode(value.toInt)
      case value: Short      => JsonNodeFactory.instance.numberNode(value.toInt)
      case value: Int        => JsonNodeFactory.instance.numberNode(value)
      case value: Long       => JsonNodeFactory.instance.numberNode(value)
      case value: Float      => JsonNodeFactory.instance.numberNode(value)
      case value: Double     => JsonNodeFactory.instance.numberNode(value)
      case value: BigDecimal => JsonNodeFactory.instance.numberNode(value.bigDecimal)
      case Some(inner)       => node(inner)
      case None              => JsonNodeFactory.instance.nullNode()
      case values: Map[_, _] =>
        val result = JsonNodeFactory.instance.objectNode()
        values.toSeq
          .map {
            case (key: String, item) => key -> item
            case (key, _) =>
              throw new IllegalArgumentException(
                s"Insight details object keys must be strings, found ${Option(key).fold("null")(_.getClass.getName)}"
              )
          }
          .sortBy(_._1)
          .foreach { case (key, item) =>
            result.set[JsonNode](key, node(item))
          }
        result
      case values: Array[_]    => array(values.toIndexedSeq)
      case values: Iterable[_] => array(values)
      case other =>
        throw new IllegalArgumentException(
          s"Unsupported insight details value type: ${other.getClass.getName}"
        )
    }

  private def array(values: Iterable[_]): JsonNode = {
    val result = JsonNodeFactory.instance.arrayNode()
    values.foreach(value => result.add(node(value)))
    result
  }
}
