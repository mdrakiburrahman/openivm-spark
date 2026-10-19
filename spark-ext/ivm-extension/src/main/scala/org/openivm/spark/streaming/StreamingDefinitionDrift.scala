package org.openivm.spark.streaming

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

/** Recognizes definition drift that cannot change what a streaming table writes, so reconciliation
  * resumes the owned checkpoint instead of rebuilding the target and replaying every source snapshot.
  *
  * A persisted definition fingerprints each source's full schema, so a column added upstream changes
  * the fingerprint even when the declaration never reads it. Telemetry sources evolve this way, and a
  * rebuild replays sources whose oldest files may already have expired from storage.
  */
private[streaming] object StreamingDefinitionDrift {

  /** Opt-in: an unchanged, view-free declaration keeps its checkpoint when only the analyzer's plan
    * encoding differs, for example after a Spark runtime patch. Off by default because session
    * settings such as ANSI mode or the time zone surface only in the analyzed plan.
    */
  val IgnoreAnalyzedPlanDriftKey: String = "spark.openivm.streaming.definition.ignoreAnalyzedPlanDrift"

  private val Mapper         = new ObjectMapper()
  private val OutputMarker   = "output=["
  private val PrunedOutput   = "output=<relation-output>"
  private val SchemaField    = "schema"
  private val OptionsField   = "options"
  private val AnalyzedField  = "analyzedPlan"
  private val SourcesField   = "sources"
  private val QuotedContexts = Set('"', '`')

  /** Whether analyzer encoding drift of an unchanged declaration resumes the owned checkpoint.
    *
    * @param spark session whose configuration applies
    */
  def ignoreAnalyzedPlanDrift(spark: SparkSession): Boolean =
    spark.conf.getOption(IgnoreAnalyzedPlanDriftKey).exists(_.trim.equalsIgnoreCase("true"))

  /** Whether a stored definition and a requested one describe the same stream although their
    * fingerprints differ.
    *
    * Every semantic field except the analyzed plan and source schemas must be equal. Each stored
    * source column must still exist unchanged, which admits only added columns. The analyzed plans
    * must match once relation output lists are ignored, so an added column cannot reach the query;
    * `ignoreAnalyzedPlan` skips that comparison. View-expanded declarations are never resumable here,
    * because a redefined view leaves the declaration unchanged.
    *
    * @param storedSemanticJson semantic JSON persisted in the owned target's manifest
    * @param requested          freshly analyzed definition for the submitted declaration
    * @param expandsViews       whether the submitted declaration expanded a view
    * @param ignoreAnalyzedPlan whether analyzer encoding drift of an unchanged declaration is accepted
    */
  def resumable(
      storedSemanticJson: String,
      requested: StreamingTableDefinition,
      expandsViews: Boolean,
      ignoreAnalyzedPlan: Boolean
  ): Boolean =
    !expandsViews && {
      val stored = Mapper.readTree(StreamingTableDefinition.normalizeSemanticJson(storedSemanticJson))
      val next   = Mapper.readTree(StreamingTableDefinition.normalizeSemanticJson(requested.semanticJson))
      fieldNames(stored, next).forall {
        case SourcesField => sourcesResumable(stored.get(SourcesField), next.get(SourcesField), requested)
        case AnalyzedField =>
          ignoreAnalyzedPlan || {
            val before = textField(stored, AnalyzedField).flatMap(withoutRelationOutputs)
            before.nonEmpty && before == textField(next, AnalyzedField).flatMap(withoutRelationOutputs)
          }
        case field => stored.get(field) == next.get(field)
      }
    }

  /** Split a bracketed signature list into its top-level elements, honouring JSON strings, backtick
    * identifiers and nested `[]`, `()` and `<>`.
    *
    * @param list signature list such as a persisted source schema
    * @return the elements, or None when the text is not a well-formed list
    */
  def topLevelElements(list: String): Option[Seq[String]] =
    if (!list.startsWith("[") || !list.endsWith("]")) None
    else {
      val body     = list.substring(1, list.length - 1)
      val elements = ArrayBuffer.empty[String]
      val current  = new StringBuilder
      if (
        !scan(body) { (char, depth, quoted) =>
          if (char == ',' && depth == 0 && !quoted) {
            elements += current.toString
            current.clear()
          } else current.append(char)
          true
        }
      ) None
      else {
        if (current.nonEmpty || elements.nonEmpty) elements += current.toString
        Some(elements.toSeq)
      }
    }

  /** Replace every relation `output=[...]` list of an encoded plan with a fixed token.
    *
    * @param plan structural plan text persisted in a definition
    * @return the plan without relation output lists, or None when the text is malformed
    */
  def withoutRelationOutputs(plan: String): Option[String] = {
    val out   = new StringBuilder
    var index = 0
    var valid = true
    while (valid && index < plan.length) {
      nextUnquoted(plan, index) match {
        case None =>
          out.append(plan.substring(index))
          index = plan.length
        case Some(marker) =>
          out.append(plan.substring(index, marker)).append(PrunedOutput)
          closingBracket(plan, marker + OutputMarker.length - 1) match {
            case Some(end) => index = end + 1
            case None      => valid = false
          }
      }
    }
    if (valid) Some(out.toString) else None
  }

  private def sourcesResumable(
      stored: JsonNode,
      requested: JsonNode,
      definition: StreamingTableDefinition
  ): Boolean =
    stored != null && requested != null && stored.isArray && requested.isArray &&
      stored.size() == requested.size() &&
      stored.elements().asScala.zip(requested.elements().asScala).zipWithIndex.forall { case ((before, after), index) =>
        val operational = definition.sources.lift(index).map(_.operationalOptions.keySet).getOrElse(Set.empty)
        fieldNames(before, after).forall {
          case SchemaField =>
            (for {
              previous <- textField(before, SchemaField).flatMap(topLevelElements)
              current  <- textField(after, SchemaField).flatMap(topLevelElements)
            } yield previous.toSet.subsetOf(current.toSet)).getOrElse(false)
          case OptionsField =>
            semanticOptions(before.get(OptionsField), operational) ==
              semanticOptions(after.get(OptionsField), operational)
          case field => before.get(field) == after.get(field)
        }
      }

  private def semanticOptions(node: JsonNode, operational: Set[String]): Map[String, JsonNode] =
    Option(node)
      .filter(_.isObject)
      .map(options => options.fieldNames().asScala.map(key => key -> options.get(key)).toMap)
      .getOrElse(Map.empty[String, JsonNode])
      .filterNot { case (key, _) => operational.contains(key) }

  private def fieldNames(left: JsonNode, right: JsonNode): Set[String] =
    (left.fieldNames().asScala ++ right.fieldNames().asScala).toSet

  private def textField(node: JsonNode, field: String): Option[String] =
    Option(node.get(field)).filter(_.isTextual).map(_.asText())

  private def nextUnquoted(text: String, from: Int): Option[Int] = {
    var found  = Option.empty[Int]
    var offset = from
    scan(text.substring(from)) { (_, _, quoted) =>
      if (!quoted && text.startsWith(OutputMarker, offset)) found = Some(offset)
      offset += 1
      found.isEmpty
    }
    found
  }

  private def closingBracket(text: String, open: Int): Option[Int] = {
    var end    = Option.empty[Int]
    var offset = open
    scan(text.substring(open)) { (char, depth, quoted) =>
      if (!quoted && char == ']' && depth == 0 && offset > open) end = Some(offset)
      offset += 1
      end.isEmpty
    }
    end
  }

  /** Visit characters until `visit` returns false, passing the bracket depth (after a closing and
    * before an opening bracket) and whether the character lies inside a JSON string or backtick
    * identifier. Returns whether a completed scan ended balanced.
    */
  private def scan(text: String)(visit: (Char, Int, Boolean) => Boolean): Boolean = {
    var depth    = 0
    var quote    = Option.empty[Char]
    var escaped  = false
    var balanced = true
    var index    = 0
    var going    = true
    while (going && index < text.length) {
      val char = text.charAt(index)
      quote match {
        case Some(open) =>
          if (escaped) escaped = false
          else if (open == '"' && char == '\\') escaped = true
          else if (char == open) quote = None
          going = visit(char, depth, true)
        case None if QuotedContexts.contains(char) =>
          quote = Some(char)
          going = visit(char, depth, true)
        case None if char == '[' || char == '(' || char == '<' =>
          going = visit(char, depth, false)
          depth += 1
        case None if char == ']' || char == ')' || char == '>' =>
          depth -= 1
          if (depth < 0) balanced = false
          going = visit(char, depth, false)
        case None =>
          going = visit(char, depth, false)
      }
      index += 1
    }
    !going || (balanced && depth == 0 && quote.isEmpty)
  }
}
