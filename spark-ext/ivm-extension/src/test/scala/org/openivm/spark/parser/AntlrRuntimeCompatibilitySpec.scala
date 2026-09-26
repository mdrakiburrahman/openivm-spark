package org.openivm.spark.parser

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class AntlrRuntimeCompatibilitySpec extends AnyFunSpec with Matchers {

  it("initializes the generated lexer with the selected ANTLR runtime") {
    noException should be thrownBy {
      Class.forName("org.openivm.spark.parser.gen.IvmSqlBaseLexer")
    }
  }
}
