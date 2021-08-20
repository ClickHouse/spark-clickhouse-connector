package xenon.clickhouse

import org.scalatest.funsuite.AnyFunSuite
import Utils._

class UtilsSuite extends AnyFunSuite {

  test("stripSingleQuote") {
    assert(stripSingleQuote("'") === "")
    assert(stripSingleQuote("''") === "")
    assert(stripSingleQuote("'abc'") === "abc")
    assert(stripSingleQuote("abc'") === "abc")
    assert(stripSingleQuote("'abc\'") === "abc")
    assert(stripSingleQuote("\'abc") === "abc")
    assert(stripSingleQuote("'abc\\'") === "abc\\'")
    assert(stripSingleQuote("\\'abc") === "\\'abc")
  }

  test("wrapBackQuote") {
    assert(wrapBackQuote("`") === "``")
    assert(wrapBackQuote("``") === "``")
    assert(wrapBackQuote("abc") === "`abc`")
    assert(wrapBackQuote("`abc") === "`abc`")
    assert(wrapBackQuote("abc`") === "`abc`")
  }
}
