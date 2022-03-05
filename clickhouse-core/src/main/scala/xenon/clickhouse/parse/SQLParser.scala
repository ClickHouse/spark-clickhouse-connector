/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package xenon.clickhouse.parse

import org.antlr.v4.runtime._
import org.antlr.v4.runtime.atn.PredictionMode
import org.antlr.v4.runtime.misc.ParseCancellationException
import xenon.clickhouse._
import xenon.clickhouse.expr.Expr
import xenon.clickhouse.spec.TableEngineSpec

class SQLParser(astVisitor: AstVisitor) extends Logging {

  def parseColumnExpr(sql: String): Expr =
    parse(sql)(parser => astVisitor.visitColumnExpr(parser.columnExpr))

  def parseEngineClause(sql: String): TableEngineSpec =
    parse(sql)(parser => astVisitor.visitEngineClause(parser.engineClause))

  private def parse[T](sql: String)(toResult: ClickHouseSQLParser => T): T = {

    log.debug(s"Parsing SQL: $sql")

    val lexer = new ClickHouseSQLLexer(CharStreams.fromString(sql))
    lexer.removeErrorListeners()
    lexer.addErrorListener(ParseErrorListener)

    val tokenStream = new CommonTokenStream(lexer)
    val parser = new ClickHouseSQLParser(tokenStream)
    parser.removeErrorListeners()
    parser.addErrorListener(ParseErrorListener)

    try try {
      // first, try parsing with potentially faster SLL mode
      parser.getInterpreter.setPredictionMode(PredictionMode.SLL)
      toResult(parser)
    } catch {
      case _: ParseCancellationException =>
        // if we fail, parse with LL mode
        tokenStream.seek(0) // rewind input stream
        parser.reset()

        // Try Again.
        parser.getInterpreter.setPredictionMode(PredictionMode.LL)
        toResult(parser)
    } catch {
      case rethrow: ParseException => throw rethrow
    }
  }
}

object ParseErrorListener extends BaseErrorListener {
  override def syntaxError(
    recognizer: Recognizer[_, _],
    offendingSymbol: scala.Any,
    line: Int,
    charPositionInLine: Int,
    msg: String,
    e: RecognitionException
  ): Unit = {
    val (start, stop) = offendingSymbol match {
      case token: CommonToken =>
        val start = Origin(Some(line), Some(token.getCharPositionInLine))
        val length = token.getStopIndex - token.getStartIndex + 1
        val stop = Origin(Some(line), Some(token.getCharPositionInLine + length))
        (start, stop)
      case _ =>
        val start = Origin(Some(line), Some(charPositionInLine))
        (start, start)
    }
    throw ParseException(msg, start, stop)
  }
}

case class ParseException(
  message: String,
  start: Origin,
  stop: Origin
) extends RuntimeException(message) {

  def this(message: String, ctx: ParserRuleContext) =
    this(
      message,
      ParseUtils.position(ctx.getStart),
      ParseUtils.position(ctx.getStop)
    )
}
