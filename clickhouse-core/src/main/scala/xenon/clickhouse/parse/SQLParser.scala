package xenon.clickhouse.parse

import org.antlr.v4.runtime._
import org.antlr.v4.runtime.atn.PredictionMode
import org.antlr.v4.runtime.misc.ParseCancellationException
import xenon.clickhouse._
import xenon.clickhouse.spec.TableEngineSpec

class SQLParser(astVisitor: AstVisitor) extends Logging {

  def parseEngineClause(sql: String): TableEngineSpec =
    parse(sql)(parser => astVisitor.visitEngineClause(parser.engineClause))

  private def parse[T](sql: String)(toResult: ClickHouseAstParser => T): T = {

    log.debug(s"Parsing SQL: $sql")

    val lexer = new ClickHouseAstLexer(CharStreams.fromString(sql))
    lexer.removeErrorListeners()
    lexer.addErrorListener(ParseErrorListener)

    val tokenStream = new CommonTokenStream(lexer)
    val parser = new ClickHouseAstParser(tokenStream)
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

  def this(message: String, ctx: ParserRuleContext) = {
    this(
      message,
      ParseUtil.position(ctx.getStart),
      ParseUtil.position(ctx.getStop)
    )
  }
}
