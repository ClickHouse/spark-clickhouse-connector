package xenon.clickhouse.parse

import java.util
import org.antlr.v4.runtime.{ParserRuleContext, Token}
import org.antlr.v4.runtime.misc.Interval

import scala.collection.JavaConverters._

object ParseUtil {

  def seqToOption[T](seq: Seq[T]): Option[T] = seq.size match {
    case 0 => None
    case 1 => Some(seq.head)
    case illegal => throw new IllegalArgumentException(s"Expect list size 0 or 1, but got $illegal")
  }

  def listToOption[T](list: util.List[T]): Option[T] = seqToOption(list.asScala)

  def source(ctx: ParserRuleContext): String = {
    val stream = ctx.getStart.getInputStream
    stream.getText(Interval.of(ctx.getStart.getStartIndex, ctx.getStop.getStopIndex))
  }

  def position(token: Token): Origin = {
    val opt = Option(token)
    Origin(opt.map(_.getLine), opt.map(_.getCharPositionInLine))
  }
}

case class Origin(
  line: Option[Int] = None,
  startPosition: Option[Int] = None
)

object CurrentOrigin {
  private val value = new ThreadLocal[Origin]() {
    override def initialValue: Origin = Origin()
  }

  def get: Origin = value.get()
  def set(o: Origin): Unit = value.set(o)

  def reset(): Unit = value.set(Origin())

  def setPosition(line: Int, start: Int): Unit =
    value.set(
      value.get.copy(line = Some(line), startPosition = Some(start))
    )

  def withOrigin[A](o: Origin)(f: => A): A = {
    // remember the previous one so it can be reset to this
    // this way withOrigin can be recursive
    val previous = get
    set(o)
    val ret =
      try f
      finally set(previous)
    ret
  }
}
