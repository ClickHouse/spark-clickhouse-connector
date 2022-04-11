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

import java.util
import org.antlr.v4.runtime.{ParserRuleContext, Token}
import org.antlr.v4.runtime.misc.Interval

import scala.collection.JavaConverters._

object ParseUtils {

  private val _parser: ThreadLocal[SQLParser] = ThreadLocal.withInitial(() => new SQLParser(new AstVisitor))

  def parser: SQLParser = _parser.get

  def seqToOption[T](seq: Seq[T]): Option[T] = seq.size match {
    case 0 => None
    case 1 => Some(seq.head)
    case illegal => throw new IllegalArgumentException(s"Expect list size 0 or 1, but got $illegal")
  }

  def listToOption[T](list: util.List[T]): Option[T] = seqToOption(list.asScala.toSeq)

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
