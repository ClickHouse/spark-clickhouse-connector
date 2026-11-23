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

package com.clickhouse.spark.read.split.primaryindex

/** Reading and writing bits of ClickHouse SQL text. */
private[primaryindex] object SqlText {

  def quote(value: String): String = s"'${value.replace("\\", "\\\\").replace("'", "\\'")}'"

  def unquote(identifier: String): String =
    if (identifier.length > 1 && identifier.startsWith("`") && identifier.endsWith("`"))
      identifier.substring(1, identifier.length - 1)
    else identifier

  // the arguments of a call, split at the commas outside parentheses and quotes; None when unbalanced
  def splitArgs(argList: String): Option[Seq[String]] = {
    val args = Seq.newBuilder[String]
    val current = new StringBuilder
    var depth = 0
    var quote: Option[Char] = None
    var i = 0
    while (i < argList.length && depth >= 0) {
      val c = argList(i)
      quote match {
        case Some(q) =>
          current += c
          if (c == '\\' && i + 1 < argList.length) {
            current += argList(i + 1)
            i += 1
          } else if (c == q) quote = None
        case None => c match {
            case '\'' | '"' | '`' =>
              quote = Some(c)
              current += c
            case '(' =>
              depth += 1
              current += c
            case ')' =>
              depth -= 1
              current += c
            case ',' if depth == 0 =>
              args += current.toString.trim
              current.clear()
            case _ => current += c
          }
      }
      i += 1
    }
    args += current.toString.trim
    if (depth == 0 && quote.isEmpty) Some(args.result()) else None
  }
}
