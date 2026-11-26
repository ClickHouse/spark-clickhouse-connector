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

package com.clickhouse.spark.client

import com.clickhouse.spark.exception.{CHClientException, RetryableCHException}
import com.clickhouse.spark.spec.NodeSpec
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import java.net.ConnectException

class NodeHealthTrackerSuite extends AnyFunSuite with BeforeAndAfterEach {

  private val a = NodeSpec("host-a", _http_port = Some(8123), _tcp_port = Some(9000))
  private val b = NodeSpec("host-b", _http_port = Some(8123), _tcp_port = Some(9000))
  private val aAgain = NodeSpec("host-a", _http_port = Some(8123), _tcp_port = Some(9000))

  override def afterEach(): Unit = NodeHealthTracker.clear()

  test("markDead excludes the node from live()") {
    assert(NodeHealthTracker.live(Seq(a, b)) === Seq(a, b))
    NodeHealthTracker.markDead(a)
    assert(NodeHealthTracker.isDead(a))
    assert(NodeHealthTracker.isDead(aAgain))
    assert(NodeHealthTracker.live(Seq(a, b)) === Seq(b))
  }

  test("clear restores all nodes") {
    NodeHealthTracker.markDead(a)
    NodeHealthTracker.markDead(b)
    NodeHealthTracker.clear()
    assert(NodeHealthTracker.live(Seq(a, b)) === Seq(a, b))
  }

  test("isFailoverWorthy recognises client and connection failures") {
    assert(NodeFailover.isFailoverWorthy(CHClientException("boom")))
    assert(NodeFailover.isFailoverWorthy(RetryableCHException(241, "oom", Some(a))))
    assert(NodeFailover.isFailoverWorthy(new ConnectException("refused")))
    assert(NodeFailover.isFailoverWorthy(new RuntimeException(new ConnectException("refused"))))
    assert(!NodeFailover.isFailoverWorthy(new IllegalArgumentException("bad arg")))
  }
}
