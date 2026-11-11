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

package com.clickhouse.spark

import com.clickhouse.spark.exception.{CHClientException, CHServerException}
import com.clickhouse.spark.spec.NodeSpec
import org.scalatest.funsuite.AnyFunSuite

/**
 * The cache lives as long as the JVM, so each case uses a host of its own rather than relying on
 * the order the suites run in.
 */
class ListingPlanCacheSuite extends AnyFunSuite {

  private def node(host: String) = NodeSpec(host, Some(8123))

  test("a settled union is resolved once") {
    var calls = 0
    def resolve: ListingDecision = { calls += 1; UnionAcross("cl", "st") }
    assert(ClickHouseHelper.listingPlanFor(node("settled"))(resolve) === UnionAcross("cl", "st"))
    assert(ClickHouseHelper.listingPlanFor(node("settled"))(resolve) === UnionAcross("cl", "st"))
    assert(calls === 1)
  }

  test("a settled refusal is resolved once") {
    var calls = 0
    def resolve: ListingDecision = { calls += 1; NoUnion }
    assert(ClickHouseHelper.listingPlanFor(node("refused"))(resolve) === NoUnion)
    assert(ClickHouseHelper.listingPlanFor(node("refused"))(resolve) === NoUnion)
    assert(calls === 1)
  }

  test("an undecided attempt is resolved again") {
    // no answer arrived, so nothing is remembered: one dropped connection must not cost the union
    // for the life of the JVM
    var calls = 0
    def resolve: ListingDecision = { calls += 1; Undecided }
    assert(ClickHouseHelper.listingPlanFor(node("undecided"))(resolve) === Undecided)
    assert(ClickHouseHelper.listingPlanFor(node("undecided"))(resolve) === Undecided)
    assert(calls === 2)
  }

  test("an undecided attempt does not shadow a later settled one") {
    var calls = 0
    def resolve: ListingDecision = {
      calls += 1
      if (calls == 1) Undecided else UnionAcross("cl", "st")
    }
    assert(ClickHouseHelper.listingPlanFor(node("recovers"))(resolve) === Undecided)
    assert(ClickHouseHelper.listingPlanFor(node("recovers"))(resolve) === UnionAcross("cl", "st"))
    assert(ClickHouseHelper.listingPlanFor(node("recovers"))(resolve) === UnionAcross("cl", "st"))
    assert(calls === 2)
  }

  test("a grant or setting the server will not give settles the question") {
    assert(ClickHouseHelper.isSettledRefusal(CHServerException(497, "needs REMOTE", None, None)))
    assert(ClickHouseHelper.isSettledRefusal(CHServerException(164, "readonly", None, None)))
    assert(ClickHouseHelper.isSettledRefusal(CHServerException(701, "no such cluster", None, None)))
  }

  test("a failure that may not recur leaves the question open") {
    // these arrive as server exceptions too, so the exception type cannot be the discriminator:
    // a timeout and a replica that was briefly down may both succeed on the next read, and a
    // proxy answering an HTTP error carries no ClickHouse code at all
    assert(!ClickHouseHelper.isSettledRefusal(CHServerException(159, "timeout", None, None)))
    assert(!ClickHouseHelper.isSettledRefusal(CHServerException(279, "no connection", None, None)))
    assert(!ClickHouseHelper.isSettledRefusal(CHServerException(0, "transport error: 503", None, None)))
    assert(!ClickHouseHelper.isSettledRefusal(CHClientException("connection reset")))
    assert(!ClickHouseHelper.isSettledRefusal(new RuntimeException("boom")))
  }
}
