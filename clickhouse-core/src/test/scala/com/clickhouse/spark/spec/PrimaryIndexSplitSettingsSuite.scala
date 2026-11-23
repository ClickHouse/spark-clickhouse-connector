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

package com.clickhouse.spark.spec

import org.scalatest.funsuite.AnyFunSuite

class PrimaryIndexSplitSettingsSuite extends AnyFunSuite {

  private def of(maxParallelism: Option[Int], tables: Int = 1): PrimaryIndexSplitSettings =
    PrimaryIndexSplitSettings(maxParallelism, targetRowsPerTask = 1000, defaultParallelism = 2, tables = tables)

  test("a table is sized by the target rows, three tasks per core at the least, the maximum set at the most") {
    assert(of(None) === PrimaryIndexSplitSettings(atLeast = 6, targetRowsPerTask = 1000, maxParallelism = None))
    assert(of(Some(8)) === PrimaryIndexSplitSettings(atLeast = 6, targetRowsPerTask = 1000, maxParallelism = Some(8)))
  }

  test("tables sharing a read get an equal share of the least and of the maximum, one task each at least") {
    assert(of(Some(8), tables = 2) === PrimaryIndexSplitSettings(3, 1000, Some(4)))
    assert(of(Some(8), tables = 3) === PrimaryIndexSplitSettings(2, 1000, Some(2)))
    assert(of(Some(2), tables = 4) === PrimaryIndexSplitSettings(1, 1000, Some(1)))
    assert(of(None, tables = 4) === PrimaryIndexSplitSettings(1, 1000, None))
  }
}
