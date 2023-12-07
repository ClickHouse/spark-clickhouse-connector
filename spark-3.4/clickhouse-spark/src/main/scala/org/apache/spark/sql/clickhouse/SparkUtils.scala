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

package org.apache.spark.sql.clickhouse

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.util.VersionUtils

object SparkUtils {

  lazy val MAJOR_MINOR_VERSION: (Int, Int) = VersionUtils.majorMinorVersion(SPARK_VERSION)

  def toArrowSchema(schema: StructType, timeZoneId: String): Schema = ArrowUtils.toArrowSchema(schema, timeZoneId)

  def spawnArrowAllocator(name: String): BufferAllocator =
    ArrowUtils.rootAllocator.newChildAllocator(name, 0, Long.MaxValue)
}
