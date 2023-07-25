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

package xenon.clickhouse.func.clickhouse.cityhash;

/**
 * @author Dmitriy Poluyanov
 * @since 15/02/2018
 * copy from https://github.com/dpoluyanov/achord/blob/master/src/main/java/com/github/mangelion/achord/UInt128.java
 */
final public class UInt128 {
    final public long first;
    final public long second;

    public UInt128(long first, long second) {
        this.first = first;
        this.second = second;
    }

    static UInt128 of(long first, long second) {
        return new UInt128(first, second);
    }
}
