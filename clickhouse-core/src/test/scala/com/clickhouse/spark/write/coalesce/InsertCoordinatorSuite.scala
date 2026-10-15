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

package com.clickhouse.spark.write.coalesce

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterEach

import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration._

class InsertCoordinatorSuite extends AnyFunSuite with BeforeAndAfterEach {

  override def afterEach(): Unit = InsertCoordinator.shutdownForTest()

  private def cfg: CoalesceConfig = cfgWith(targetBytes = 1)

  private def cfgWith(targetBytes: Long): CoalesceConfig =
    CoalesceConfig(
      enabled = true,
      targetBytes = targetBytes,
      maxBufferedBytes = 1 << 20,
      lingerMs = 0,
      senderThreads = 2,
      dedupMode = "none",
      maxRetry = 0,
      retryInterval = java.time.Duration.ZERO,
      retryableErrorCodes = Set.empty
    )

  private def ctx(db: String, t: String): BucketContext =
    BucketContext(null, db, t, Array.emptyByteArray, Map.empty, cfg)

  private def ctxWithHeader(header: String, c: CoalesceConfig = cfg): BucketContext =
    BucketContext(null, "db", "t", header.getBytes("UTF-8"), Map.empty, c)

  /** acquire + submit, the way a writer does for its first batch to a key. */
  private def acquireAndSubmit(h: BatchHandle, c: BucketContext): scala.concurrent.Future[Unit] = {
    InsertCoordinator.acquire(h.bucket, c)
    InsertCoordinator.submit(h)
  }

  private def handle(key: ShardKey, id: String): BatchHandle =
    BatchHandle(key, id, 1L, new BytesPayload(Array[Byte](1)))

  test("submit routes handles to the bucket for their ShardKey and acks them") {
    val sent = new ConcurrentLinkedQueue[String]()
    InsertCoordinator.setSenderForTest((_, g) => g.foreach { h => sent.add(h.dedupId); h.promise.success(()) })
    val k1 = ShardKey("q", "db", "t1", None)
    val f1 = acquireAndSubmit(handle(k1, "a"), ctx("db", "t1"))
    Await.result(f1, 5.seconds)
    assert(sent.asScala.toList == List("a"))
  }

  test("distinct ShardKeys use distinct buckets") {
    val byTable = new ConcurrentLinkedQueue[String]()
    InsertCoordinator.setSenderForTest((c, g) => g.foreach { h => byTable.add(c.table); h.promise.success(()) })
    val fa = acquireAndSubmit(handle(ShardKey("q", "db", "t1", None), "a"), ctx("db", "t1"))
    val fb = acquireAndSubmit(handle(ShardKey("q", "db", "t2", None), "b"), ctx("db", "t2"))
    Await.result(fa, 5.seconds); Await.result(fb, 5.seconds)
    assert(byTable.asScala.toSet == Set("t1", "t2"))
  }

  test("the sender never runs on the submitting thread") {
    @volatile var senderThread: Thread = null
    InsertCoordinator.setSenderForTest { (_, g) =>
      senderThread = Thread.currentThread(); g.foreach(_.promise.success(()))
    }
    val f = acquireAndSubmit(handle(ShardKey("q", "db", "t", None), "a"), ctx("db", "t"))
    Await.result(f, 5.seconds)
    assert(senderThread != null && (senderThread ne Thread.currentThread()))
  }

  test("one writer's commit flush does not stop the shared bucket coalescing for the others") {
    val groups = new ConcurrentLinkedQueue[List[String]]()
    InsertCoordinator.setSenderForTest { (_, g) =>
      groups.add(g.map(_.dedupId).toList); g.foreach(_.promise.success(()))
    }
    val k = ShardKey("q", "db", "t", None)
    val c = ctxWithHeader("h", cfgWith(targetBytes = 1000))
    InsertCoordinator.acquire(k, c) // writer A
    InsertCoordinator.acquire(k, c) // writer B
    val fa = InsertCoordinator.submit(handle(k, "a"))
    InsertCoordinator.flush(k) // writer A commits
    Await.result(fa, 5.seconds)
    InsertCoordinator.release(k)
    val fb1 = InsertCoordinator.submit(handle(k, "b1"))
    val fb2 = InsertCoordinator.submit(handle(k, "b2"))
    Thread.sleep(300)
    assert(!fb1.isCompleted && !fb2.isCompleted, "B's batches must keep coalescing after A's commit")
    InsertCoordinator.flush(k) // writer B commits
    Await.result(fb2, 5.seconds)
    InsertCoordinator.release(k)
    assert(groups.asScala.toList == List(List("a"), List("b1", "b2")))
  }

  test("a bucket is dropped when its last writer releases; a later write gets its own context") {
    val headers = new ConcurrentLinkedQueue[String]()
    InsertCoordinator.setSenderForTest { (c, g) =>
      headers.add(new String(c.schemaHeader, "UTF-8")); g.foreach(_.promise.success(()))
    }
    // Same table name across two write jobs (e.g. dropped and recreated with a new schema).
    val job1 = ShardKey("q1", "db", "t", None)
    Await.result(acquireAndSubmit(handle(job1, "a"), ctxWithHeader("schema-v1")), 5.seconds)
    InsertCoordinator.release(job1)
    assert(InsertCoordinator.bucketCountForTest == 0)

    val job2 = ShardKey("q2", "db", "t", None)
    Await.result(acquireAndSubmit(handle(job2, "b"), ctxWithHeader("schema-v2")), 5.seconds)
    InsertCoordinator.release(job2)

    // Re-acquiring a released key also starts fresh rather than reusing the old context.
    Await.result(acquireAndSubmit(handle(job1, "c"), ctxWithHeader("schema-v3")), 5.seconds)
    InsertCoordinator.release(job1)

    assert(headers.asScala.toList == List("schema-v1", "schema-v2", "schema-v3"))
    assert(InsertCoordinator.bucketCountForTest == 0)
  }

  test("distinct write jobs to the same table never share a bucket") {
    val headers = new ConcurrentLinkedQueue[String]()
    InsertCoordinator.setSenderForTest { (c, g) =>
      g.foreach(_ => headers.add(new String(c.schemaHeader, "UTF-8"))); g.foreach(_.promise.success(()))
    }
    val c1 = ctxWithHeader("v1", cfgWith(targetBytes = 1000))
    val c2 = ctxWithHeader("v2", cfgWith(targetBytes = 1000))
    val k1 = ShardKey("q1", "db", "t", None)
    val k2 = ShardKey("q2", "db", "t", None)
    val f1 = acquireAndSubmit(handle(k1, "a"), c1)
    val f2 = acquireAndSubmit(handle(k2, "b"), c2)
    InsertCoordinator.flush(k1); InsertCoordinator.flush(k2)
    Await.result(f1, 5.seconds); Await.result(f2, 5.seconds)
    InsertCoordinator.release(k1); InsertCoordinator.release(k2)
    assert(headers.asScala.toSet == Set("v1", "v2"))
  }

  test("batches still buffered when the last writer releases (abort path) are drained, not lost") {
    InsertCoordinator.setSenderForTest((_, g) => g.foreach(_.promise.success(())))
    val k = ShardKey("q", "db", "t", None)
    val f = acquireAndSubmit(handle(k, "a"), ctxWithHeader("h", cfgWith(targetBytes = 1000)))
    InsertCoordinator.release(k)
    Await.result(f, 5.seconds)
    assert(InsertCoordinator.bucketCountForTest == 0)
  }

  test("submit without acquire fails fast") {
    intercept[IllegalStateException] {
      InsertCoordinator.submit(handle(ShardKey("q", "db", "t", None), "a"))
    }
  }
}
