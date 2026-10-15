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

import com.clickhouse.spark.Logging
import com.clickhouse.spark.Utils
import com.clickhouse.spark.exception.{CHException, RetryableCHException}

import java.util.concurrent.{ConcurrentHashMap, Executors, ScheduledExecutorService, ThreadFactory}
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
 * Executor-wide coordinator: routes submitted batch handles into per-ShardKey buckets and drains
 * them on a shared sender pool. One instance per executor JVM (Scala object). The pool is created
 * lazily on first acquire using the first config's senderThreads.
 *
 * Bucket lifecycle: a writer calls acquire(key, ctx) before its first submit to a key, flush(key)
 * at commit, and release(key) when it closes. Buckets are reference-counted; when the last writer
 * releases, the bucket is sealed (so any remainder drains) and removed, so it never outlives its
 * write job and a later write to the same table starts with a fresh BucketContext.
 */
object InsertCoordinator extends Logging {

  private val buckets = new ConcurrentHashMap[ShardKey, InsertBucket]()
  @volatile private var poolRef: ScheduledExecutorService = _
  @volatile private var sender: (BucketContext, Seq[BatchHandle]) => Unit = performInsert

  Runtime.getRuntime.addShutdownHook(new Thread(() => shutdown(), "ch-insert-coordinator-shutdown"))

  /** Register a writer on the bucket for `key`, creating it with `ctx` if absent. */
  def acquire(key: ShardKey, ctx: BucketContext): Unit =
    buckets.compute(
      key,
      (_, existing) => {
        val b =
          if (existing != null) existing
          else new InsertBucket(key, ctx, pool(ctx.config.senderThreads), sender)
        b.refs += 1
        b
      }
    )

  /** Submit to an acquired bucket. The caller must hold a reference via acquire(h.bucket, _). */
  def submit(h: BatchHandle): Future[Unit] = {
    val b = buckets.get(h.bucket)
    if (b == null) throw new IllegalStateException(s"submit to unacquired coalesce bucket ${h.bucket}")
    b.submit(h)
  }

  /** One-shot flush of whatever the bucket holds; the bucket keeps coalescing afterwards. */
  def flush(key: ShardKey): Unit = {
    val b = buckets.get(key)
    if (b != null) b.flushNow()
  }

  /** Drop a writer's reference; the last release seals (drains the remainder) and removes it. */
  def release(key: ShardKey): Unit =
    buckets.computeIfPresent(
      key,
      (_, b) => {
        b.refs -= 1
        if (b.refs > 0) b
        else { b.seal(); null }
      }
    )

  private def pool(threads: Int): ScheduledExecutorService = {
    if (poolRef == null) synchronized {
      if (poolRef == null) poolRef = Executors.newScheduledThreadPool(math.max(1, threads), daemonFactory)
    }
    poolRef
  }

  private val daemonFactory: ThreadFactory = new ThreadFactory {
    private val n = new AtomicInteger(0)
    override def newThread(r: Runnable): Thread = {
      val t = new Thread(r, s"ch-insert-sender-${n.incrementAndGet()}")
      t.setDaemon(true)
      t
    }
  }

  /** Production insert: assemble the stream, set the dedup token, retry, ack all-or-nothing. */
  private def performInsert(ctx: BucketContext, group: Seq[BatchHandle]): Unit = {
    val settings = ctx.config.dedupMode match {
      case "coalesced" => ctx.baseSettings + ("insert_deduplication_token" -> DedupToken.forGroup(group))
      case _ => ctx.baseSettings
    }
    val result = Utils.retry[Unit, RetryableCHException](ctx.config.maxRetry, ctx.config.retryInterval) {
      ctx.client.insertArrowStream(
        ctx.table,
        ArrowStreamAssembler.lazyStream(ctx.schemaHeader, group),
        ctx.database,
        settings
      ) match {
        case Right(_) => ()
        case Left(e: CHException) if ctx.config.retryableErrorCodes.contains(e.code) =>
          throw RetryableCHException(e.code, e.reason, Some(ctx.client.nodeSpec))
        case Left(e) => throw e
      }
    }
    result match {
      case Success(_) => group.foreach { h => h.payload.close(); h.promise.success(()) }
      case Failure(ex) => group.foreach { h => h.payload.close(); h.promise.failure(ex) }
    }
  }

  private def shutdown(): Unit = {
    val p = poolRef
    if (p != null) p.shutdownNow()
    buckets.clear()
  }

  // test seams
  // NOTE: a bucket snapshots `sender` at creation, so this must be called BEFORE the first submit
  // for a given key (tests reset all buckets via shutdownForTest between cases).
  private[coalesce] def setSenderForTest(s: (BucketContext, Seq[BatchHandle]) => Unit): Unit = sender = s
  private[coalesce] def bucketCountForTest: Int = buckets.size()
  private[coalesce] def shutdownForTest(): Unit = {
    shutdown()
    poolRef = null
    sender = performInsert
  }
}
