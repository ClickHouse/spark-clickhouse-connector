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

import java.util.concurrent.{ScheduledExecutorService, TimeUnit}
import java.util.concurrent.locks.ReentrantLock
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future

/**
 * Per-target buffer of record-batch handles. A flush is triggered by size (bufferedBytes >=
 * targetBytes), flushNow() (one-shot, used at task commit), seal() (sticky, used once the bucket
 * has no writers left), or a linger timer; draining and the sender call always run on `pool`,
 * never on the submitting (task) thread. submit() blocks while bufferedBytes >= maxBufferedBytes
 * (backpressure). `sender` performs the insert and completes each handle's promise.
 *
 * Byte accounting: a batch's bytes count against `bufferedBytes` from the moment it is buffered
 * until its group's send completes (acked or failed). In-flight groups therefore keep occupying
 * the byte budget, so backpressure reflects total outstanding work, not just the unsent buffer.
 */
final class InsertBucket(
  key: ShardKey,
  ctx: BucketContext,
  pool: ScheduledExecutorService,
  sender: (BucketContext, Seq[BatchHandle]) => Unit
) {
  private val lock = new ReentrantLock()
  private val notFull = lock.newCondition()
  private val buffer = ArrayBuffer.empty[BatchHandle]
  // Total outstanding bytes: buffered (not yet sent) plus in-flight (sent, not yet acked).
  private var bufferedBytes = 0L
  private var isSealed = false
  private var draining = false
  private var lingerScheduled = false
  // Set by flushNow()/seal()/linger to force a flush of a sub-target remainder.
  private var forceFlush = false
  // Number of writers holding this bucket. Only read/written inside InsertCoordinator's
  // ConcurrentHashMap.compute, which serialises access per key.
  private[coalesce] var refs = 0

  def submit(h: BatchHandle): Future[Unit] = {
    lock.lock()
    try {
      while (bufferedBytes >= ctx.config.maxBufferedBytes && !isSealed) notFull.await()
      buffer += h
      bufferedBytes += h.payload.sizeBytes
      val overTarget = pendingBytes >= ctx.config.targetBytes
      if (!overTarget && !lingerScheduled && ctx.config.lingerMs > 0) {
        lingerScheduled = true
        pool.schedule(
          new Runnable { override def run(): Unit = onLinger() },
          ctx.config.lingerMs,
          TimeUnit.MILLISECONDS
        )
      }
      if (overTarget || isSealed) ensureDraining()
      h.future
    } finally lock.unlock()
  }

  /**
   * Flush everything buffered now, without sealing: once the buffer drains, later batches resume
   * normal target-size grouping. A committing task uses this so its own batches are sent while
   * the bucket keeps coalescing for the job's other tasks.
   */
  def flushNow(): Unit = {
    lock.lock()
    try if (buffer.nonEmpty) { forceFlush = true; ensureDraining() }
    finally lock.unlock()
  }

  /** Sticky: every later submit is sent immediately. Used when the last writer releases. */
  def seal(): Unit = {
    lock.lock()
    try { isSealed = true; forceFlush = true; ensureDraining() }
    finally lock.unlock()
  }

  private def onLinger(): Unit = {
    lock.lock()
    try {
      lingerScheduled = false
      if (buffer.nonEmpty) { forceFlush = true; ensureDraining() }
    } finally lock.unlock()
  }

  // caller must hold `lock`; bytes still sitting in the buffer (not yet handed to the sender)
  private def pendingBytes: Long = {
    var n = 0L
    var i = 0
    while (i < buffer.length) { n += buffer(i).payload.sizeBytes; i += 1 }
    n
  }

  // caller must hold `lock`
  private def ensureDraining(): Unit =
    if (!draining && buffer.nonEmpty) {
      draining = true
      pool.execute(new Runnable { override def run(): Unit = drainLoop() })
    }

  private def drainLoop(): Unit = {
    var continue = true
    while (continue) {
      var group: Seq[BatchHandle] = null
      var groupBytes = 0L
      lock.lock()
      try {
        val flushAll = isSealed || forceFlush
        if (buffer.isEmpty || (!flushAll && pendingBytes < ctx.config.targetBytes)) {
          // A linger-forced flush is one-shot: clear it once the buffer is drained so subsequent
          // batches resume normal target-size grouping. seal() stays sticky via isSealed.
          forceFlush = false
          draining = false
          continue = false
        } else {
          val taken = takeGroup(flushAll)
          group = taken._1
          groupBytes = taken._2
          // A forced group takes the whole buffer, so the flushNow()/linger request is satisfied.
          // Clear it now: batches arriving while this group is in flight resume normal grouping
          // instead of being sent as an extra, premature group.
          forceFlush = false
          // bytes stay counted while in flight; we do NOT signal notFull here.
        }
      } finally lock.unlock()
      if (group != null) {
        try sender(ctx, group)
        catch {
          // The sender contract is to complete handle promises itself (all-or-nothing) and not
          // throw. Defend against a buggy/unexpected throw: fail any still-pending handles so
          // awaiting tasks don't hang, and let the loop continue so `draining` is reset normally
          // (otherwise the bucket would wedge with draining=true and deadlock backpressure).
          case t: Throwable => group.foreach(h => if (!h.promise.isCompleted) h.promise.failure(t))
        } finally releaseInFlight(groupBytes)
      }
    }
  }

  // Release in-flight bytes once a group's send completes, then admit any blocked submitters.
  private def releaseInFlight(groupBytes: Long): Unit = {
    lock.lock()
    try {
      bufferedBytes -= groupBytes
      notFull.signalAll()
    } finally lock.unlock()
  }

  // caller must hold `lock`; removes a group from the buffer, returning it and its byte size.
  // bufferedBytes is NOT decremented here: the group remains "in flight" until its send completes.
  private def takeGroup(flushAll: Boolean): (Seq[BatchHandle], Long) = {
    val group = ArrayBuffer.empty[BatchHandle]
    var groupBytes = 0L
    while (buffer.nonEmpty && (flushAll || groupBytes < ctx.config.targetBytes)) {
      val h = buffer.remove(0)
      group += h
      groupBytes += h.payload.sizeBytes
    }
    (group.toSeq, groupBytes)
  }
}
