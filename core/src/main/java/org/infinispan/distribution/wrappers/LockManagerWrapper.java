/*
 * INESC-ID, Instituto de Engenharia de Sistemas e Computadores Investigação e Desevolvimento em Lisboa
 * Copyright 2013 INESC-ID and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 3.0 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.distribution.wrappers;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.stats.LockRelatedStatsHelper;
import org.infinispan.stats.TransactionsStatisticsRegistry;
import org.infinispan.stats.container.TransactionStatistics;
import org.infinispan.stats.topK.StreamLibContainer;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collection;

import static org.infinispan.stats.ExposedStatistic.*;

/**
 * @author Mircea Markus <mircea.markus@jboss.com> (C) 2011 Red Hat Inc.
 * @author Diego Didona <didona@gsd.inesc-id.pt>
 * @author Pedro Ruivo
 * @since 5.2
 */
public class LockManagerWrapper implements LockManager {
   private static final Log log = LogFactory.getLog(LockManagerWrapper.class);
   private final LockManager actual;
   private final StreamLibContainer streamLibContainer;
   private boolean sampleHoldTimes = false;
   private final boolean isGMU;

   public LockManagerWrapper(LockManager actual, StreamLibContainer streamLibContainer, boolean sampleHoldTimes, boolean isGMU) {
      this.actual = actual;
      this.streamLibContainer = streamLibContainer;
      this.sampleHoldTimes = sampleHoldTimes;
      this.isGMU = isGMU;
   }

   @Override
   public boolean lockAndRecord(Object key, InvocationContext ctx, long timeoutMillis) throws InterruptedException {
      if (log.isTraceEnabled())
         log.tracef("LockManagerWrapper.lockAndRecord");
      return actual.lockAndRecord(key, ctx, timeoutMillis);
   }

   @Override
   public boolean shareLockAndRecord(Object key, InvocationContext ctx, long timeoutMillis) throws InterruptedException {
      if (log.isTraceEnabled()) log.tracef("LockManagerWrapper.shareLockAndRecord");
      return actual.shareLockAndRecord(key, ctx, timeoutMillis);
   }

   @Override
   public void unlock(Collection<Object> lockedKeys, Object lockOwner) {
      if (log.isTraceEnabled()) log.tracef("LockManagerWrapper.unlock");
      actual.unlock(lockedKeys, lockOwner);
   }

   @Override
   /*
   THIS is the method invoked at commit/abort time to unlock keys in GMU
   //TODO: what about RR? Does it use the completion notification command???? (and thus uses the unlock from the above method?)
   */
   public void unlockAll(InvocationContext ctx) {
      final boolean trace = log.isTraceEnabled();
      //If we're not in tx class, do nothing
      if (ctx.isInTxScope()) {
         if (trace) {
            log.tracef("LockManagerWrapper.unlockAll");
         }
         flushLocksIfNeeded(((TxInvocationContext) ctx).getGlobalTransaction(), isGMU);
      }
      actual.unlockAll(ctx);
   }

   @Override
   public boolean ownsLock(Object key, Object owner) {
      if (log.isTraceEnabled()) log.tracef("LockManagerWrapper.ownsLock");
      return actual.ownsLock(key, owner);
   }

   @Override
   public boolean isLocked(Object key) {
      if (log.isTraceEnabled()) log.tracef("LockManagerWrapper.isExclusiveLocked");
      return actual.isLocked(key);
   }

   @Override
   public Object getOwner(Object key) {
      if (log.isTraceEnabled()) log.tracef("LockManagerWrapper.getOwner");
      return actual.getOwner(key);
   }

   @Override
   public String printLockInfo() {
      if (log.isTraceEnabled()) log.tracef("LockManagerWrapper.printLockInfo");
      return actual.printLockInfo();
   }

   @Override
   public boolean possiblyLocked(CacheEntry entry) {
      if (log.isTraceEnabled()) log.tracef("LockManagerWrapper.possiblyLocked");
      return actual.possiblyLocked(entry);
   }

   @Override
   public int getNumberOfLocksHeld() {
      if (log.isTraceEnabled()) log.tracef("LockManagerWrapper.getNumberOfLocksHeld");
      return actual.getNumberOfLocksHeld();
   }

   @Override
   public int getLockId(Object key) {
      log.tracef("LockManagerWrapper.getLockId");
      return actual.getLockId(key);
   }

   @Override
   public boolean acquireLock(InvocationContext ctx, Object key, long timeoutMillis, boolean skipLocking, boolean shared) throws InterruptedException, TimeoutException {
      if (log.isTraceEnabled()) log.tracef("LockManagerWrapper.acquireLock");

      long lockingTime = 0;
      boolean experiencedContention = false;
      boolean txScope = ctx.isInTxScope();

      final TransactionStatistics transactionStatistics = TransactionsStatisticsRegistry.getTransactionStatistics();
      if (txScope && transactionStatistics != null) {
         experiencedContention = updateContentionStats(transactionStatistics, key, (TxInvocationContext) ctx);
         lockingTime = System.nanoTime();
      }

      boolean locked;
      try {
         locked = actual.acquireLock(ctx, key, timeoutMillis, skipLocking, shared);  //this returns false if you already have acquired the lock previously
      } catch (TimeoutException e) {
         streamLibContainer.addLockInformation(key, experiencedContention, true);
         throw e;
      } catch (InterruptedException e) {
         streamLibContainer.addLockInformation(key, experiencedContention, true);
         throw e;
      }

      streamLibContainer.addLockInformation(key, experiencedContention, false);

      if (txScope && experiencedContention && locked) {
         lockingTime = System.nanoTime() - lockingTime;
         transactionStatistics.addValue(LOCK_WAITING_TIME, lockingTime);
         transactionStatistics.incrementValue(NUM_WAITED_FOR_LOCKS);
      }
      if (locked && transactionStatistics != null) {
         transactionStatistics.addTakenLock(key); //Idempotent
      }

      return locked;
   }

   @Override
   public boolean acquireLockNoCheck(InvocationContext ctx, Object key, long timeoutMillis, boolean skipLocking, boolean shared) throws InterruptedException, TimeoutException {
      if (log.isTraceEnabled()) log.tracef("LockManagerWrapper.acquireLockNoCheck");

      long lockingTime = 0;
      boolean experiencedContention = false;
      boolean txScope = ctx.isInTxScope();

      final TransactionStatistics transactionStatistics = TransactionsStatisticsRegistry.getTransactionStatistics();
      if (txScope && transactionStatistics != null) {
         experiencedContention = this.updateContentionStats(transactionStatistics, key, (TxInvocationContext) ctx);
         lockingTime = System.nanoTime();
      }

      boolean locked = actual.acquireLockNoCheck(ctx, key, timeoutMillis, skipLocking, shared);

      if (txScope && experiencedContention && locked) {
         lockingTime = System.nanoTime() - lockingTime;
         transactionStatistics.addValue(LOCK_WAITING_TIME, lockingTime);
         transactionStatistics.incrementValue(NUM_WAITED_FOR_LOCKS);
      }
      if (locked && transactionStatistics != null) {
         transactionStatistics.addTakenLock(key); //Idempotent
      }
      return locked;
   }

   private boolean updateContentionStats(TransactionStatistics transactionStatistics, Object key, TxInvocationContext tctx) {
      Object owner = getOwner(key);
      GlobalTransaction holder = owner instanceof GlobalTransaction ? (GlobalTransaction) owner : null;
      if (holder != null) {
         GlobalTransaction me = tctx.getGlobalTransaction();
         if (holder != me) {
            if (holder.isRemote()) {
               transactionStatistics.incrementValue(LOCK_CONTENTION_TO_REMOTE);
            } else {
               transactionStatistics.incrementValue(LOCK_CONTENTION_TO_LOCAL);
            }
            return true;
         }
      }
      return false;
   }

   private void newFlushRRLocks(GlobalTransaction lockOwner) {
      final boolean trace = log.isTraceEnabled();
      TransactionStatistics txs = TransactionsStatisticsRegistry.getTransactionStatistics();
      if (txs == null) {
         log.fatal("Txs statistic is null upon flushing for " + lockOwner);
         return;
      }
      if (trace)
         log.trace("DLOCKS going to flush in RR mode for " + lockOwner);
      txs.immediateLockFlushIfNeeded();

   }


   private void flushPendingLocksIfNeeded(GlobalTransaction lockOwner) {
      final boolean trace = log.isTraceEnabled();
      if (trace) {
         if (!sampleHoldTimes) {
            log.trace("DLOCKS: not sampling for " + lockOwner.globalId() + " as sampleHT is not enabled");
         }
         if (!LockRelatedStatsHelper.maybePendingLocks(lockOwner)) {
            log.trace("DLOCKS: not sampling for " + lockOwner.globalId() + " as no locks may be pending");
         }
      }

      if (LockRelatedStatsHelper.maybePendingLocks(lockOwner)) {
         if (trace) {
            log.trace("DLOCKS : Will I flush locks for " + lockOwner.globalId() + "?");
         }
         TransactionsStatisticsRegistry.flushPendingRemoteLocksIfNeeded(lockOwner);
      }

   }

   private void flushLocksIfNeeded(GlobalTransaction lockOwner, final boolean isGMU) {
      if (TransactionsStatisticsRegistry.isActive() && sampleHoldTimes) {
         if (!isGMU) {
            newFlushRRLocks(lockOwner);
         } else {
            flushPendingLocksIfNeeded(lockOwner);
         }
      } else {
         log.trace("DLOCKS: not sampling for " + lockOwner.globalId() + " as Stats are not enabled");
      }
   }

}
