package org.infinispan.stats.wrappers;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.stats.CacheStatisticManager;
import org.infinispan.stats.ExtendedStatistic;
import org.infinispan.stats.TimeService;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.concurrent.ConcurrentMapFactory;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.concurrent.locks.LockManager;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.infinispan.stats.ExtendedStatistic.*;

/**
 * @author Mircea Markus <mircea.markus@jboss.com> (C) 2011 Red Hat Inc.
 * @author Diego Didona <didona@gsd.inesc-id.pt>
 * @author Pedro Ruivo
 * @since 5.2
 */
public class ExtendedStatisticLockManager implements LockManager {
   private final LockManager actual;
   private final CacheStatisticManager cacheStatisticManager;
   private final Map<Object, LockInfo> lockInfoMap = ConcurrentMapFactory.makeConcurrentMap();
   private final TimeService timeService;

   public ExtendedStatisticLockManager(LockManager actual, CacheStatisticManager cacheStatisticManager,
                                       TimeService timeService) {
      this.cacheStatisticManager = cacheStatisticManager;
      this.actual = actual;
      this.timeService = timeService;
   }

   @Override
   public boolean lockAndRecord(Object key, InvocationContext ctx, long timeoutMillis) throws InterruptedException {
      return actual.lockAndRecord(key, ctx, timeoutMillis);
   }

   @Override
   public void unlock(Collection<Object> lockedKeys, Object lockOwner) {
      List<LockInfo> acquiredLockInfo = new ArrayList<LockInfo>();
      for (Object key : lockedKeys) {
         LockInfo lockInfo = lockInfoMap.get(key);
         if (lockInfo != null && lockInfo.owner.equals(lockOwner)) {
            acquiredLockInfo.add(lockInfo);
            lockInfoMap.remove(key);
         }
      }
      actual.unlock(lockedKeys, lockOwner);
      long timestamp = timeService.now();
      for (LockInfo lockInfo : acquiredLockInfo) {
         lockInfo.updateStats(timestamp);
      }
   }

   @Override
   public void unlockAll(InvocationContext ctx) {
      List<LockInfo> acquiredLockInfo = new ArrayList<LockInfo>();
      for (Object key : ctx.getLockedKeys()) {
         LockInfo lockInfo = lockInfoMap.get(key);
         if (lockInfo != null && lockInfo.owner.equals(ctx.getLockOwner())) {
            acquiredLockInfo.add(lockInfo);
            lockInfoMap.remove(key);
         }
      }
      actual.unlockAll(ctx);
      long timestamp = timeService.now();
      for (LockInfo lockInfo : acquiredLockInfo) {
         lockInfo.updateStats(timestamp);
      }
   }

   @Override
   public boolean ownsLock(Object key, Object owner) {
      return actual.ownsLock(key, owner);
   }

   @Override
   public boolean isLocked(Object key) {
      return actual.isLocked(key);
   }

   @Override
   public Object getOwner(Object key) {
      return actual.getOwner(key);
   }

   @Override
   public String printLockInfo() {
      return actual.printLockInfo();
   }

   @Override
   public boolean possiblyLocked(CacheEntry entry) {
      return actual.possiblyLocked(entry);
   }

   @Override
   public int getNumberOfLocksHeld() {
      return actual.getNumberOfLocksHeld();
   }

   @Override
   public int getLockId(Object key) {
      return actual.getLockId(key);
   }

   @Override
   public boolean acquireLock(InvocationContext ctx, Object key, long timeoutMillis, boolean skipLocking) throws InterruptedException, TimeoutException {
      LockInfo lockInfo = new LockInfo(ctx);
      updateContentionStats(key, lockInfo);

      long start = timeService.now();
      boolean locked = actual.acquireLock(ctx, key, timeoutMillis, skipLocking);  //this returns false if you already have acquired the lock previously
      long end = timeService.now();

      lockInfo.lockTimeStamp = end;
      if (lockInfo.contention != null && locked) {
         lockInfo.lockWaiting = timeService.duration(start, end);
      }

      //if some owner tries to acquire the lock twice, we don't added it
      if (lockInfoMap.get(key) == null && locked) {
         lockInfoMap.put(key, lockInfo);
      }

      return locked;
   }

   @Override
   public boolean acquireLockNoCheck(InvocationContext ctx, Object key, long timeoutMillis, boolean skipLocking) throws InterruptedException, TimeoutException {
      LockInfo lockInfo = new LockInfo(ctx);
      updateContentionStats(key, lockInfo);

      long start = timeService.now();
      boolean locked = actual.acquireLockNoCheck(ctx, key, timeoutMillis, skipLocking);  //this returns false if you already have acquired the lock previously
      long end = timeService.now();

      lockInfo.lockTimeStamp = end;
      if (lockInfo.contention != null && locked) {
         lockInfo.lockWaiting = timeService.duration(start, end);
      }

      //if some owner tries to acquire the lock twice, we don't added it
      if (lockInfoMap.get(key) == null && locked) {
         lockInfoMap.put(key, lockInfo);
      }

      return locked;
   }

   private void updateContentionStats(Object key, LockInfo lockInfo) {
      Object holder = getOwner(key);
      if (holder != null) {
         if (holder != lockInfo.owner) {
            if (holder instanceof GlobalTransaction) {
               lockInfo.contention = ((GlobalTransaction) holder).isRemote() ? LOCK_CONTENTION_TO_REMOTE :
                     LOCK_CONTENTION_TO_LOCAL;
            } else {
               lockInfo.contention = LOCK_CONTENTION_TO_LOCAL;
            }
         }
      }
   }

   private class LockInfo {
      private final GlobalTransaction owner;
      private final boolean local;
      private long lockTimeStamp = -1;
      private ExtendedStatistic contention = null;
      private long lockWaiting = -1;

      public LockInfo(InvocationContext ctx) {
         owner = ctx.getLockOwner() instanceof GlobalTransaction ? (GlobalTransaction) ctx.getLockOwner() : null;
         local = ctx.isOriginLocal();
      }

      public final void updateStats(long releaseTimeStamp) {
         long holdTime = timeService.duration(lockTimeStamp, releaseTimeStamp);
         cacheStatisticManager.add(LOCK_HOLD_TIME, holdTime, owner, local);
         if (lockWaiting != -1) {
            cacheStatisticManager.add(LOCK_WAITING_TIME, lockWaiting, owner, local);
            cacheStatisticManager.increment(NUM_WAITED_FOR_LOCKS, owner, local);
         }
         cacheStatisticManager.increment(NUM_HELD_LOCKS, owner, local);
         if (contention != null) {
            cacheStatisticManager.increment(contention, owner, local);
         }
      }
   }
}
