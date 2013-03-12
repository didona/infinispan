package org.infinispan.stats;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.stats.exception.ExtendedStatisticNotFoundException;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.concurrent.ConcurrentMapFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.concurrent.ConcurrentMap;

/**
 * Websiste: www.cloudtm.eu Date: 20/04/12
 *
 * @author Diego Didona <didona@gsd.inesc-id.pt>
 * @author Pedro Ruivo
 * @since 5.2
 */
public final class CacheStatisticManager {

   private static final Log log = LogFactory.getLog(CacheStatisticManager.class);
   /**
    * collects statistic for a remote transaction
    */
   private final ConcurrentMap<GlobalTransaction, RemoteTransactionStatistics> remoteTransactionStatistics =
         ConcurrentMapFactory.makeConcurrentMap();
   /**
    * collects statistic for a local transaction
    */
   private final ConcurrentMap<GlobalTransaction, LocalTransactionStatistics> localTransactionStatistics =
         ConcurrentMapFactory.makeConcurrentMap();
   /**
    * collects statistic for a Infinispan cache
    */
   private final CacheStatisticCollector cacheStatisticCollector;
   private final boolean isOptimisticLocking;
   private final TimeService timeService;

   public CacheStatisticManager(Configuration configuration, TimeService timeService) {
      this.timeService = timeService;
      this.isOptimisticLocking = configuration.transaction().lockingMode() == LockingMode.OPTIMISTIC;
      this.cacheStatisticCollector = new CacheStatisticCollector(timeService);
   }

   public void add(ExtendedStatistic stat, double value, GlobalTransaction globalTransaction, boolean local) {
      TransactionStatistics txs = getTransactionStatistic(globalTransaction, local);
      if (txs == null) {
         if (log.isDebugEnabled()) {
            log.debugf("Trying to add %s to %s but no transaction exist. Add to Cache Statistic", value, stat);
         }
         if (local) {
            cacheStatisticCollector.addLocalValue(stat, value);
         } else {
            cacheStatisticCollector.addRemoteValue(stat, value);
         }
         return;
      }
      txs.addValue(stat, value);
   }

   public void increment(ExtendedStatistic stat, GlobalTransaction globalTransaction, boolean local) {
      TransactionStatistics txs = getTransactionStatistic(globalTransaction, local);
      if (txs == null) {
         if (log.isDebugEnabled()) {
            log.debugf("Trying to increment %s but no transaction exist. Add to Cache Statistic", stat);
         }
         if (local) {
            cacheStatisticCollector.addLocalValue(stat, 1);
         } else {
            cacheStatisticCollector.addRemoteValue(stat, 1);
         }
         return;
      }
      txs.addValue(stat, 1);
   }

   public void onPrepareCommand(GlobalTransaction globalTransaction, boolean local) {
      //NB: If I want to give up using the InboundInvocationHandler, I can create the remote transaction
      //here, just overriding the handlePrepareCommand
      TransactionStatistics txs = getTransactionStatistic(globalTransaction, local);
      if (txs == null) {
         log.error("Trying to invoke onPrepareCommand() but no transaction is associated");
         return;
      }
      txs.onPrepareCommand();
   }

   public void setTransactionOutcome(boolean commit, GlobalTransaction globalTransaction, boolean local) {
      TransactionStatistics txs = getTransactionStatistic(globalTransaction, local);
      if (txs == null) {
         log.errorf("Trying to set the transaction outcome to %s but no transaction is associated",
                    commit ? "COMMIT" : "ROLLBACK");
         return;
      }
      txs.setCommit(commit);
   }

   public double getAttribute(ExtendedStatistic stat) throws ExtendedStatisticNotFoundException {
      return cacheStatisticCollector.getAttribute(stat);
   }

   public double getPercentile(ExtendedStatistic stat, int percentile) throws ExtendedStatisticNotFoundException {
      return cacheStatisticCollector.getPercentile(stat, percentile);
   }

   public void markAsWriteTransaction(GlobalTransaction globalTransaction, boolean local) {
      TransactionStatistics txs = getTransactionStatistic(globalTransaction, local);
      if (txs == null) {
         log.error("Trying to mark the transaction as write transaction but no transaction is associated");
         return;
      }
      txs.markAsUpdateTransaction();
   }

   //This is synchronized because depending on the local/remote nature, a different object is created
   //Now, remote transactionStatistics get initialized at InboundInvocationHandler level
   public void beginTransaction(GlobalTransaction globalTransaction, boolean local) {
      if (local) {
         //Not overriding the InitialValue method leads me to have "null" at the first invocation of get()
         TransactionStatistics lts = createTransactionStatisticIfAbsent(globalTransaction, true);
         if (log.isTraceEnabled()) {
            log.tracef("Local transaction statistic is already initialized: %s", lts);
         }
      } else {
         TransactionStatistics rts = createTransactionStatisticIfAbsent(globalTransaction, false);
         if (log.isTraceEnabled()) {
            log.tracef("Using the remote transaction statistic %s for transaction %s", rts, globalTransaction);
         }
      }
   }

   public void terminateTransaction(GlobalTransaction globalTransaction) {
      TransactionStatistics txs = removeTransactionStatistic(globalTransaction, true);
      if (txs != null) {
         txs.terminateTransaction();
         cacheStatisticCollector.merge(txs);
      }
      txs = removeTransactionStatistic(globalTransaction, false);
      if (txs != null) {
         txs.terminateTransaction();
         cacheStatisticCollector.merge(txs);
      }
   }

   public void reset() {
      cacheStatisticCollector.reset();
   }

   public final boolean hasPendingTransactions() {
      log.debugf("Checking for pending transactions. local=%s, remote=%s", localTransactionStatistics.keySet(),
                 remoteTransactionStatistics.keySet());
      return !localTransactionStatistics.isEmpty() || !remoteTransactionStatistics.isEmpty();
   }

   private TransactionStatistics getTransactionStatistic(GlobalTransaction globalTransaction, boolean local) {
      if (globalTransaction != null) {
         return local ? localTransactionStatistics.get(globalTransaction) :
               remoteTransactionStatistics.get(globalTransaction);
      }
      return null;
   }

   private TransactionStatistics removeTransactionStatistic(GlobalTransaction globalTransaction, boolean local) {
      if (globalTransaction != null) {
         if (log.isTraceEnabled()) {
            log.tracef("Removing %s statistic for %s", local ? "local" : "remote", globalTransaction.globalId());
         }
         return local ? localTransactionStatistics.remove(globalTransaction) :
               remoteTransactionStatistics.remove(globalTransaction);
      }
      return null;
   }

   private TransactionStatistics createTransactionStatisticIfAbsent(GlobalTransaction globalTransaction, boolean local) {
      if (globalTransaction == null) {
         throw new NullPointerException("Global Transaction cannot be null");
      }
      TransactionStatistics ts = local ? localTransactionStatistics.get(globalTransaction) :
            remoteTransactionStatistics.get(globalTransaction);

      if (ts != null) {
         return ts;
      }

      if (local) {
         if (log.isTraceEnabled()) {
            log.tracef("Creating local statistic for %s", globalTransaction.globalId());
         }
         LocalTransactionStatistics lts = new LocalTransactionStatistics(isOptimisticLocking, timeService);
         TransactionStatistics existing = localTransactionStatistics.putIfAbsent(globalTransaction, lts);
         return existing == null ? lts : existing;
      } else {
         if (log.isTraceEnabled()) {
            log.tracef("Creating remote statistic for %s", globalTransaction.globalId());
         }
         RemoteTransactionStatistics rts = new RemoteTransactionStatistics(timeService);
         TransactionStatistics existing = remoteTransactionStatistics.putIfAbsent(globalTransaction, rts);
         return existing == null ? rts : existing;
      }
   }

}
