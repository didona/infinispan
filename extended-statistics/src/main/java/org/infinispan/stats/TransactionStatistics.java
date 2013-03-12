package org.infinispan.stats;

import org.infinispan.stats.container.ExtendedStatisticsContainer;
import org.infinispan.stats.exception.ExtendedStatisticNotFoundException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import static org.infinispan.stats.ExtendedStatistic.*;


/**
 * @author Diego Didona <didona@gsd.inesc-id.pt>
 * @author Pedro Ruivo
 * @since 5.2
 */
public abstract class TransactionStatistics {

   //Here the elements which are common for local and remote transactions
   protected final long initTime;
   protected final Log log = LogFactory.getLog(getClass());
   private final ExtendedStatisticsContainer container;
   protected final TimeService timeService;
   private boolean isReadOnly;
   private boolean isCommit;
   private long lastOpTimestamp;


   protected TransactionStatistics(ExtendedStatisticsContainer container, TimeService timeService) {
      this.timeService = timeService;
      this.initTime = timeService.now();
      this.isReadOnly = true; //as far as it does not tries to perform a put operation
      this.container = container;
      if (log.isTraceEnabled()) {
         log.tracef("Created transaction statistics. Start time=%s", initTime);
      }
   }

   public final boolean isCommit() {
      return this.isCommit;
   }

   public final void setCommit(boolean commit) {
      isCommit = commit;
   }

   public final boolean isReadOnly() {
      return this.isReadOnly;
   }

   public final void markAsUpdateTransaction() {
      this.isReadOnly = false;
   }

   public final void addValue(ExtendedStatistic stat, double value) {
      container.addValue(stat, value);
      if (log.isTraceEnabled()) {
         log.tracef("Add %s to %s", value, stat);
      }
   }

   public final double getValue(ExtendedStatistic stat) throws ExtendedStatisticNotFoundException {
      double value = container.getValue(stat);
      if (log.isTraceEnabled()) {
         log.tracef("Value of %s is %s", stat, value);
      }
      return value;
   }

   public final void incrementValue(ExtendedStatistic stat) {
      this.addValue(stat, 1);
   }

   public final void terminateTransaction() {
      if (log.isTraceEnabled()) {
         log.tracef("Terminating transaction. Is read only? %s. Is commit? %s", isReadOnly, isCommit);
      }
      long execTime = timeService.duration(initTime);
      if (isReadOnly) {
         if (isCommit) {
            incrementValue(NUM_COMMITTED_RO_TX);
            addValue(RO_TX_SUCCESSFUL_EXECUTION_TIME, execTime);
            moveValue(NUM_GET, NUM_SUCCESSFUL_GETS_RO_TX);
            moveValue(NUM_REMOTE_GET, NUM_SUCCESSFUL_REMOTE_GETS_RO_TX);
         } else {
            incrementValue(NUM_ABORTED_RO_TX);
            addValue(RO_TX_ABORTED_EXECUTION_TIME, execTime);
         }
      } else {
         if (isCommit) {
            incrementValue(NUM_COMMITTED_WR_TX);
            addValue(WR_TX_SUCCESSFUL_EXECUTION_TIME, execTime);
            moveValue(NUM_GET, NUM_SUCCESSFUL_GETS_WR_TX);
            moveValue(NUM_REMOTE_GET, NUM_SUCCESSFUL_REMOTE_GETS_WR_TX);
            moveValue(NUM_PUT, NUM_SUCCESSFUL_PUTS_WR_TX);
            moveValue(NUM_REMOTE_PUT, NUM_SUCCESSFUL_REMOTE_PUTS_WR_TX);
         } else {
            incrementValue(NUM_ABORTED_WR_TX);
            addValue(WR_TX_ABORTED_EXECUTION_TIME, execTime);
         }
      }

      terminate();
   }

   public final void flushTo(ExtendedStatisticsContainer globalContainer) {
      if (log.isTraceEnabled()) {
         log.tracef("Flush this [%s] to %s", this, globalContainer);
      }
      globalContainer.merge(container);
   }

   public final void dump() {
      this.container.dumpTo(System.out);
   }

   @Override
   public String toString() {
      return "initTime=" + initTime +
            ", isReadOnly=" + isReadOnly +
            ", isCommit=" + isCommit +
            '}';
   }

   public long getLastOpTimestamp() {
      return lastOpTimestamp;
   }

   public void setLastOpTimestamp(long lastOpTimestamp) {
      this.lastOpTimestamp = lastOpTimestamp;
   }

   public abstract void onPrepareCommand();

   public abstract boolean isLocalTransaction();

   protected abstract void terminate();

   protected final void moveValue(ExtendedStatistic from, ExtendedStatistic to) {
      try {
         double value = container.getValue(from);
         container.addValue(to, value);
         log.debugf("Move value [%s] from [%s] to [%s]", value, from, to);
      } catch (ExtendedStatisticNotFoundException e) {
         log.warnf("Cannot move value from %s to %s", from, to);
      }
   }
}

