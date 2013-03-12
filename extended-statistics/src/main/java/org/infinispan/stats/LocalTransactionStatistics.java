package org.infinispan.stats;

import org.infinispan.stats.container.LocalExtendedStatisticsContainer;
import org.infinispan.stats.exception.ExtendedStatisticNotFoundException;

import static org.infinispan.stats.ExtendedStatistic.*;

/**
 * Websiste: www.cloudtm.eu Date: 20/04/12
 *
 * @author Diego Didona <didona@gsd.inesc-id.pt>
 * @author Pedro Ruivo
 * @since 5.2
 */
public class LocalTransactionStatistics extends TransactionStatistics {

   private final boolean optimisticLockingScheme;
   private boolean stillLocalExecution;

   public LocalTransactionStatistics(boolean optimisticLockingScheme, TimeService timeService) {
      super(new LocalExtendedStatisticsContainer(), timeService);
      this.optimisticLockingScheme = optimisticLockingScheme;
      this.stillLocalExecution = true;
   }

   @Override
   public final String toString() {
      return "LocalTransactionStatistics{" +
            "stillLocalExecution=" + stillLocalExecution +
            ", " + super.toString();
   }

   @Override
   public final void onPrepareCommand() {
      this.stillLocalExecution = false;

      if (!isReadOnly()) {
         this.addValue(WR_TX_LOCAL_EXECUTION_TIME, timeService.duration(initTime));
      }
      this.incrementValue(NUM_PREPARES);
   }

   @Override
   public final boolean isLocalTransaction() {
      return true;
   }

   @Override
   protected final void terminate() {
      if (!isReadOnly() && isCommit()) {
         moveValue(NUM_PUT, NUM_SUCCESSFUL_PUTS);
         moveValue(NUM_HELD_LOCKS, NUM_HELD_LOCKS_SUCCESS_TX);
         if (optimisticLockingScheme) {
            moveValue(WR_TX_LOCAL_EXECUTION_TIME, LOCAL_EXEC_NO_CONT);
         } else {
            try {
               double localLockAcquisitionTime = getValue(LOCK_WAITING_TIME);
               double totalLocalDuration = getValue(WR_TX_LOCAL_EXECUTION_TIME);
               this.addValue(LOCAL_EXEC_NO_CONT, (totalLocalDuration - localLockAcquisitionTime));
            } catch (ExtendedStatisticNotFoundException e) {
               log.warnf("Cannot calculate local execution time without contention. %s", e.getLocalizedMessage());
            }
         }
      }
   }
}
