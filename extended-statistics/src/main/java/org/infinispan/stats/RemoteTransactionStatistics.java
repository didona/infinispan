package org.infinispan.stats;

import org.infinispan.stats.container.RemoteExtendedStatisticsContainer;

/**
 * Websiste: www.cloudtm.eu Date: 20/04/12
 *
 * @author Diego Didona <didona@gsd.inesc-id.pt>
 * @since 5.2
 */
public class RemoteTransactionStatistics extends TransactionStatistics {

   public RemoteTransactionStatistics(TimeService timeService) {
      super(new RemoteExtendedStatisticsContainer(), timeService);
   }

   @Override
   public final String toString() {
      return "RemoteTransactionStatistics{" + super.toString();
   }

   @Override
   public final void onPrepareCommand() {
      //nop
   }

   @Override
   public final boolean isLocalTransaction() {
      return false;
   }

   @Override
   protected final void terminate() {
      //nop
   }
}
