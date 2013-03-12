package org.infinispan.stats.container;

import org.infinispan.stats.ExtendedStatistic;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.3
 */
public class RemoteExtendedStatisticsContainer extends BaseExtendedStatisticsContainer {
   public RemoteExtendedStatisticsContainer() {
      super(ExtendedStatistic.getRemoteStatsSize());
   }

   @Override
   public void merge(ExtendedStatisticsContainer other) {
      if (other instanceof RemoteExtendedStatisticsContainer) {
         RemoteExtendedStatisticsContainer otherLocal = (RemoteExtendedStatisticsContainer) other;
         for (int i = 0; i < stats.length; ++i) {
            this.stats[i] += otherLocal.stats[i];
         }
      }
   }

   @Override
   protected int getIndex(ExtendedStatistic statistic) {
      return statistic.getRemoteIndex();
   }
}
