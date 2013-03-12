package org.infinispan.stats.container;

import org.infinispan.stats.ExtendedStatistic;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.3
 */
public class LocalExtendedStatisticsContainer extends BaseExtendedStatisticsContainer {
   public LocalExtendedStatisticsContainer() {
      super(ExtendedStatistic.getLocalStatsSize());
   }

   @Override
   public final void merge(ExtendedStatisticsContainer other) {
      if (other instanceof LocalExtendedStatisticsContainer) {
         LocalExtendedStatisticsContainer otherLocal = (LocalExtendedStatisticsContainer) other;
         for (int i = 0; i < stats.length; ++i) {
            this.stats[i] += otherLocal.stats[i];
         }
      }
   }

   @Override
   protected final int getIndex(ExtendedStatistic statistic) {
      return statistic.getLocalIndex();
   }
   
   @Override
   public final String toString() {
      return "LocalExtendedStatisticsContainer";
   }
   
}
