package org.infinispan.stats.container;

import org.infinispan.stats.ExposedStatistic;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * A Statistic Snapshot;
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class StatisticsSnapshot {

   private final long[] snapshot;
   private final static Log log = LogFactory.getLog(StatisticsSnapshot.class);


   public StatisticsSnapshot(long[] snapshot) {
      this.snapshot = snapshot;
   }

   public final long getRemote(ExposedStatistic stat) {

      if (log.isTraceEnabled()) {
         log.trace("Querying for remote stat " + stat);
      }
      return snapshot[ConcurrentGlobalContainer.getRemoteIndex(stat)];
   }

   public final long getLocal(ExposedStatistic stat) {
      if (log.isTraceEnabled()) {
         log.trace("Querying for local stat " + stat);
      }
      return snapshot[ConcurrentGlobalContainer.getLocalIndex(stat)];
   }

   public final long getLastResetTime() {
      return snapshot[0];
   }
}
