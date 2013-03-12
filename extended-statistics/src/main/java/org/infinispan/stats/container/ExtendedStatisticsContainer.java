package org.infinispan.stats.container;

import org.infinispan.stats.ExtendedStatistic;
import org.infinispan.stats.exception.ExtendedStatisticNotFoundException;

import java.io.PrintStream;

/**
 * Websiste: www.cloudtm.eu Date: 01/05/12
 *
 * @author Diego Didona <didona@gsd.inesc-id.pt>
 * @author Pedro Ruivo
 * @since 5.2
 */
public interface ExtendedStatisticsContainer {

   /**
    * it adds the {@code value} to the {@code statistic}. If the statistic does not exist in this container, it should
    * fail silently
    * 
    * @param statistic  the statistic
    * @param value      the value
    */
   void addValue(ExtendedStatistic statistic, double value);

   /**
    * it returns the value to the {@code statistic}.
    * 
    * @param statistic  the statistic
    * @throws org.infinispan.stats.exception.ExtendedStatisticNotFoundException   if the {@code statistic} was not found in this container
    * @return  the value
    */
   double getValue(ExtendedStatistic statistic) throws ExtendedStatisticNotFoundException;

   /**
    * it merges in {@code this} the values in {@code other}. If for some reason the {@code other} cannot be merged,
    * it should fail silently
    * 
    * @param other   the other container.
    */
   void merge(ExtendedStatisticsContainer other);

   void dumpTo(PrintStream stream);
   
   void dumpTo(StringBuilder stringBuilder);

}
