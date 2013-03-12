package org.infinispan.stats.percentiles;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Date: 01/01/12 Time: 18:38
 *
 * @author Roberto
 * @since 5.2
 */
public class ReservoirSampling implements PercentileStats {

   private static final int DEFAULT_NUM_SPOTS = 100;
   private final AtomicInteger index;
   private final Random rand;
   private double[] reservoir;
   private int numSpot;

   public ReservoirSampling() {
      this(DEFAULT_NUM_SPOTS);
   }

   public ReservoirSampling(int numSpots) {
      this.numSpot = numSpots;
      this.reservoir = createArray();
      this.index = new AtomicInteger(0);
      rand = new Random(System.nanoTime());

   }

   public final void insertSample(double sample) {
      int i = index.getAndIncrement();
      if (i < numSpot)
         reservoir[i] = sample;
      else {
         int rand_generated = rand.nextInt(i + 2);//should be nextInt(index+1) but nextInt is exclusive
         if (rand_generated < numSpot) {
            reservoir[rand_generated] = sample;
         }
      }
   }

   public final double get95Percentile() {
      return getKPercentile(95);
   }

   public final double get90Percentile() {
      return getKPercentile(90);
   }

   public final double get99Percentile() {
      return getKPercentile(99);
   }

   public final double getKPercentile(int k) {
      if (k < 0 || k > 100) {
         throw new RuntimeException("Wrong index in getKpercentile");
      }
      double[] copy = createArray();
      System.arraycopy(this.reservoir, 0, copy, 0, numSpot);
      Arrays.sort(copy);
      return copy[this.getIndex(k)];
   }

   public final void reset() {
      this.index.set(0);
      this.reservoir = createArray();
   }

   private int getIndex(int k) {
      //I solve the proportion k:100=x:NUM_SAMPLE
      //Every percentage is covered by NUM_SAMPLE / 100 buckets; I consider here only the first as representative
      //of a percentage
      return (int) (numSpot * (k - 1) / 100);
   }

   private double[] createArray() {
      return new double[numSpot];
   }
}


