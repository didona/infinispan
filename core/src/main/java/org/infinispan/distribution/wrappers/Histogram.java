/*
 * INESC-ID, Instituto de Engenharia de Sistemas e Computadores Investigação e Desevolvimento em Lisboa
 * Copyright 2013 INESC-ID and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 3.0 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.distribution.wrappers;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Massive hack for a noble cause!
 *
 * @author Diego Didona <didona@gsd.inesc-id.pt>
 * @since 5.2
 */


public class Histogram {

   private AtomicInteger[] buckets;
   private int step;
   private long min;
   private long max;
   private int numBuckets;

   private String fileName;

   public Histogram(long min, long max, int step) {
      this.step = step;
      this.min = min;
      this.max = max;
      this.numBuckets = (int) (max - min) / step;
      this.buckets = new AtomicInteger[numBuckets];
      for (int i = 0; i < numBuckets; i++) {
         this.buckets[i] = new AtomicInteger(0);
      }
      this.fileName = "Histogram.txt";
   }


   public Histogram(long min, long max, int step, String name) {
      this.step = step;
      this.min = min;
      this.max = max;
      this.numBuckets = (int) (max - min) / step;
      this.buckets = new AtomicInteger[numBuckets];
      for (int i = 0; i < numBuckets; i++) {
         this.buckets[i] = new AtomicInteger(0);
      }
      this.fileName = name;

   }

   public Histogram() {
   }

   public void initBuckets() {
      this.buckets = new AtomicInteger[numBuckets];
      for (int i = 0; i < numBuckets; i++) {
         this.buckets[i] = new AtomicInteger(0);
      }
   }

   public int getStep() {
      return step;
   }

   public void setStep(int step) {
      this.step = step;
   }

   public long getMin() {
      return min;
   }

   public void setMin(long min) {
      this.min = min;
   }

   public long getMax() {
      return max;
   }

   public void setMax(long max) {
      this.max = max;
   }

   public int getNumBuckets() {
      return numBuckets;
   }

   public void setNumBuckets(int numBuckets) {
      this.numBuckets = numBuckets;
   }

   public String getFileName() {
      return fileName;
   }

   public void setFileName(String fileName) {
      this.fileName = fileName;
   }

   public void insertSample(double sample) {
      int index = determineIndex(sample);
      this.buckets[index].incrementAndGet();
   }

   private int determineIndex(double sample) {
      int index;
      if (sample > max) {
         index = numBuckets - 1;
      } else if (sample <= min) {
         index = 0;
      } else {
         index = (int) ((Math.ceil((sample - min) / step)) - 1);
      }
      return index;
   }

   public void dumpHistogram() {
      try {
         File f = new File(this.fileName);
         PrintWriter pw = new PrintWriter(f);
         for (int k = 0; k < this.numBuckets; k++) {
            pw.println(step * (k + 1) + "\t" + this.buckets[k].get());
         }
         pw.close();
      } catch (IOException e) {
         e.printStackTrace();
      }
   }

}
