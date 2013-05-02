/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

package org.infinispan.util;

import java.util.concurrent.TimeUnit;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.3
 */
public class DefaultTimeService implements TimeService {

   private volatile long cachedNanos = System.nanoTime();

   @Override
   public long wallClockTime() {
      return System.currentTimeMillis();
   }

   @Override
   public long wallClockTimeDuration(long startWallClockTime) {
      return wallClockTimeDuration(startWallClockTime, wallClockTime());
   }

   @Override
   public long wallClockTimeDuration(long startWallClockTime, long endWallClockTime) {
      return startWallClockTime < 0 || endWallClockTime < 0 ? 0 : endWallClockTime - startWallClockTime;
   }

   @Override
   public long time(boolean precise) {
      return precise ? (cachedNanos = System.nanoTime()) : cachedNanos;
   }

   @Override
   public long timeDuration(long startTime, TimeUnit outputTimeUnit, boolean precise) {
      return timeDuration(startTime, time(precise), outputTimeUnit);
   }

   @Override
   public long timeDuration(long startTime, long endTime, TimeUnit outputTimeUnit) {
      if (startTime < 0 || endTime < 0 || startTime >= endTime) {
         return 0;
      }
      return TimeUnit.NANOSECONDS.convert(endTime - startTime, outputTimeUnit);
   }

   @Override
   public boolean isTimeExpired(long endTime, boolean precise) {
      long now = time(precise);
      return endTime > 0 && now >= endTime;
   }

   @Override
   public long remainingTime(long endTime, TimeUnit outputTimeUnit, boolean precise) {
      if (endTime <= 0) {
         return 0;
      }
      long now = time(precise);
      return now > endTime ? 0 : TimeUnit.NANOSECONDS.convert(endTime - now, outputTimeUnit);
   }

   @Override
   public long expectedEndTime(long duration, TimeUnit inputTimeUnit, boolean precise) {
      if (duration <= 0) {
         return 0;
      }
      return time(precise) + inputTimeUnit.toNanos(duration);
   }
}
