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
public interface TimeService {

   /**
    * @return the current clock time in milliseconds. Note that this time depends of the system time.
    */
   long wallClockTime();

   /**
    * It is equivalent to {@code wallClockTimeDuration(startWallClockTime, wallClockTime())}.
    *
    * @param startWallClockTime a value returned by invoking {@link #wallClockTime()}.
    * @return the duration between current wall time and {@param startWallClockTime}. It returns zero if {@param
    *         startWallClockTime} is lower than zero and it returns a negative value is {@param startWallClockTime} if
    *         is higher than current wall clock.
    */
   long wallClockTimeDuration(long startWallClockTime);

   /**
    * @param startWallClockTime a value returned by invoking {@link #wallClockTime()}.
    * @param endWallClockTime   a value returned by invoking {@link #wallClockTime()}.
    * @return the duration between {@param endWallClockTime} and {@param startWallClockTime}. It returns zero if {@param
    *         startWallClockTime} or {@param endWallClockTime} are lower than zero and it returns a negative value if
    *         {@param startWallClockTime} is higher than {@param endWallClockTime}.
    */
   long wallClockTimeDuration(long startWallClockTime, long endWallClockTime);

   /**
    * @param precise {@code true} for a precise current time calculation.
    * @return the current time.
    */
   long time(boolean precise);

   /**
    * It is equivalent to {@code timeDuration(startTime, time(precise))}.
    *
    * @param startTime      a value returned by previously invoking {@link #time(boolean)}.
    * @param outputTimeUnit the {@link TimeUnit} of the returned duration.
    * @param precise        {@code true} for a precise current time calculation.
    * @return the duration between the current time and {@param startTime}. It returns zero if {@param startTime} is
    *         lower than zero or if {@param startTime} is higher than the current time.
    */
   long timeDuration(long startTime, TimeUnit outputTimeUnit, boolean precise);

   /**
    * @param startTime      a value returned by previously invoking {@link #time(boolean)}.
    * @param endTime        a value returned by previously invoking {@link #time(boolean)}.
    * @param outputTimeUnit the {@link TimeUnit} of the returned duration.
    * @return the duration between the {@param endTime} and {@param startTime}. It returns zero if {@param startTime} or
    *         {@param endTime} are lower than zero or if {@param startTime} is higher than the {@param endTime}.
    */
   long timeDuration(long startTime, long endTime, TimeUnit outputTimeUnit);

   /**
    * @param endTime a value returned by invoking {@link #time(boolean)}.
    * @param precise {@code true} for a precise current time calculation.
    * @return {@code true} if the {@param endTime} is lower or equals than the current time.
    */
   boolean isTimeExpired(long endTime, boolean precise);

   /**
    * @param endTime        the end timestamp returned by {@link #expectedEndTime(long, java.util.concurrent.TimeUnit,
    *                       boolean)}.
    * @param outputTimeUnit the {@link TimeUnit} of the returned remaining time.
    * @param precise        {@code true} for a precise current time calculation.
    * @return the remaining time, in {@link TimeUnit}, until the {@param endTime} is reached.
    */
   long remainingTime(long endTime, TimeUnit outputTimeUnit, boolean precise);

   /**
    * @param duration      the duration required
    * @param inputTimeUnit the {@link TimeUnit} of the {@param duration}.
    * @param precise       {@code true} for a precise current time calculation.
    * @return the expected end time
    */
   long expectedEndTime(long duration, TimeUnit inputTimeUnit, boolean precise);

}
