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
package org.infinispan.transaction.gmu;


import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.InequalVersionComparisonResult;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.container.versioning.gmu.GMUReadVersion;
import org.infinispan.container.versioning.gmu.GMUVersion;
import org.infinispan.container.versioning.gmu.GMUVersionGenerator;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.stats.ExposedStatistic;
import org.infinispan.stats.TransactionsStatisticsRegistry;
import org.infinispan.stats.container.TransactionStatistics;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.gmu.manager.CommittedTransaction;
import org.infinispan.util.Util;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.*;

import static org.infinispan.container.versioning.InequalVersionComparisonResult.*;
import static org.infinispan.container.versioning.gmu.GMUVersion.NON_EXISTING;

/**
 * @author Pedro Ruivo
 * @author Sebastiano Peluso
 * @since 5.2
 */
public class CommitLog {

   private static final Log log = LogFactory.getLog(CommitLog.class);
   private volatile GMUVersion mostRecentVersion;
   private volatile VersionEntry currentVersion;
   private GMUVersionGenerator versionGenerator;
   private boolean enabled = false;

   @Inject
   public void inject(VersionGenerator versionGenerator, Configuration configuration) {
      if (configuration.locking().isolationLevel() == IsolationLevel.SERIALIZABLE) {
         this.versionGenerator = (GMUVersionGenerator) versionGenerator;
      }
      enabled = this.versionGenerator != null;
   }

   //AFTER THE VersionVCFactory
   @Start(priority = 31)
   public void start() {
      if (!enabled) {
         return;
      }
      currentVersion = new VersionEntry((GMUVersion) versionGenerator.generateNew(), Collections.emptySet(), 0, 0L);
      mostRecentVersion = (GMUVersion) versionGenerator.generateNew();
   }

   @Stop
   public void stop() {

   }

   public final boolean isEnabled() {
      return enabled;
   }

   public final void initLocalTransaction(LocalTransaction localTransaction) {
      if (!enabled) {
         return;
      }
      localTransaction.setTransactionVersion(versionGenerator.updatedVersion(mostRecentVersion));
   }

   public final GMUVersion getCurrentVersion() {
      assertEnabled();
      //versions are immutable
      GMUVersion version = versionGenerator.updatedVersion(mostRecentVersion);
      //GMUVersion version = versionGenerator.updatedVersion(currentVersion.getVersion());
      if (log.isTraceEnabled()) {
         log.tracef("getCurrentVersion() ==> %s", version);
      }
      return version;
   }

   public final EntryVersion getOldestVersion() {
      VersionEntry iterator = currentVersion;
      while (iterator.getPrevious() != null) {
         iterator = iterator.getPrevious();
      }
      return iterator.getVersion();
   }

   public final EntryVersion getEntry(EntryVersion entryVersion) {
      GMUVersion gmuEntryVersion = (GMUVersion) entryVersion;
      VersionEntry versionEntry = currentVersion;
      while (versionEntry != null) {
         if (versionEntry.getVersion().getThisNodeVersionValue() == gmuEntryVersion.getThisNodeVersionValue()) {
            return versionEntry.getVersion();
         }
         versionEntry = versionEntry.getPrevious();
      }
      return getOldestVersion();
   }

   public final GMUVersion getAvailableVersionLessThan(EntryVersion other) {
      assertEnabled();
      if (other == null) {
         return versionGenerator.updatedVersion(mostRecentVersion);
         //return versionGenerator.updatedVersion(currentVersion.getVersion());
      }
      TransactionStatistics ts = TransactionsStatisticsRegistry.getTransactionStatistics();
      final boolean stat = ts != null;
      long all_init = 0, count = 0;
      if (stat)
         all_init = System.nanoTime();
      GMUVersion gmuVersion = (GMUVersion) other;

      if (gmuVersion.getThisNodeVersionValue() != NON_EXISTING) {
         return gmuVersion;
      }

      LinkedList<GMUVersion> possibleVersion = new LinkedList<GMUVersion>();

      VersionEntry firstFoundPossible = null; //These are used to optimize the search
      long concurrentClockNumber = 0L;

      /*
      *
      * If this is the node N, the concurrentClockNumber of a VersionEntry A is the value
      * of the last committed vector clock's N-th entry at the time A was "prepared" on this node.
       */

      VersionEntry iterator = currentVersion;

      while (iterator != null &&
              (firstFoundPossible == null ||
                      concurrentClockNumber < iterator.getVersion().getThisNodeVersionValue())) {
         if (stat)
            count++;
         if (isLessOrEquals(iterator.getVersion(), gmuVersion)) {
            possibleVersion.add(iterator.getVersion());

            if (firstFoundPossible == null) {
               firstFoundPossible = iterator;
               concurrentClockNumber = iterator.getConcurrentClockNumber();
            }

         }
         if (firstFoundPossible != null && concurrentClockNumber > iterator.getConcurrentClockNumber()) {
            //We move the bound before.
            concurrentClockNumber = iterator.getConcurrentClockNumber();

         }
         iterator = iterator.getPrevious();
      }
      GMUVersion ret = versionGenerator.mergeAndMax(possibleVersion.toArray(new GMUVersion[possibleVersion.size()]));
      if (stat) {
         ts.addValue(ExposedStatistic.COMMIT_LOG_GET_AVAILABLE_VERSION_LESS_THAN, System.nanoTime() - all_init);
         ts.incrementValue(ExposedStatistic.NUM_COMMIT_LOG_GET_AVAILABLE_VERSION_LESS_THAN);
         ts.addValue(ExposedStatistic.COMMIT_LOG_GET_AVAILABLE_VERSION_LESS_THAN_COUNT, count);
      }
      return ret;
   }

   public final GMUReadVersion getReadVersion(EntryVersion other) {
      if (other == null) {
         return null;
      }
      TransactionStatistics ts = TransactionsStatisticsRegistry.getTransactionStatistics();
      final boolean stat = ts != null;
      long all_init = 0, count = 0;
      if (stat)
         all_init = System.nanoTime();
      GMUVersion gmuVersion = (GMUVersion) other;
      GMUReadVersion gmuReadVersion = versionGenerator.convertVersionToRead(gmuVersion);

      VersionEntry firstFoundPossible = null; //These are used to optimize the search
      long concurrentClockNumber = 0L;

      /*
      *
      * If this is the node N, the concurrentClockNumber of a VersionEntry A is the value
      * of the last committed vector clock's N-th entry at the time A was "prepared" on this node.
       */


      VersionEntry iterator = currentVersion;

      while (iterator != null &&
              (firstFoundPossible == null ||
                      concurrentClockNumber < iterator.getVersion().getThisNodeVersionValue())) {
         if (stat)
            count++;
         if (iterator.getVersion().getThisNodeVersionValue() <= gmuReadVersion.getThisNodeVersionValue()) {
            if (!isLessOrEquals(iterator.getVersion(), gmuVersion)) {
               if (log.isTraceEnabled()) {
                  log.tracef("getReadVersion(...) ==> comparing %s and %s ==> NOT VISIBLE", iterator.getVersion(), gmuReadVersion);
               }
               gmuReadVersion.addNotVisibleSubversion(iterator.getVersion().getThisNodeVersionValue(), iterator.getSubVersion());
            } else {
               if (log.isTraceEnabled()) {
                  log.tracef("getReadVersion(...) ==> comparing %s and %s ==> VISIBLE", iterator.getVersion(), gmuReadVersion);
               }
               if (firstFoundPossible == null) {
                  firstFoundPossible = iterator;
                  concurrentClockNumber = iterator.getConcurrentClockNumber();
               }
            }
         } else {
            if (log.isTraceEnabled()) {
               log.tracef("getReadVersion(...) ==> comparing %s and %s ==> IGNORE", iterator.getVersion(), gmuReadVersion);
            }
         }

         if (firstFoundPossible != null && concurrentClockNumber > iterator.getConcurrentClockNumber()) {
            //We move the bound before.
            concurrentClockNumber = iterator.getConcurrentClockNumber();

         }
         iterator = iterator.getPrevious();
      }
      if (stat) {
         ts.addValue(ExposedStatistic.COMMIT_LOG_GET_READ_VERSION, System.nanoTime() - all_init);
         ts.incrementValue(ExposedStatistic.NUM_COMMIT_LOG_GET_READ_VERSION);
         ts.addValue(ExposedStatistic.COMMIT_LOG_GET_READ_VERSION_COUNT, count);
      }
      return gmuReadVersion;
   }

   public final synchronized void insertNewCommittedVersions(Collection<CommittedTransaction> transactions) {
      assertEnabled();
      VersionEntry oldCurrentVersion = currentVersion;
      GMUVersion oldMostRecentVersion = mostRecentVersion;
      for (CommittedTransaction transaction : transactions) {
         if (log.isTraceEnabled()) {
            log.tracef("insertNewCommittedVersions(...) ==> add %s", transaction.getCommitVersion());
         }
         VersionEntry current = new VersionEntry((GMUVersion) transaction.getCommitVersion(),
                 getAffectedKeys(transaction.getModifications()),
                 transaction.getSubVersion(), transaction.getConcurrentClockNumber());
         current.setPrevious(oldCurrentVersion);
         oldCurrentVersion = current;
         oldMostRecentVersion = versionGenerator.mergeAndMax(oldMostRecentVersion, oldCurrentVersion.getVersion());
      }
      currentVersion = oldCurrentVersion;
      mostRecentVersion = oldMostRecentVersion;
      if (log.isTraceEnabled()) {
         log.tracef("insertNewCommittedVersions(...) ==> %s", oldCurrentVersion.getVersion());
      }
      notifyAll();
   }

   public final void updateMostRecentVersion(EntryVersion newVersion) {
      /*
      assertEnabled();
      GMUVersion gmuEntryVersion = toGMUVersion(newVersion);
      if (gmuEntryVersion.getThisNodeVersionValue() > mostRecentVersion.getThisNodeVersionValue()) {
         log.warn("Cannot update the most recent version to a version higher than " +
                                                  "the current version");
         return;
      }
      mostRecentVersion = versionGenerator.mergeAndMax(mostRecentVersion, gmuEntryVersion);
      */
   }

   public final void waitForVersion(EntryVersion version, long timeout) throws InterruptedException {
      assertEnabled();
      final long versionValue = ((GMUVersion) version).getThisNodeVersionValue();
      if (currentVersion.getVersion().getThisNodeVersionValue() >= versionValue) {
         return;
      }
      if (log.isTraceEnabled()) {
         log.tracef("waitForVersion(%s,%s) and current version is %s", version, timeout, currentVersion.getVersion());
      }
      synchronized (this) {
         while (currentVersion.getVersion().getThisNodeVersionValue() < versionValue) {
            wait();
         }
      }
      if (log.isTraceEnabled()) {
         log.tracef("waitForVersion(%s) ==> %s TRUE ?", version,
                 currentVersion.getVersion().getThisNodeVersionValue());
      }
   }

   public final boolean isMinVersionAvailable(EntryVersion version) {
      assertEnabled();

      long versionValue = ((GMUVersion) version).getThisNodeVersionValue();
      if (log.isTraceEnabled()) {
         log.tracef("isMinVersionAvailable(%s) and current version is %s", version, currentVersion.getVersion());
      }

      return currentVersion.getVersion().getThisNodeVersionValue() >= versionValue;
   }

   public final boolean dumpTo(String filePath) {
      assertEnabled();
      BufferedWriter bufferedWriter = Util.getBufferedWriter(filePath);
      if (bufferedWriter == null) {
         return false;
      }
      try {
         VersionEntry iterator = currentVersion;
         bufferedWriter.newLine();
         while (iterator != null) {
            iterator.dumpTo(bufferedWriter);
            iterator = iterator.getPrevious();
         }
         return true;
      } catch (IOException e) {
         return false;
      } finally {
         Util.close(bufferedWriter);
      }
   }

   /**
    * removes the older version than {@param minVersion} and returns the minimum usable version to remove the old values
    * in data container
    *
    * @param minVersion the minimum visible version
    * @return the minimum usable version (to remove entries in data container)
    */
   public final GMUVersion gcOlderVersions(GMUVersion minVersion) {
      VersionEntry iterator = currentVersion;
      VersionEntry removeFromHere = null;
      GMUVersion minimumVisibleVersion = null;

      while (iterator != null) {
         if (isLessOrEquals(iterator.getVersion(), minVersion)) {
            if (minimumVisibleVersion == null) {
               minimumVisibleVersion = iterator.getVersion();
               removeFromHere = iterator;
            }
         } else {
            minimumVisibleVersion = null;
            removeFromHere = null;
         }
         iterator = iterator.getPrevious();
      }

      while (removeFromHere != null) {
         VersionEntry previous = removeFromHere.getPrevious();
         removeFromHere.setPrevious(null);
         removeFromHere = previous;
      }

      if (log.isTraceEnabled()) {
         log.tracef("gcOlderVersion(%s) ==> %s", minVersion, minimumVisibleVersion);
      }

      return minimumVisibleVersion;
   }

   public final int calculateMinimumViewId() {
      VersionEntry iterator = currentVersion;
      int minimumViewId = iterator.getVersion().getViewId();
      iterator = iterator.getPrevious();
      while (iterator != null) {
         minimumViewId = Math.min(minimumViewId, iterator.getVersion().getViewId());
      }
      return minimumViewId;
   }

   private Set<Object> getAffectedKeys(Collection<WriteCommand> modifications) {
      Set<Object> keys = new HashSet<Object>();
      for (WriteCommand writeCommand : modifications) {
         if (writeCommand instanceof ClearCommand) {
            return null;
         }
         keys.addAll(writeCommand.getAffectedKeys());
      }
      return keys;
   }

   private void assertEnabled() {
      if (!enabled) {
         throw new IllegalStateException("Commit Log not enabled!");
      }
   }

   private boolean isLessOrEquals(EntryVersion version1, EntryVersion version2) {
      InequalVersionComparisonResult comparisonResult = version1.compareToWithCheckUnsafeBeforeOrEqual(version2);
      if (comparisonResult == InequalVersionComparisonResult.UNSAFE_BEFORE_OR_EQUAL) {
         throw new IllegalArgumentException("GMU entry version cannot compare BeforeOrEqual" + version1 + " " + version2);
      }
      return comparisonResult == BEFORE_OR_EQUAL || comparisonResult == BEFORE || comparisonResult == EQUAL;
   }

   private static class VersionEntry {
      private final GMUVersion version;
      private final Object[] keysModified;
      private final int subVersion;
      private final long concurrentClockNumber;
      private VersionEntry previous;

      private VersionEntry(GMUVersion version, Set<Object> keysModified, int subVersion, long concurrentClockNumber) {
         this.version = version;
         if (keysModified == null) {
            this.keysModified = null;
         } else {
            this.keysModified = keysModified.toArray(new Object[keysModified.size()]);
         }
         this.subVersion = subVersion;
         this.concurrentClockNumber = concurrentClockNumber;
      }

      public GMUVersion getVersion() {
         return version;
      }

      public VersionEntry getPrevious() {
         return previous;
      }

      public void setPrevious(VersionEntry previous) {
         this.previous = previous;
      }

      public int getSubVersion() {
         return subVersion;
      }

      public long getConcurrentClockNumber() {
         return concurrentClockNumber;
      }

      @Override
      public String toString() {
         return "VersionEntry{" +
                 "version=" + version +
                 ", subVersion=" + subVersion +
                 ", concurrentClockNumber=" + concurrentClockNumber +
                 ", keysModified=" + (keysModified == null ? "ALL" : Arrays.asList(keysModified)) +
                 '}';
      }

      public final void dumpTo(BufferedWriter writer) throws IOException {
         writer.write(version.toString());
         writer.write("=");
         writer.write((keysModified == null ? "ALL" : Arrays.asList(keysModified).toString()));
         writer.newLine();
         writer.flush();
      }
   }
}
