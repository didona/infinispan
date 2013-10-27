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
package org.infinispan.container.gmu;

import org.infinispan.container.EntryFactoryImpl;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.container.entries.SerializableEntry;
import org.infinispan.container.entries.gmu.InternalGMUCacheEntry;
import org.infinispan.container.entries.gmu.InternalGMUNullCacheEntry;
import org.infinispan.container.entries.gmu.InternalGMUValueCacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.container.versioning.gmu.GMUVersionGenerator;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.SingleKeyNonTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.stats.ExposedStatistic;
import org.infinispan.stats.TransactionsStatisticsRegistry;
import org.infinispan.stats.container.TransactionStatistics;
import org.infinispan.transaction.gmu.CommitLog;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * @author Pedro Ruivo
 * @author Sebastiano Peluso
 * @since 5.2
 */
public class GMUEntryFactoryImpl extends EntryFactoryImpl {

   private static final Log log = LogFactory.getLog(GMUEntryFactoryImpl.class);
   private CommitLog commitLog;
   private GMUVersionGenerator gmuVersionGenerator;
   private ClusteringDependentLogic clusteringDependentLogic;

   public static InternalGMUCacheEntry wrap(Object key, InternalCacheEntry entry, boolean mostRecent,
                                            EntryVersion maxTxVersion, EntryVersion creationVersion,
                                            EntryVersion maxValidVersion, boolean unsafeToRead) {
      if (entry == null || entry.isNull()) {
         return new InternalGMUNullCacheEntry(key, (entry == null ? null : entry.getVersion()), maxTxVersion, mostRecent,
                                              creationVersion, maxValidVersion, unsafeToRead);
      }
      return new InternalGMUValueCacheEntry(entry, maxTxVersion, mostRecent, creationVersion, maxValidVersion, unsafeToRead);
   }

   @Inject
   public void injectDependencies(CommitLog commitLog, VersionGenerator versionGenerator,
                                  ClusteringDependentLogic clusteringDependentLogic) {
      this.commitLog = commitLog;
      this.gmuVersionGenerator = (GMUVersionGenerator) versionGenerator;
      this.clusteringDependentLogic = clusteringDependentLogic;
   }

   public void start() {
      useRepeatableRead = false;
      localModeWriteSkewCheck = false;
   }

   @Override
   protected MVCCEntry createWrappedEntry(Object key, Object value, EntryVersion version, boolean isForInsert, boolean forRemoval, long lifespan) {
      return new SerializableEntry(key, value, lifespan, version);
   }

   @Override
   protected InternalCacheEntry getFromContainer(Object key, InvocationContext context) {
      final TransactionStatistics ts = TransactionsStatisticsRegistry.getTransactionStatistics();
      final boolean stats = ts != null;
      long init = 0, end = 0, allInit = 0;
      if (stats)
         allInit = System.nanoTime();
      if (context.isOriginLocal() && !clusteringDependentLogic.localNodeIsOwner(key)) {
         //force a remote get...
         return null;
      }

      boolean singleRead = context instanceof SingleKeyNonTxInvocationContext;
      boolean remotePrepare = !context.isOriginLocal() && context.isInTxScope();
      boolean remoteRead = !context.isOriginLocal() && !context.isInTxScope();

      EntryVersion versionToRead;
      if (singleRead || remotePrepare) {
         //read the most recent version
         //in the prepare, the value does not matter (it will be written or it is not read)
         //                and the version does not matter either (it will be overwritten)
         versionToRead = null;
      } else {
         if (stats) {
            init = System.nanoTime();
         }
         versionToRead = context.calculateVersionToRead(gmuVersionGenerator);
         if (stats) {
            end = System.nanoTime();
            ts.addValue(ExposedStatistic.CALCULATE_VERSION, end - init);
            ts.incrementValue(ExposedStatistic.NUM_CALCULATE_VERSION);
         }
      }

      boolean hasAlreadyReadFromThisNode = context.hasAlreadyReadOnThisNode();

      if (context.isInTxScope() && context.isOriginLocal() && !context.hasAlreadyReadOnThisNode()) {
         //firs read on the local node for a transaction. ensure the min version
         EntryVersion transactionVersion = ((TxInvocationContext) context).getTransactionVersion();
         try {
            init = end;
            commitLog.waitForVersion(transactionVersion, -1);
            if (stats) {
               end = System.nanoTime();
               ts.addValue(ExposedStatistic.WAIT_FROM_CONTAINER, end - init);
               ts.incrementValue(ExposedStatistic.NUM_WAIT_FROM_CONTAINER);
            }

         } catch (InterruptedException e) {
            //ignore...
         }
      }
      init = end;

      EntryVersion maxVersionToRead;
      if (hasAlreadyReadFromThisNode)
         maxVersionToRead = versionToRead;
      else {
         maxVersionToRead = commitLog.getAvailableVersionLessThan(versionToRead);
         if (stats) {
            ts.addValue(ExposedStatistic.CONTAINER_AVAILABLE_VERSION, System.nanoTime() - init);
            ts.incrementValue(ExposedStatistic.NUM_CONTAINER_AVAILABLE_VERSION);
         }
      }

      final EntryVersion mostRecentCommitLogVersion = commitLog.getCurrentVersion();
      final InternalGMUCacheEntry entry = (InternalGMUCacheEntry) container.get(key, maxVersionToRead);

      if (remoteRead) {
         if (entry.getMaximumValidVersion() == null) {
            entry.setMaximumValidVersion(mostRecentCommitLogVersion);
         } else {
            entry.setMaximumValidVersion(commitLog.getEntry(entry.getMaximumValidVersion()));
         }
         if (entry.getCreationVersion() == null) {
            entry.setCreationVersion(commitLog.getOldestVersion());
         } else {
            entry.setCreationVersion(commitLog.getEntry(entry.getCreationVersion()));
         }
      }

      context.addKeyReadInCommand(key, entry);

      if (log.isTraceEnabled()) {
         log.tracef("Retrieved from container %s", entry);
      }
      if (stats) {
         ts.addValue(ExposedStatistic.GET_FROM_CONTAINER, System.nanoTime() - allInit);
         ts.incrementValue(ExposedStatistic.NUM_GET_FROM_CONTAINER);
      }
      return entry.getInternalCacheEntry();
   }
}
