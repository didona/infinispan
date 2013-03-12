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

package org.infinispan.stats;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.InterceptorConfiguration;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.stats.wrappers.ExtendedStatisticInterceptor;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.transaction.TransactionProtocol;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertNull;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.3
 */
@Test(groups = "functional", testName = "stats.BaseClusteredExtendedStatisticTest")
public abstract class BaseClusteredExtendedStatisticTest extends MultipleCacheManagersTest {

   protected static final String KEY_1 = "key_1";
   protected static final String KEY_2 = "key_2";
   protected static final String KEY_3 = "key_3";
   protected static final String VALUE_1 = "value_1";
   protected static final String VALUE_2 = "value_2";
   protected static final String VALUE_3 = "value_3";
   protected static final String VALUE_4 = "value_4";
   protected final CacheMode mode;
   protected final boolean sync2ndPhase;
   protected final boolean writeSkew;
   protected final boolean totalOrder;

   protected BaseClusteredExtendedStatisticTest(CacheMode mode, boolean sync2ndPhase, boolean writeSkew,
                                                boolean totalOrder) {
      this.mode = mode;
      this.sync2ndPhase = sync2ndPhase;
      this.writeSkew = writeSkew;
      this.totalOrder = totalOrder;
   }

   public void testPut() {
      assertEmpty(KEY_1, KEY_2, KEY_3);
      ExtendedStatisticInterceptor statisticInterceptor0 = getExtendedStatistic(cache(0));
      ExtendedStatisticInterceptor statisticInterceptor1 = getExtendedStatistic(cache(1));

      cache(0).put(KEY_1, VALUE_1);

      assertCacheValue(KEY_1, VALUE_1);

      Map<Object, Object> map = new HashMap<Object, Object>();
      map.put(KEY_2, VALUE_2);
      map.put(KEY_3, VALUE_3);

      cache(0).putAll(map);

      assertCacheValue(KEY_1, VALUE_1);
      assertCacheValue(KEY_2, VALUE_2);
      assertCacheValue(KEY_3, VALUE_3);

      assertNoTransactions();
      Assert.assertFalse(statisticInterceptor0.getCacheStatisticManager().hasPendingTransactions());
      Assert.assertFalse(statisticInterceptor1.getCacheStatisticManager().hasPendingTransactions());
   }

   public void removeTest() {
      assertEmpty(KEY_1);
      ExtendedStatisticInterceptor statisticInterceptor0 = getExtendedStatistic(cache(0));
      ExtendedStatisticInterceptor statisticInterceptor1 = getExtendedStatistic(cache(1));

      cache(1).put(KEY_1, VALUE_1);

      assertCacheValue(KEY_1, VALUE_1);

      cache(0).remove(KEY_1);

      assertCacheValue(KEY_1, null);

      cache(0).put(KEY_1, VALUE_1);

      assertCacheValue(KEY_1, VALUE_1);

      cache(0).remove(KEY_1);

      assertCacheValue(KEY_1, null);

      assertNoTransactions();
      Assert.assertFalse(statisticInterceptor0.getCacheStatisticManager().hasPendingTransactions());
      Assert.assertFalse(statisticInterceptor1.getCacheStatisticManager().hasPendingTransactions());
   }

   public void testPutIfAbsent() {
      assertEmpty(KEY_1, KEY_2);
      ExtendedStatisticInterceptor statisticInterceptor0 = getExtendedStatistic(cache(0));
      ExtendedStatisticInterceptor statisticInterceptor1 = getExtendedStatistic(cache(1));

      cache(1).put(KEY_1, VALUE_1);

      assertCacheValue(KEY_1, VALUE_1);

      cache(0).putIfAbsent(KEY_1, VALUE_2);

      assertCacheValue(KEY_1, VALUE_1);

      cache(1).put(KEY_1, VALUE_3);

      assertCacheValue(KEY_1, VALUE_3);

      cache(0).putIfAbsent(KEY_1, VALUE_4);

      assertCacheValue(KEY_1, VALUE_3);

      cache(0).putIfAbsent(KEY_2, VALUE_1);

      assertCacheValue(KEY_2, VALUE_1);

      assertNoTransactions();
      Assert.assertFalse(statisticInterceptor0.getCacheStatisticManager().hasPendingTransactions());
      Assert.assertFalse(statisticInterceptor1.getCacheStatisticManager().hasPendingTransactions());
   }

   public void testRemoveIfPresent() {
      assertEmpty(KEY_1);
      ExtendedStatisticInterceptor statisticInterceptor0 = getExtendedStatistic(cache(0));
      ExtendedStatisticInterceptor statisticInterceptor1 = getExtendedStatistic(cache(1));

      cache(0).put(KEY_1, VALUE_1);

      assertCacheValue(KEY_1, VALUE_1);

      cache(1).put(KEY_1, VALUE_2);

      assertCacheValue(KEY_1, VALUE_2);

      cache(0).remove(KEY_1, VALUE_1);

      assertCacheValue(KEY_1, VALUE_2);

      cache(0).remove(KEY_1, VALUE_2);

      assertCacheValue(KEY_1, null);

      assertNoTransactions();
      Assert.assertFalse(statisticInterceptor0.getCacheStatisticManager().hasPendingTransactions());
      Assert.assertFalse(statisticInterceptor1.getCacheStatisticManager().hasPendingTransactions());
   }

   public void testClear() {
      assertEmpty(KEY_1);
      ExtendedStatisticInterceptor statisticInterceptor0 = getExtendedStatistic(cache(0));
      ExtendedStatisticInterceptor statisticInterceptor1 = getExtendedStatistic(cache(1));

      cache(0).put(KEY_1, VALUE_1);

      assertCacheValue(KEY_1, VALUE_1);

      cache(0).clear();

      assertCacheValue(KEY_1, null);

      assertNoTransactions();
      Assert.assertFalse(statisticInterceptor0.getCacheStatisticManager().hasPendingTransactions());
      Assert.assertFalse(statisticInterceptor1.getCacheStatisticManager().hasPendingTransactions());
   }

   public void testReplace() {
      assertEmpty(KEY_1);
      ExtendedStatisticInterceptor statisticInterceptor0 = getExtendedStatistic(cache(0));
      ExtendedStatisticInterceptor statisticInterceptor1 = getExtendedStatistic(cache(1));

      cache(1).put(KEY_1, VALUE_1);

      assertCacheValue(KEY_1, VALUE_1);

      Assert.assertEquals(cache(0).replace(KEY_1, VALUE_2), VALUE_1);

      assertCacheValue(KEY_1, VALUE_2);

      cache(0).put(KEY_1, VALUE_3);

      assertCacheValue(KEY_1, VALUE_3);

      cache(0).replace(KEY_1, VALUE_3);

      assertCacheValue(KEY_1, VALUE_3);

      cache(0).put(KEY_1, VALUE_4);

      assertCacheValue(KEY_1, VALUE_4);

      assertNoTransactions();
      Assert.assertFalse(statisticInterceptor0.getCacheStatisticManager().hasPendingTransactions());
      Assert.assertFalse(statisticInterceptor1.getCacheStatisticManager().hasPendingTransactions());
   }

   public void testReplaceWithOldVal() {
      assertEmpty(KEY_1);
      ExtendedStatisticInterceptor statisticInterceptor0 = getExtendedStatistic(cache(0));
      ExtendedStatisticInterceptor statisticInterceptor1 = getExtendedStatistic(cache(1));

      cache(1).put(KEY_1, VALUE_1);

      assertCacheValue(KEY_1, VALUE_1);

      cache(0).put(KEY_1, VALUE_2);

      assertCacheValue(KEY_1, VALUE_2);

      cache(0).replace(KEY_1, VALUE_3, VALUE_4);

      assertCacheValue(KEY_1, VALUE_2);

      cache(0).replace(KEY_1, VALUE_2, VALUE_4);

      assertCacheValue(KEY_1, VALUE_4);

      assertNoTransactions();
      Assert.assertFalse(statisticInterceptor0.getCacheStatisticManager().hasPendingTransactions());
      Assert.assertFalse(statisticInterceptor1.getCacheStatisticManager().hasPendingTransactions());
   }

   public void testRemoveUnexistingEntry() {
      assertEmpty(KEY_1);
      ExtendedStatisticInterceptor statisticInterceptor0 = getExtendedStatistic(cache(0));
      ExtendedStatisticInterceptor statisticInterceptor1 = getExtendedStatistic(cache(1));

      cache(0).remove(KEY_1);

      assertCacheValue(KEY_1, null);

      assertNoTransactions();
      Assert.assertFalse(statisticInterceptor0.getCacheStatisticManager().hasPendingTransactions());
      Assert.assertFalse(statisticInterceptor1.getCacheStatisticManager().hasPendingTransactions());
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      for (int i = 0; i < 2; ++i) {
         ConfigurationBuilder builder = getDefaultClusteredCacheConfig(mode, true);
         builder.transaction().syncCommitPhase(sync2ndPhase).syncRollbackPhase(sync2ndPhase);
         if (totalOrder) {
            builder.transaction().transactionProtocol(TransactionProtocol.TOTAL_ORDER);
         }
         builder.locking().isolationLevel(IsolationLevel.REPEATABLE_READ).writeSkewCheck(writeSkew);
         builder.clustering().hash().numOwners(1);
         if (writeSkew) {
            builder.versioning().enable().scheme(VersioningScheme.SIMPLE);
         }
         builder.transaction().recovery().disable();
         builder.customInterceptors().addInterceptor().interceptor(new ExtendedStatisticInterceptor())
               .position(InterceptorConfiguration.Position.FIRST);
         addClusterEnabledCacheManager(builder);
      }
      waitForClusterToForm();
   }

   protected void assertEmpty(Object... keys) {
      for (Cache cache : caches()) {
         for (Object key : keys) {
            assertNull(cache.get(key));
         }
      }
   }

   protected void assertCacheValue(Object key, Object value) {
      for (int index = 0; index < caches().size(); ++index) {
         if (mode.isSynchronous() && sync2ndPhase) {
            assertEquals(index, key, value);
         } else {
            assertEventuallyEquals(index, key, value);
         }
      }

   }

   private void assertEquals(int index, Object key, Object value) {
      Assert.assertEquals(cache(index).get(key), value);
   }

   private ExtendedStatisticInterceptor getExtendedStatistic(Cache<?, ?> cache) {
      for (CommandInterceptor commandInterceptor : cache.getAdvancedCache().getInterceptorChain()) {
         if (commandInterceptor instanceof ExtendedStatisticInterceptor) {
            ((ExtendedStatisticInterceptor) commandInterceptor).resetStatistics();
            return (ExtendedStatisticInterceptor) commandInterceptor;
         }
      }
      return null;
   }
}
