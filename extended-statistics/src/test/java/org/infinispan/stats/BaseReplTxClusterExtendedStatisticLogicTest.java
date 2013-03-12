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
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.TxInterceptor;
import org.infinispan.interceptors.base.BaseCustomInterceptor;
import org.infinispan.stats.wrappers.ExtendedStatisticInterceptor;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.transaction.TransactionProtocol;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

import static org.infinispan.stats.CacheStatisticCollector.convertNanosToMicro;
import static org.infinispan.stats.CacheStatisticCollector.convertNanosToSeconds;
import static org.testng.Assert.*;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.3
 */
@Test(groups = "functional", testName = "stats.BaseReplTxClusterExtendedStatisticLogicTest")
public abstract class BaseReplTxClusterExtendedStatisticLogicTest extends MultipleCacheManagersTest {

   protected static final String KEY_1 = "key_1";
   protected static final String KEY_2 = "key_2";
   protected static final String KEY_3 = "key_3";
   protected static final String VALUE_1 = "value_1";
   protected static final String VALUE_2 = "value_2";
   protected static final String VALUE_3 = "value_3";
   protected static final String VALUE_4 = "value_4";
   private static final int NUM_NODES = 2;
   private static final TimeService TEST_TIME_SERVICE = new TimeService() {
      @Override
      public long now() {
         return 0;
      }

      @Override
      public long duration(long start) {
         assertEquals(start, 0, "Start timestamp must be zero!");
         return 1;
      }

      @Override
      public long duration(long start, long end) {
         assertEquals(start, 0, "Start timestamp must be zero!");
         assertEquals(end, 0, "End timestamp must be zero!");
         return 1;
      }
   };
   private static final double MICROSECONDS = convertNanosToMicro(TEST_TIME_SERVICE.duration(0));
   private static final double SECONDS = convertNanosToSeconds(TEST_TIME_SERVICE.duration(0));
   protected final boolean sync;
   protected final boolean sync2ndPhase;
   protected final boolean writeSkew;
   protected final boolean totalOrder;
   private final ExtendedStatisticInterceptor[] extendedStatisticInterceptors = new ExtendedStatisticInterceptor[NUM_NODES];
   private final TransactionInterceptor[] transactionInterceptors = new TransactionInterceptor[NUM_NODES];

   protected BaseReplTxClusterExtendedStatisticLogicTest(boolean sync, boolean sync2ndPhase, boolean writeSkew,
                                                         boolean totalOrder) {
      this.sync = sync;
      this.sync2ndPhase = sync2ndPhase;
      this.writeSkew = writeSkew;
      this.totalOrder = totalOrder;
   }

   public final void testSimplePut() throws InterruptedException {
      assertEmpty(KEY_1);
      resetStats();
      resetCounter();

      cache(0).put(KEY_1, VALUE_1);

      //ensure that the transaction is committed everywhere.
      /*if (!sync) {
         transactionInterceptors[0].awaitForPrepares(totalOrder ? 2 : 1);
         transactionInterceptors[1].awaitForPrepares(1);
      } else if (!sync2ndPhase) {
         transactionInterceptors[0].awaitForPrepares(totalOrder ? 2 : 1);
         transactionInterceptors[0].awaitForCommits(1);
         transactionInterceptors[1].awaitForPrepares(1);
         transactionInterceptors[1].awaitForCommits(1);
      }*/
      Thread.sleep(2000);

      assertAttributeValue(ExtendedStatistic.WR_TX_LOCAL_EXECUTION_TIME, MICROSECONDS, 0);
      assertAttributeValue(ExtendedStatistic.NUM_COMMITTED_RO_TX, 0, 0); //not exposed via JMX
      assertAttributeValue(ExtendedStatistic.NUM_COMMITTED_WR_TX, 1, 1); //not exposed via JMX
      assertAttributeValue(ExtendedStatistic.NUM_ABORTED_WR_TX, 0, 0); //not exposed via JMX
      assertAttributeValue(ExtendedStatistic.NUM_ABORTED_RO_TX, 0, 0); //not exposed via JMX
      assertAttributeValue(ExtendedStatistic.NUM_COMMITS, 1, 1);
      assertAttributeValue(ExtendedStatistic.NUM_LOCAL_COMMITS, 1, 0);
      assertAttributeValue(ExtendedStatistic.NUM_PREPARES, 1, 0);
      assertAttributeValue(ExtendedStatistic.LOCAL_EXEC_NO_CONT, MICROSECONDS, 0);
      assertAttributeValue(ExtendedStatistic.LOCAL_CONTENTION_PROBABILITY, 0, 0);
      assertAttributeValue(ExtendedStatistic.REMOTE_CONTENTION_PROBABILITY, 0, 0);
      assertAttributeValue(ExtendedStatistic.LOCK_CONTENTION_PROBABILITY, 0, 0);
      assertAttributeValue(ExtendedStatistic.LOCK_HOLD_TIME_LOCAL, totalOrder ? 0 : MICROSECONDS, 0);
      assertAttributeValue(ExtendedStatistic.LOCK_HOLD_TIME_REMOTE, 0, 0);
      assertAttributeValue(ExtendedStatistic.LOCK_CONTENTION_TO_LOCAL, 0, 0);
      assertAttributeValue(ExtendedStatistic.LOCK_CONTENTION_TO_REMOTE, 0, 0);
      assertAttributeValue(ExtendedStatistic.NUM_SUCCESSFUL_PUTS, 1, 0);
      assertAttributeValue(ExtendedStatistic.PUTS_PER_LOCAL_TX, 1, 0);
      assertAttributeValue(ExtendedStatistic.NUM_WAITED_FOR_LOCKS, 0, 0);
      assertAttributeValue(ExtendedStatistic.NUM_REMOTE_GET, 0, 0);
      assertAttributeValue(ExtendedStatistic.NUM_GET, 0, 0);
      assertAttributeValue(ExtendedStatistic.NUM_SUCCESSFUL_GETS_RO_TX, 0, 0);
      assertAttributeValue(ExtendedStatistic.NUM_SUCCESSFUL_GETS_WR_TX, 0, 0);
      assertAttributeValue(ExtendedStatistic.NUM_SUCCESSFUL_REMOTE_GETS_WR_TX, 0, 0);
      assertAttributeValue(ExtendedStatistic.NUM_SUCCESSFUL_REMOTE_GETS_RO_TX, 0, 0);
      assertAttributeValue(ExtendedStatistic.LOCAL_GET_EXECUTION, 0, 0);
      assertAttributeValue(ExtendedStatistic.ALL_GET_EXECUTION, 0, 0);
      assertAttributeValue(ExtendedStatistic.REMOTE_GET_EXECUTION, 0, 0);
      assertAttributeValue(ExtendedStatistic.REMOTE_PUT_EXECUTION, 0, 0);
      assertAttributeValue(ExtendedStatistic.NUM_REMOTE_PUT, 0, 0);
      assertAttributeValue(ExtendedStatistic.NUM_PUT, 1, 0);
      assertAttributeValue(ExtendedStatistic.NUM_SUCCESSFUL_PUTS_WR_TX, 1, 0);
      assertAttributeValue(ExtendedStatistic.NUM_SUCCESSFUL_REMOTE_PUTS_WR_TX, 0, 0);
      assertAttributeValue(ExtendedStatistic.TX_WRITE_PERCENTAGE, 1, 0);
      assertAttributeValue(ExtendedStatistic.SUCCESSFUL_WRITE_PERCENTAGE, 1, 0);
      assertAttributeValue(ExtendedStatistic.WR_TX_ABORTED_EXECUTION_TIME, 0, 0);
      assertAttributeValue(ExtendedStatistic.WR_TX_SUCCESSFUL_EXECUTION_TIME, MICROSECONDS, 0);
      assertAttributeValue(ExtendedStatistic.RO_TX_SUCCESSFUL_EXECUTION_TIME, 0, 0);
      assertAttributeValue(ExtendedStatistic.RO_TX_ABORTED_EXECUTION_TIME, 0, 0);
      assertAttributeValue(ExtendedStatistic.APPLICATION_CONTENTION_FACTOR, 0, 0);
      assertAttributeValue(ExtendedStatistic.NUM_WRITE_SKEW, 0, 0);
      assertAttributeValue(ExtendedStatistic.WRITE_SKEW_PROBABILITY, 0, 0);
      assertAttributeValue(ExtendedStatistic.ABORT_RATE, 0, 0);
      assertAttributeValue(ExtendedStatistic.ARRIVAL_RATE, 1 / SECONDS, 1 / SECONDS);
      assertAttributeValue(ExtendedStatistic.THROUGHPUT, 1 / SECONDS, 0);
      //assertAttributeValue(ExtendedStatistic.RO_LOCAL_PERCENTILE);  //TODO ONLY FOR QUERY, derived on the fly
      //assertAttributeValue(ExtendedStatistic.WR_LOCAL_PERCENTILE);  //TODO ONLY FOR QUERY, derived on the fly
      //assertAttributeValue(ExtendedStatistic.RO_REMOTE_PERCENTILE); //TODO ONLY FOR QUERY, derived on the fly
      //assertAttributeValue(ExtendedStatistic.WR_REMOTE_PERCENTILE); //TODO ONLY FOR QUERY, derived on the fly
      assertAttributeValue(ExtendedStatistic.ROLLBACK_EXECUTION_TIME, 0, 0);
      assertAttributeValue(ExtendedStatistic.NUM_ROLLBACKS, 0, 0);
      assertAttributeValue(ExtendedStatistic.LOCAL_ROLLBACK_EXECUTION_TIME, 0, 0);
      assertAttributeValue(ExtendedStatistic.REMOTE_ROLLBACK_EXECUTION_TIME, 0, 0);
      assertAttributeValue(ExtendedStatistic.COMMIT_EXECUTION_TIME, sync ? MICROSECONDS : 0, 0);
      assertAttributeValue(ExtendedStatistic.NUM_COMMIT_COMMAND, sync ? 1 : 0, sync ? 1 : 0);
      assertAttributeValue(ExtendedStatistic.LOCAL_COMMIT_EXECUTION_TIME, sync ? MICROSECONDS : 0, 0);
      assertAttributeValue(ExtendedStatistic.REMOTE_COMMIT_EXECUTION_TIME, 0, sync ? MICROSECONDS : 0);
      assertAttributeValue(ExtendedStatistic.PREPARE_EXECUTION_TIME, 1, 1); // //not exposed via JMX
      assertAttributeValue(ExtendedStatistic.NUM_PREPARE_COMMAND, 1, 1);
      assertAttributeValue(ExtendedStatistic.LOCAL_PREPARE_EXECUTION_TIME, MICROSECONDS, 0);
      assertAttributeValue(ExtendedStatistic.REMOTE_PREPARE_EXECUTION_TIME, 0, MICROSECONDS);
      assertAttributeValue(ExtendedStatistic.TX_COMPLETE_NOTIFY_EXECUTION_TIME, 0, 0);
      assertAttributeValue(ExtendedStatistic.NUM_TX_COMPLETE_NOTIFY_COMMAND, 0, 0);
      assertAttributeValue(ExtendedStatistic.NUM_LOCK_PER_LOCAL_TX, 1, 0);
      assertAttributeValue(ExtendedStatistic.NUM_LOCK_PER_REMOTE_TX, 0, 0);
      assertAttributeValue(ExtendedStatistic.NUM_LOCK_PER_SUCCESS_LOCAL_TX, 1, 0);
      assertAttributeValue(ExtendedStatistic.LOCK_WAITING_TIME, 0, 0);
      assertAttributeValue(ExtendedStatistic.LOCK_HOLD_TIME, MICROSECONDS, 0);
      assertAttributeValue(ExtendedStatistic.NUM_HELD_LOCKS, 1, 0);
      assertAttributeValue(ExtendedStatistic.NUM_HELD_LOCKS_SUCCESS_TX, 1, 0);
      //assertAttributeValue(ExtendedStatistic.PREPARE_COMMAND_SIZE);        // L
      //assertAttributeValue(ExtendedStatistic.COMMIT_COMMAND_SIZE);         // L
      //assertAttributeValue(ExtendedStatistic.CLUSTERED_GET_COMMAND_SIZE);  // L
      assertAttributeValue(ExtendedStatistic.NUM_LOCK_FAILED_TIMEOUT, 0, 0);
      assertAttributeValue(ExtendedStatistic.NUM_LOCK_FAILED_DEADLOCK, 0, 0);
      assertAttributeValue(ExtendedStatistic.NUM_RTTS_PREPARE, sync ? 1 : 0, 0);
      assertAttributeValue(ExtendedStatistic.RTT_PREPARE, sync ? 1 : 0, 0);
      assertAttributeValue(ExtendedStatistic.NUM_RTTS_COMMIT, sync2ndPhase ? 1 : 0, 0);
      assertAttributeValue(ExtendedStatistic.RTT_COMMIT, sync2ndPhase ? 1 : 0, 0);
      assertAttributeValue(ExtendedStatistic.NUM_RTTS_ROLLBACK, 0, 0);
      assertAttributeValue(ExtendedStatistic.RTT_ROLLBACK, 0, 0);
      assertAttributeValue(ExtendedStatistic.NUM_RTTS_GET, 0, 0);
      assertAttributeValue(ExtendedStatistic.RTT_GET, 0, 0);
      assertAttributeValue(ExtendedStatistic.ASYNC_PREPARE, sync ? 0 : MICROSECONDS, 0);
      assertAttributeValue(ExtendedStatistic.NUM_ASYNC_PREPARE, sync ? 0 : 1, 0);
      assertAttributeValue(ExtendedStatistic.ASYNC_COMMIT, !sync || sync2ndPhase ? 0 : MICROSECONDS, 0);
      assertAttributeValue(ExtendedStatistic.NUM_ASYNC_COMMIT, !sync || sync2ndPhase ? 0 : 1, 0);
      assertAttributeValue(ExtendedStatistic.ASYNC_ROLLBACK, 0, 0);
      assertAttributeValue(ExtendedStatistic.NUM_ASYNC_ROLLBACK, 0, 0);
      assertAttributeValue(ExtendedStatistic.ASYNC_COMPLETE_NOTIFY, 0, 0);
      assertAttributeValue(ExtendedStatistic.NUM_ASYNC_COMPLETE_NOTIFY, 0, 0);
      assertAttributeValue(ExtendedStatistic.NUM_NODES_PREPARE, 2, 0);
      assertAttributeValue(ExtendedStatistic.NUM_NODES_COMMIT, sync ? 2 : 0, 0);
      assertAttributeValue(ExtendedStatistic.NUM_NODES_ROLLBACK, 0, 0);
      assertAttributeValue(ExtendedStatistic.NUM_NODES_COMPLETE_NOTIFY, 0, 0);
      assertAttributeValue(ExtendedStatistic.NUM_NODES_GET, 0, 0);
      assertAttributeValue(ExtendedStatistic.RESPONSE_TIME, MICROSECONDS, 0);
      resetStats();
   }

   public final void testSimpleGet() throws Exception {
      assertEmpty(KEY_1);
      resetStats();
      resetCounter();

      tm(0).begin();
      cache(0).get(KEY_1);
      tm(0).commit();

      assertAttributeValue(ExtendedStatistic.WR_TX_LOCAL_EXECUTION_TIME, 0, 0);
      assertAttributeValue(ExtendedStatistic.NUM_COMMITTED_RO_TX, 1, 0); //not exposed via JMX
      assertAttributeValue(ExtendedStatistic.NUM_COMMITTED_WR_TX, 0, 0); //not exposed via JMX
      assertAttributeValue(ExtendedStatistic.NUM_ABORTED_WR_TX, 0, 0); //not exposed via JMX
      assertAttributeValue(ExtendedStatistic.NUM_ABORTED_RO_TX, 0, 0); //not exposed via JMX
      assertAttributeValue(ExtendedStatistic.NUM_COMMITS, 1, 0);
      assertAttributeValue(ExtendedStatistic.NUM_LOCAL_COMMITS, 1, 0);
      assertAttributeValue(ExtendedStatistic.NUM_PREPARES, 1, 0);
      assertAttributeValue(ExtendedStatistic.LOCAL_EXEC_NO_CONT, 0, 0);
      assertAttributeValue(ExtendedStatistic.LOCAL_CONTENTION_PROBABILITY, 0, 0);
      assertAttributeValue(ExtendedStatistic.REMOTE_CONTENTION_PROBABILITY, 0, 0);
      assertAttributeValue(ExtendedStatistic.LOCK_CONTENTION_PROBABILITY, 0, 0);
      assertAttributeValue(ExtendedStatistic.LOCK_HOLD_TIME_LOCAL, 0, 0);
      assertAttributeValue(ExtendedStatistic.LOCK_HOLD_TIME_REMOTE, 0, 0);
      assertAttributeValue(ExtendedStatistic.LOCK_CONTENTION_TO_LOCAL, 0, 0);
      assertAttributeValue(ExtendedStatistic.LOCK_CONTENTION_TO_REMOTE, 0, 0);
      assertAttributeValue(ExtendedStatistic.NUM_SUCCESSFUL_PUTS, 0, 0);
      assertAttributeValue(ExtendedStatistic.PUTS_PER_LOCAL_TX, 0, 0);
      assertAttributeValue(ExtendedStatistic.NUM_WAITED_FOR_LOCKS, 0, 0);
      assertAttributeValue(ExtendedStatistic.NUM_REMOTE_GET, 0, 0);
      assertAttributeValue(ExtendedStatistic.NUM_GET, 1, 0);
      assertAttributeValue(ExtendedStatistic.NUM_SUCCESSFUL_GETS_RO_TX, 1, 0);
      assertAttributeValue(ExtendedStatistic.NUM_SUCCESSFUL_GETS_WR_TX, 0, 0);
      assertAttributeValue(ExtendedStatistic.NUM_SUCCESSFUL_REMOTE_GETS_WR_TX, 0, 0);
      assertAttributeValue(ExtendedStatistic.NUM_SUCCESSFUL_REMOTE_GETS_RO_TX, 0, 0);
      assertAttributeValue(ExtendedStatistic.LOCAL_GET_EXECUTION, MICROSECONDS, 0);
      assertAttributeValue(ExtendedStatistic.ALL_GET_EXECUTION, 1, 0);
      assertAttributeValue(ExtendedStatistic.REMOTE_GET_EXECUTION, 0, 0);
      assertAttributeValue(ExtendedStatistic.REMOTE_PUT_EXECUTION, 0, 0);
      assertAttributeValue(ExtendedStatistic.NUM_REMOTE_PUT, 0, 0);
      assertAttributeValue(ExtendedStatistic.NUM_PUT, 0, 0);
      assertAttributeValue(ExtendedStatistic.NUM_SUCCESSFUL_PUTS_WR_TX, 0, 0);
      assertAttributeValue(ExtendedStatistic.NUM_SUCCESSFUL_REMOTE_PUTS_WR_TX, 0, 0);
      assertAttributeValue(ExtendedStatistic.TX_WRITE_PERCENTAGE, 0, 0);
      assertAttributeValue(ExtendedStatistic.SUCCESSFUL_WRITE_PERCENTAGE, 0, 0);
      assertAttributeValue(ExtendedStatistic.WR_TX_ABORTED_EXECUTION_TIME, 0, 0);
      assertAttributeValue(ExtendedStatistic.WR_TX_SUCCESSFUL_EXECUTION_TIME, 0, 0);
      assertAttributeValue(ExtendedStatistic.RO_TX_SUCCESSFUL_EXECUTION_TIME, MICROSECONDS, 0);
      assertAttributeValue(ExtendedStatistic.RO_TX_ABORTED_EXECUTION_TIME, 0, 0);
      assertAttributeValue(ExtendedStatistic.APPLICATION_CONTENTION_FACTOR, 0, 0);
      assertAttributeValue(ExtendedStatistic.NUM_WRITE_SKEW, 0, 0);
      assertAttributeValue(ExtendedStatistic.WRITE_SKEW_PROBABILITY, 0, 0);
      assertAttributeValue(ExtendedStatistic.ABORT_RATE, 0, 0);
      assertAttributeValue(ExtendedStatistic.ARRIVAL_RATE, 1 / SECONDS, 0);
      assertAttributeValue(ExtendedStatistic.THROUGHPUT, 1 / SECONDS, 0);
      //assertAttributeValue(ExtendedStatistic.RO_LOCAL_PERCENTILE);  //TODO ONLY FOR QUERY, derived on the fly
      //assertAttributeValue(ExtendedStatistic.WR_LOCAL_PERCENTILE);  //TODO ONLY FOR QUERY, derived on the fly
      //assertAttributeValue(ExtendedStatistic.RO_REMOTE_PERCENTILE); //TODO ONLY FOR QUERY, derived on the fly
      //assertAttributeValue(ExtendedStatistic.WR_REMOTE_PERCENTILE); //TODO ONLY FOR QUERY, derived on the fly
      assertAttributeValue(ExtendedStatistic.ROLLBACK_EXECUTION_TIME, 0, 0);
      assertAttributeValue(ExtendedStatistic.NUM_ROLLBACKS, 0, 0);
      assertAttributeValue(ExtendedStatistic.LOCAL_ROLLBACK_EXECUTION_TIME, 0, 0);
      assertAttributeValue(ExtendedStatistic.REMOTE_ROLLBACK_EXECUTION_TIME, 0, 0);
      assertAttributeValue(ExtendedStatistic.COMMIT_EXECUTION_TIME, 0, 0);
      assertAttributeValue(ExtendedStatistic.NUM_COMMIT_COMMAND, 0, 0);
      assertAttributeValue(ExtendedStatistic.LOCAL_COMMIT_EXECUTION_TIME, 0, 0);
      assertAttributeValue(ExtendedStatistic.REMOTE_COMMIT_EXECUTION_TIME, 0, 0);
      assertAttributeValue(ExtendedStatistic.PREPARE_EXECUTION_TIME, 1, 0); // //not exposed via JMX
      assertAttributeValue(ExtendedStatistic.NUM_PREPARE_COMMAND, 1, 0);
      assertAttributeValue(ExtendedStatistic.LOCAL_PREPARE_EXECUTION_TIME, MICROSECONDS, 0);
      assertAttributeValue(ExtendedStatistic.REMOTE_PREPARE_EXECUTION_TIME, 0, 0);
      assertAttributeValue(ExtendedStatistic.TX_COMPLETE_NOTIFY_EXECUTION_TIME, 0, 0);
      assertAttributeValue(ExtendedStatistic.NUM_TX_COMPLETE_NOTIFY_COMMAND, 0, 0);
      assertAttributeValue(ExtendedStatistic.NUM_LOCK_PER_LOCAL_TX, 0, 0);
      assertAttributeValue(ExtendedStatistic.NUM_LOCK_PER_REMOTE_TX, 0, 0);
      assertAttributeValue(ExtendedStatistic.NUM_LOCK_PER_SUCCESS_LOCAL_TX, 0, 0);
      assertAttributeValue(ExtendedStatistic.LOCK_WAITING_TIME, 0, 0);
      assertAttributeValue(ExtendedStatistic.LOCK_HOLD_TIME, 0, 0);
      assertAttributeValue(ExtendedStatistic.NUM_HELD_LOCKS, 0, 0);
      assertAttributeValue(ExtendedStatistic.NUM_HELD_LOCKS_SUCCESS_TX, 0, 0);
      //assertAttributeValue(ExtendedStatistic.PREPARE_COMMAND_SIZE);        // L
      //assertAttributeValue(ExtendedStatistic.COMMIT_COMMAND_SIZE);         // L
      //assertAttributeValue(ExtendedStatistic.CLUSTERED_GET_COMMAND_SIZE);  // L
      assertAttributeValue(ExtendedStatistic.NUM_LOCK_FAILED_TIMEOUT, 0, 0);
      assertAttributeValue(ExtendedStatistic.NUM_LOCK_FAILED_DEADLOCK, 0, 0);
      assertAttributeValue(ExtendedStatistic.NUM_RTTS_PREPARE, 0, 0);
      assertAttributeValue(ExtendedStatistic.RTT_PREPARE, 0, 0);
      assertAttributeValue(ExtendedStatistic.NUM_RTTS_COMMIT, 0, 0);
      assertAttributeValue(ExtendedStatistic.RTT_COMMIT, 0, 0);
      assertAttributeValue(ExtendedStatistic.NUM_RTTS_ROLLBACK, 0, 0);
      assertAttributeValue(ExtendedStatistic.RTT_ROLLBACK, 0, 0);
      assertAttributeValue(ExtendedStatistic.NUM_RTTS_GET, 0, 0);
      assertAttributeValue(ExtendedStatistic.RTT_GET, 0, 0);
      assertAttributeValue(ExtendedStatistic.ASYNC_PREPARE, 0, 0);
      assertAttributeValue(ExtendedStatistic.NUM_ASYNC_PREPARE, 0, 0);
      assertAttributeValue(ExtendedStatistic.ASYNC_COMMIT, 0, 0);
      assertAttributeValue(ExtendedStatistic.NUM_ASYNC_COMMIT, 0, 0);
      assertAttributeValue(ExtendedStatistic.ASYNC_ROLLBACK, 0, 0);
      assertAttributeValue(ExtendedStatistic.NUM_ASYNC_ROLLBACK, 0, 0);
      assertAttributeValue(ExtendedStatistic.ASYNC_COMPLETE_NOTIFY, 0, 0);
      assertAttributeValue(ExtendedStatistic.NUM_ASYNC_COMPLETE_NOTIFY, 0, 0);
      assertAttributeValue(ExtendedStatistic.NUM_NODES_PREPARE, 0, 0);
      assertAttributeValue(ExtendedStatistic.NUM_NODES_COMMIT, 0, 0);
      assertAttributeValue(ExtendedStatistic.NUM_NODES_ROLLBACK, 0, 0);
      assertAttributeValue(ExtendedStatistic.NUM_NODES_COMPLETE_NOTIFY, 0, 0);
      assertAttributeValue(ExtendedStatistic.NUM_NODES_GET, 0, 0);
      assertAttributeValue(ExtendedStatistic.RESPONSE_TIME, MICROSECONDS, 0);
      resetStats();
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      for (int i = 0; i < NUM_NODES; ++i) {
         ConfigurationBuilder builder = getDefaultClusteredCacheConfig(sync ? CacheMode.REPL_SYNC : CacheMode.REPL_ASYNC, true);
         builder.transaction().syncCommitPhase(sync2ndPhase).syncRollbackPhase(sync2ndPhase);
         if (totalOrder) {
            builder.transaction().transactionProtocol(TransactionProtocol.TOTAL_ORDER);
         }
         builder.locking().isolationLevel(IsolationLevel.REPEATABLE_READ).writeSkewCheck(writeSkew)
               .lockAcquisitionTimeout(0);
         builder.clustering().hash().numOwners(1);
         if (writeSkew) {
            builder.versioning().enable().scheme(VersioningScheme.SIMPLE);
         }
         builder.transaction().recovery().disable();
         extendedStatisticInterceptors[i] = new ExtendedStatisticInterceptor(TEST_TIME_SERVICE);
         //transactionInterceptors[i] = new TransactionInterceptor();
         builder.customInterceptors().addInterceptor().interceptor(extendedStatisticInterceptors[i])
               .after(TxInterceptor.class);
         //builder.customInterceptors().addInterceptor().interceptor(transactionInterceptors[i])
         //      .position(InterceptorConfiguration.Position.FIRST);
         addClusterEnabledCacheManager(builder);
      }
      waitForClusterToForm();
   }

   private void assertEmpty(Object... keys) {
      for (Cache cache : caches()) {
         for (Object key : keys) {
            assertNull(cache.get(key));
         }
      }
   }

   private void resetStats() {
      for (ExtendedStatisticInterceptor interceptor : extendedStatisticInterceptors) {
         interceptor.resetStatistics();
         for (ExtendedStatistic extendedStatistic : ExtendedStatistic.values()) {
            assertEquals(interceptor.getAttribute(extendedStatistic), 0.0, "Attribute " + extendedStatistic +
                  " is not zero after reset");
         }
      }
   }

   private void resetCounter() {
      //for (TransactionInterceptor interceptor : transactionInterceptors) {
      //   interceptor.reset();
      //}
   }

   private void assertAttributeValue(ExtendedStatistic attr, double... expected) {
      assertNotNull(expected);
      assertEquals(NUM_NODES, expected.length);
      for (int i = 0; i < NUM_NODES; ++i) {
         assertEquals(extendedStatisticInterceptors[i].getAttribute(attr), expected[i],
                      "Attribute " + attr + " has wrong value");
      }
   }

   public static class TransactionInterceptor extends BaseCustomInterceptor {
      private int prepares = 0;
      private int commits = 0;
      private int rollbacks = 0;

      @Override
      public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
         try {
            return invokeNextInterceptor(ctx, command);
         } finally {
            incrementPrepare();
         }
      }

      @Override
      public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
         try {
            return invokeNextInterceptor(ctx, command);
         } finally {
            incrementCommit();
         }
      }

      @Override
      public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
         try {
            return invokeNextInterceptor(ctx, command);
         } finally {
            incrementRollback();
         }
      }

      public final synchronized void awaitForPrepares(int expected) throws InterruptedException {
         while (prepares < expected) {
            wait();
         }
      }

      public final synchronized void awaitForCommits(int expected) throws InterruptedException {
         while (commits < expected) {
            wait();
         }
      }

      public final synchronized void awaitForRollbacks(int expected) throws InterruptedException {
         while (rollbacks < expected) {
            wait();
         }
      }

      public final void reset() {
         prepares = 0;
         commits = 0;
         rollbacks = 0;
      }

      private synchronized void incrementPrepare() {
         prepares++;
         notifyAll();
      }

      private synchronized void incrementCommit() {
         commits++;
         notifyAll();
      }

      private synchronized void incrementRollback() {
         rollbacks++;
         notifyAll();
      }
   }
}
