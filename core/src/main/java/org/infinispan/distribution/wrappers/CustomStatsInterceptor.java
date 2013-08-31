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

import org.infinispan.commands.SetTransactionClassCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.GMUPrepareCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.base.BaseCustomInterceptor;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.jmx.annotations.Parameter;
import org.infinispan.remoting.InboundInvocationHandler;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.CommandAwareRpcDispatcher;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.stats.ExposedStatistic;
import org.infinispan.stats.LockRelatedStatsHelper;
import org.infinispan.stats.TransactionsStatisticsRegistry;
import org.infinispan.stats.container.TransactionStatistics;
import org.infinispan.stats.topK.StreamLibContainer;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.transaction.WriteSkewException;
import org.infinispan.transaction.gmu.ValidationException;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.concurrent.locks.DeadlockDetectedException;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.lang.reflect.Field;
import java.util.Collection;

import static org.infinispan.stats.ExposedStatistic.*;

/**
 * Massive hack for a noble cause!
 *
 * @author Mircea Markus <mircea.markus@jboss.com> (C) 2011 Red Hat Inc.
 * @author Diego Didona <didona@gsd.inesc-id.pt>
 * @author Pedro Ruivo
 * @since 5.2
 */
@MBean(objectName = "ExtendedStatistics",
        description = "Component that manages and exposes extended statistics " +
                "relevant to transactions.")
public final class CustomStatsInterceptor extends BaseCustomInterceptor {
   //TODO what about the transaction implicit vs transaction explicit? should we take in account this and ignore
   //the implicit stuff?

   private final Log log = LogFactory.getLog(getClass());
   private TransactionTable transactionTable;
   private Configuration configuration;
   private RpcManager rpcManager;
   private DistributionManager distributionManager;

   @Inject
   public void inject(TransactionTable transactionTable, Configuration config, DistributionManager distributionManager) {
      this.transactionTable = transactionTable;
      this.configuration = config;
      this.distributionManager = distributionManager;
   }

   @Start(priority = 99)
   public void start() {
      // we want that this method is the last to be invoked, otherwise the start method is not invoked
      // in the real components
      replace();
      log.info("Initializing the TransactionStatisticsRegistry");
      TransactionsStatisticsRegistry.init(this.configuration);
      if (configuration.versioning().scheme().equals(VersioningScheme.GMU))
         LockRelatedStatsHelper.enable();
   }

   @Override
   public Object visitSetTransactionClassCommand(InvocationContext ctx, SetTransactionClassCommand command) throws Throwable {
      final String transactionClass = command.getTransactionalClass();

      invokeNextInterceptor(ctx, command);
      if (ctx.isInTxScope() && transactionClass != null && !transactionClass.isEmpty()) {
         final GlobalTransaction globalTransaction = ((TxInvocationContext) ctx).getGlobalTransaction();
         if (log.isTraceEnabled()) {
            log.tracef("Setting the transaction class %s for %s", transactionClass, globalTransaction);
         }
         final TransactionStatistics txs = initStatsIfNecessary(ctx);
         TransactionsStatisticsRegistry.setTransactionalClass(transactionClass, txs);
         globalTransaction.setTransactionClass(command.getTransactionalClass());
         return Boolean.TRUE;
      } else if (log.isTraceEnabled()) {
         log.tracef("Did not set transaction class %s. It is not inside a transaction or the transaction class is null",
                 transactionClass);
      }
      return Boolean.FALSE;
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      if (log.isTraceEnabled()) {
         log.tracef("Visit Put Key Value command %s. Is it in transaction scope? %s. Is it local? %s", command,
                 ctx.isInTxScope(), ctx.isOriginLocal());
      }
      Object ret;
      if (TransactionsStatisticsRegistry.isActive() && ctx.isInTxScope()) {
         final TransactionStatistics transactionStatistics = initStatsIfNecessary(ctx);
         transactionStatistics.setUpdateTransaction();
         long initTime = System.nanoTime();
         transactionStatistics.incrementValue(NUM_PUT);
         transactionStatistics.addNTBCValue(initTime);
         try {
            ret = invokeNextInterceptor(ctx, command);
         } catch (TimeoutException e) {
            if (ctx.isOriginLocal()) {
               transactionStatistics.incrementValue(NUM_LOCK_FAILED_TIMEOUT);
            }
            throw e;
         } catch (DeadlockDetectedException e) {
            if (ctx.isOriginLocal()) {
               transactionStatistics.incrementValue(NUM_LOCK_FAILED_DEADLOCK);
            }
            throw e;
         } catch (WriteSkewException e) {
            if (ctx.isOriginLocal()) {
               transactionStatistics.incrementValue(NUM_WRITE_SKEW);
            }
            throw e;
         }
         if (isRemote(command.getKey())) {
            transactionStatistics.addValue(REMOTE_PUT_EXECUTION, System.nanoTime() - initTime);
            transactionStatistics.incrementValue(NUM_REMOTE_PUT);
         }
         transactionStatistics.setLastOpTimestamp(System.nanoTime());
         return ret;
      } else
         return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      if (log.isTraceEnabled()) {
         log.tracef("Visit Get Key Value command %s. Is it in transaction scope? %s. Is it local? %s", command,
                 ctx.isInTxScope(), ctx.isOriginLocal());
      }
      Object ret;
      if (TransactionsStatisticsRegistry.isActive() && ctx.isInTxScope()) {

         final TransactionStatistics transactionStatistics = initStatsIfNecessary(ctx);
         transactionStatistics.notifyRead();
         long currCpuTime = 0;
         boolean isRemoteKey = isRemote(command.getKey());
         if (isRemoteKey) {
            currCpuTime = TransactionsStatisticsRegistry.getThreadCPUTime();
         }
         long currTimeForAllGetCommand = System.nanoTime();
         transactionStatistics.addNTBCValue(currTimeForAllGetCommand);
         ret = invokeNextInterceptor(ctx, command);
         long lastTimeOp = System.nanoTime();
         if (isRemoteKey) {
            transactionStatistics.incrementValue(NUM_LOCAL_REMOTE_GET);
            transactionStatistics.addValue(LOCAL_REMOTE_GET_R, lastTimeOp - currTimeForAllGetCommand);
            if (TransactionsStatisticsRegistry.isSampleServiceTime())
               transactionStatistics.addValue(LOCAL_REMOTE_GET_S, TransactionsStatisticsRegistry.getThreadCPUTime() - currCpuTime);
         }
         transactionStatistics.addValue(ALL_GET_EXECUTION, lastTimeOp - currTimeForAllGetCommand);
         transactionStatistics.incrementValue(NUM_GET);
         transactionStatistics.setLastOpTimestamp(lastTimeOp);
      } else {
         ret = invokeNextInterceptor(ctx, command);
      }
      return ret;
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      if (log.isTraceEnabled()) {
         log.tracef("Visit Commit command %s. Is it local?. Transaction is %s", command,
                 ctx.isOriginLocal(), command.getGlobalTransaction().globalId());
      }
      if (!TransactionsStatisticsRegistry.isActive()) {
         return invokeNextInterceptor(ctx, command);
      }

      TransactionStatistics transactionStatistics = initStatsIfNecessary(ctx);
      long currCpuTime = TransactionsStatisticsRegistry.getThreadCPUTime();
      long currTime = System.nanoTime();
      transactionStatistics.addNTBCValue(currTime);
      transactionStatistics.attachId(ctx.getGlobalTransaction());
      if (LockRelatedStatsHelper.shouldAppendLocks(configuration, true, !ctx.isOriginLocal())) {
         if (log.isTraceEnabled())
            log.trace("Appending locks for " + ((!ctx.isOriginLocal()) ? "remote " : "local ") + "transaction " +
                    ctx.getGlobalTransaction().getId());
         TransactionsStatisticsRegistry.appendLocks();
      } else {
         if (log.isTraceEnabled())
            log.trace("Not appending locks for " + ((!ctx.isOriginLocal()) ? "remote " : "local ") + "transaction " +
                    ctx.getGlobalTransaction().getId());
      }
      Object ret = invokeNextInterceptor(ctx, command);

      handleCommitCommand(transactionStatistics, currTime, currCpuTime, ctx);

      transactionStatistics.setTransactionOutcome(true);
      //We only terminate a local transaction, since RemoteTransactions are terminated from the InboundInvocationHandler
      if (ctx.isOriginLocal()) {
         TransactionsStatisticsRegistry.terminateTransaction(transactionStatistics);
      }

      return ret;
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      if (log.isTraceEnabled()) {
         log.tracef("Visit Prepare command %s. Is it local?. Transaction is %s", command,
                 ctx.isOriginLocal(), command.getGlobalTransaction().globalId());
      }

      if (!TransactionsStatisticsRegistry.isActive()) {
         return invokeNextInterceptor(ctx, command);
      }

      TransactionStatistics transactionStatistics = initStatsIfNecessary(ctx);
      transactionStatistics.onPrepareCommand();
      if (command.hasModifications()) {
         transactionStatistics.setUpdateTransaction();
      }


      boolean success = false;
      try {
         long currTime = System.nanoTime();
         long currCpuTime = TransactionsStatisticsRegistry.getThreadCPUTime();
         Object ret = invokeNextInterceptor(ctx, command);
         success = true;
         handlePrepareCommand(transactionStatistics, currTime, currCpuTime, ctx, command);
         return ret;
      } catch (TimeoutException e) {
         if (ctx.isOriginLocal()) {
            transactionStatistics.incrementValue(NUM_LOCK_FAILED_TIMEOUT);
         }
         throw e;
      } catch (DeadlockDetectedException e) {
         if (ctx.isOriginLocal()) {
            transactionStatistics.incrementValue(NUM_LOCK_FAILED_DEADLOCK);
         }
         throw e;
      } catch (WriteSkewException e) {
         if (ctx.isOriginLocal()) {
            transactionStatistics.incrementValue(NUM_WRITE_SKEW);
         }
         throw e;
      } catch (ValidationException e) {
         if (ctx.isOriginLocal()) {
            transactionStatistics.incrementValue(NUM_ABORTED_TX_DUE_TO_VALIDATION);
         }
         //Increment the killed xact only if the murder has happened on this node (whether the xact is local or remote)
         transactionStatistics.incrementValue(NUM_KILLED_TX_DUE_TO_VALIDATION);
         throw e;

      }
      //We don't care about cacheException for earlyAbort

      //Remote exception we care about
      catch (Exception e) {
         if (ctx.isOriginLocal()) {
            if (e.getCause() instanceof TimeoutException) {
               transactionStatistics.incrementValue(NUM_LOCK_FAILED_TIMEOUT);
            } else if (e.getCause() instanceof ValidationException) {
               transactionStatistics.incrementValue(NUM_ABORTED_TX_DUE_TO_VALIDATION);

            }
         }

         throw e;
      } finally {
         if (command.isOnePhaseCommit()) {
            transactionStatistics.setTransactionOutcome(success);
            if (ctx.isOriginLocal()) {
               transactionStatistics.terminateTransaction();
            }
         }
      }
   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      if (log.isTraceEnabled()) {
         log.tracef("Visit Rollback command %s. Is it local?. Transaction is %s", command,
                 ctx.isOriginLocal(), command.getGlobalTransaction().globalId());
      }

      if (!TransactionsStatisticsRegistry.isActive()) {
         return invokeNextInterceptor(ctx, command);
      }

      TransactionStatistics transactionStatistics = initStatsIfNecessary(ctx);
      long currentCpuTime = TransactionsStatisticsRegistry.getThreadCPUTime();
      long initRollbackTime = System.nanoTime();
      Object ret = invokeNextInterceptor(ctx, command);
      transactionStatistics.setTransactionOutcome(false);

      handleRollbackCommand(transactionStatistics, initRollbackTime, currentCpuTime, ctx);
      if (ctx.isOriginLocal()) {
         TransactionsStatisticsRegistry.terminateTransaction(transactionStatistics);
      }

      return ret;
   }


   @ManagedAttribute(description = "Average Prepare Round-Trip Time duration (in microseconds)",
           displayName = "Avg Prepare RTT")
   public long getAvgPrepareRtt() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((RTT_PREPARE), null)));
   }

   @ManagedAttribute(description = "Average Commit Round-Trip Time duration (in microseconds)",
           displayName = "Avg Commit RTT")
   public long getAvgCommitRtt() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute(RTT_COMMIT, null)));
   }

   @ManagedAttribute(description = "Average Remote Get Round-Trip Time duration (in microseconds)",
           displayName = "Avg Remote Get RTT")
   public long getAvgRemoteGetRtt() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute((RTT_GET), null)));
   }

   @ManagedAttribute(description = "Average number of nodes in Commit destination set",
           displayName = "Avg no. of Nodes in Commit Destination Set")
   public long getAvgNumNodesCommit() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute(NUM_NODES_COMMIT, null)));
   }

   @ManagedAttribute(description = "Avg prepare command size (in bytes)",
           displayName = "Avg prepare command size (in bytes)")
   public long getAvgPrepareCommandSize() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(PREPARE_COMMAND_SIZE, null));
   }


   @ManagedAttribute(description = "Percentage of Write transaction executed in all successfully executed " +
           "transactions (local transaction only)",
           displayName = "Percentage of Successfully Write Transactions")
   public double getPercentageSuccessWriteTransactions() {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(SUCCESSFUL_WRITE_PERCENTAGE, null));
   }

   @ManagedAttribute(description = "Abort Rate",
           displayName = "Abort Rate")
   public double getAbortRate() {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(ABORT_RATE, null));
   }

   @ManagedAttribute(description = "Throughput (in transactions per second)",
           displayName = "Throughput")
   public double getThroughput() {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(THROUGHPUT, null));
   }
   @ManagedAttribute(description = "Average number of get operations per local read transaction",
           displayName = "Avg no. of get operations per local read transaction")
   public long getAvgGetsPerROTransaction() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_SUCCESSFUL_GETS_RO_TX, null));
   }

   @ManagedAttribute(description = "Average number of get operations per local write transaction",
           displayName = "Avg no. of get operations per local write transaction")
   public long getAvgGetsPerWrTransaction() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_SUCCESSFUL_GETS_WR_TX, null));
   }

   @ManagedAttribute(description = "Average number of remote get operations per local write transaction",
           displayName = "Avg no. of remote get operations per local write transaction")
   public double getAvgRemoteGetsPerWrTransaction() {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(NUM_SUCCESSFUL_REMOTE_GETS_WR_TX, null));
   }

   @ManagedAttribute(description = "Average number of remote get operations per local read transaction",
           displayName = "Avg no. of remote get operations per local read transaction")
   public double getAvgRemoteGetsPerROTransaction() {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(NUM_SUCCESSFUL_REMOTE_GETS_RO_TX, null));
   }

   @ManagedAttribute(description = "Average number of put operations per local write transaction",
           displayName = "Avg no. of put operations per local write transaction")
   public double getAvgPutsPerWrTransaction() {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(NUM_SUCCESSFUL_PUTS_WR_TX, null));
   }

   @ManagedAttribute(description = "Average number of remote put operations per local write transaction",
           displayName = "Avg no. of remote put operations per local write transaction")
   public double getAvgRemotePutsPerWrTransaction() {
      return handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(NUM_SUCCESSFUL_REMOTE_PUTS_WR_TX, null));
   }

   @ManagedAttribute(description = "Number of gets performed since last reset",
           displayName = "No. of Gets")
   public long getNumberOfGets() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_GET, null));
   }

   @ManagedAttribute(description = "Number of remote gets performed since last reset",
           displayName = "No. of Remote Gets")
   public long getNumberOfRemoteGets() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_LOCAL_REMOTE_GET, null));
   }

   @ManagedAttribute(description = "Number of puts performed since last reset",
           displayName = "No. of Puts")
   public long getNumberOfPuts() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_PUT, null));
   }

   @ManagedAttribute(description = "Number of remote puts performed since last reset",
           displayName = "No. of Remote Puts")
   public long getNumberOfRemotePuts() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_REMOTE_PUT, null));
   }

   @ManagedOperation(description = "Reset all the statistics collected",
           displayName = "Reset All Statistics")
   public void resetStatistics() {
      TransactionsStatisticsRegistry.reset();
   }


   @ManagedAttribute(description = "Average NTCB time (in microseconds)",
           displayName = "Avg NTCB time")
   public long getAvgNTCBTime() {
      return handleLong((Long) (TransactionsStatisticsRegistry.getAttribute(NTBC, null)));
   }

   @ManagedAttribute(description = "Number of nodes in the cluster",
           displayName = "No. of nodes")
   public long getNumNodes() {
      try {
         if (rpcManager != null) {
            Collection<Address> members = rpcManager.getMembers();
            //member can be null if we haven't received the initial topology
            return members == null ? 1 : members.size();
         }
         //if rpc manager is null, we are in local mode.
         return 1;
      } catch (Throwable throwable) {
         log.error("Error obtaining Number of Nodes. returning 1", throwable);
         return 1;
      }
   }

   @ManagedAttribute(description = "Number of replicas for each key",
           displayName = "Replication Degree")
   public long getReplicationDegree() {
      try {
         return distributionManager == null ? 1 : distributionManager.getConsistentHash().getNumOwners();
      } catch (Throwable throwable) {
         log.error("Error obtaining Replication Degree. returning 0", throwable);
         return 0;
      }
   }

   @ManagedAttribute(description = "Number of concurrent transactions executing on the current node",
           displayName = "Local Active Transactions")
   public long getLocalActiveTransactions() {
      try {
         return transactionTable == null ? 0 : transactionTable.getLocalTxCount();
      } catch (Throwable throwable) {
         log.error("Error obtaining Local Active Transactions. returning 0", throwable);
         return 0;
      }
   }




    /*Local Update*/

   @ManagedAttribute(description = "Avg CPU demand to execute a local tx up to the prepare phase",
           displayName = "Avg CPU to execute a local tx up to the prepare phase")
   public long getLocalUpdateTxLocalServiceTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(UPDATE_TX_LOCAL_S, null));
   }

   @ManagedAttribute(description = "Avg response time of the local part of a tx up to the prepare phase",
           displayName = "Avg response time of a tx up to the prepare phase")
   public long getLocalUpdateTxLocalResponseTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(UPDATE_TX_LOCAL_R, null));
   }



   /*Remote*/

   /* Read Only*/


   @ManagedAttribute(description = "Avg response time of the local execution of a read only tx",
           displayName = "Avg response time of local execution of a read only tx")
   public long getLocalReadOnlyTxLocalResponseTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(READ_ONLY_TX_LOCAL_R, null));
   }

   @ManagedAttribute(displayName = "Avg response time of a local update tx",
           description = "Avg response time of a local update tx")
   public long getLocalUpdateTxTotalResponseTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(UPDATE_TX_TOTAL_R, null));
   }

   @ManagedAttribute(displayName = "Avg CPU of a read only tx",
           description = "Avg CPU demand of a read only tx")
   public long getReadOnlyTxTotalResponseTime() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(READ_ONLY_TX_TOTAL_R, null));
   }



   @ManagedAttribute(description = "Returns true if the statistics are enabled, false otherwise",
           displayName = "Enabled")
   public final boolean isEnabled() {
      return TransactionsStatisticsRegistry.isActive();
   }

   @ManagedAttribute(description = "Number of successful TO-GMU prepares that did not wait",
           displayName = "No. of successful TO-GMU prepares that did not wait")
   public final long getNumTOGMUSuccessfulPrepareNoWait() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_TO_GMU_PREPARE_COMMAND_RTT_NO_WAITED, null));
   }

   @ManagedAttribute(description = "Avg rtt for TO-GMU prepares that did not wait",
           displayName = "Avg rtt for TO-GMU prepares that did not wait")
   public final long getAvgTOGMUPrepareNoWaitRtt() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(TO_GMU_PREPARE_COMMAND_RTT_NO_WAIT, null));
   }

   @ManagedAttribute(description = "Avg rtt for GMU remote get that do not wait",
           displayName = "Avg rtt for GMU remote get that do not wait")
   public final long getAvgGmuClusteredGetCommandRttNoWait() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(RTT_GET_NO_WAIT, null));
   }



   @ManagedOperation(description = "Remote nodes from which a read-only transaction reads from per tx class",
           displayName = "Remote nodes from which a read-only transaction reads from per tx class")
   public final long getLocalReadOnlyTxRemoteNodesRead() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_REMOTE_NODES_READ_RO_TX, null));
   }

   @ManagedOperation(description = "Remote nodes from which an update transaction reads from per tx class",
           displayName = "Remote nodes from which an update transaction reads from per tx class")
   public final long getLocalUpdateTxRemoteNodesRead() {
      return handleLong((Long) TransactionsStatisticsRegistry.getAttribute(NUM_REMOTE_NODES_READ_WR_TX, null));
   }

   //NB: readOnly transactions are never aborted (RC, RR, GMU)
   private void handleRollbackCommand(TransactionStatistics transactionStatistics, long initTime, long initCpuTime, TxInvocationContext ctx) {
      ExposedStatistic stat, cpuStat, counter;
      if (ctx.isOriginLocal()) {
         if (transactionStatistics.isPrepareSent() || ctx.getCacheTransaction().wasPrepareSent()) {
            cpuStat = UPDATE_TX_LOCAL_REMOTE_ROLLBACK_S;
            stat = UPDATE_TX_LOCAL_REMOTE_ROLLBACK_R;
            counter = NUM_UPDATE_TX_LOCAL_REMOTE_ROLLBACK;
            transactionStatistics.incrementValue(NUM_REMOTELY_ABORTED);
         } else {
            if (transactionStatistics.stillLocalExecution()) {
               transactionStatistics.incrementValue(NUM_EARLY_ABORTS);
            } else {
               transactionStatistics.incrementValue(NUM_LOCALPREPARE_ABORTS);
            }
            cpuStat = UPDATE_TX_LOCAL_LOCAL_ROLLBACK_S;
            stat = UPDATE_TX_LOCAL_LOCAL_ROLLBACK_R;
            counter = NUM_UPDATE_TX_LOCAL_LOCAL_ROLLBACK;
         }
      } else {
         cpuStat = UPDATE_TX_REMOTE_ROLLBACK_S;
         stat = UPDATE_TX_REMOTE_ROLLBACK_R;
         counter = NUM_UPDATE_TX_REMOTE_ROLLBACK;
      }
      updateWallClockTime(transactionStatistics, stat, counter, initTime);
      if (TransactionsStatisticsRegistry.isSampleServiceTime())
         updateServiceTimeWithoutCounter(transactionStatistics, cpuStat, initCpuTime);
   }

   private void handleCommitCommand(TransactionStatistics transactionStatistics, long initTime, long initCpuTime, TxInvocationContext ctx) {
      ExposedStatistic stat, cpuStat, counter;
      //In TO some xacts can be marked as RO even though remote
      if (ctx.isOriginLocal() && transactionStatistics.isReadOnly()) {
         cpuStat = READ_ONLY_TX_COMMIT_S;
         counter = NUM_READ_ONLY_TX_COMMIT;
         stat = READ_ONLY_TX_COMMIT_R;
      }
      //This is valid both for local and remote. The registry will populate the right container
      else {
         if (ctx.isOriginLocal()) {
            cpuStat = UPDATE_TX_LOCAL_COMMIT_S;
            counter = NUM_UPDATE_TX_LOCAL_COMMIT;
            stat = UPDATE_TX_LOCAL_COMMIT_R;
         } else {
            cpuStat = UPDATE_TX_REMOTE_COMMIT_S;
            counter = NUM_UPDATE_TX_REMOTE_COMMIT;
            stat = UPDATE_TX_REMOTE_COMMIT_R;
         }
      }

      updateWallClockTime(transactionStatistics, stat, counter, initTime);
      if (TransactionsStatisticsRegistry.isSampleServiceTime())
         updateServiceTimeWithoutCounter(transactionStatistics, cpuStat, initCpuTime);
   }

   /**
    * Increases the service and responseTime; This is invoked *only* if the prepareCommand is executed correctly
    */
   private void handlePrepareCommand(TransactionStatistics transactionStatistics, long initTime, long initCpuTime, TxInvocationContext ctx, PrepareCommand command) {
      ExposedStatistic stat, cpuStat, counter;
      if (transactionStatistics.isReadOnly()) {
         stat = READ_ONLY_TX_PREPARE_R;
         cpuStat = READ_ONLY_TX_PREPARE_S;
         updateWallClockTimeWithoutCounter(transactionStatistics, stat, initTime);
         if (TransactionsStatisticsRegistry.isSampleServiceTime())
            updateServiceTimeWithoutCounter(transactionStatistics, cpuStat, initCpuTime);
      }
      //This is valid both for local and remote. The registry will populate the right container
      else {
         if (ctx.isOriginLocal()) {
            stat = UPDATE_TX_LOCAL_PREPARE_R;
            cpuStat = UPDATE_TX_LOCAL_PREPARE_S;

         } else {
            stat = UPDATE_TX_REMOTE_PREPARE_R;
            cpuStat = UPDATE_TX_REMOTE_PREPARE_S;
         }
         counter = NUM_UPDATE_TX_PREPARED;
         updateWallClockTime(transactionStatistics, stat, counter, initTime);
         if (TransactionsStatisticsRegistry.isSampleServiceTime())
            updateServiceTimeWithoutCounter(transactionStatistics, cpuStat, initCpuTime);
      }

      //Take stats relevant to the avg number of read and write data items in the message
      //NB: these stats are taken only for prepare that will turn into a commit
      transactionStatistics.addValue(NUM_OWNED_RD_ITEMS_IN_OK_PREPARE, localRd(command));
      transactionStatistics.addValue(NUM_OWNED_WR_ITEMS_IN_OK_PREPARE, localWr(command));
   }

   private int localWr(PrepareCommand command) {
      WriteCommand[] wrSet = command.getModifications();
      int localWr = 0;
      for (WriteCommand wr : wrSet) {
         for (Object k : wr.getAffectedKeys()) {
            if (!isRemote(k))
               localWr++;
         }
      }
      return localWr;
   }

   private int localRd(PrepareCommand command) {
      if (!(command instanceof GMUPrepareCommand))
         return 0;
      int localRd = 0;

      Object[] rdSet = ((GMUPrepareCommand) command).getReadSet();
      for (Object rd : rdSet) {
         if (!isRemote(rd))
            localRd++;
      }
      return localRd;
   }

   private void replace() {
      log.info("CustomStatsInterceptor Enabled!");
      ComponentRegistry componentRegistry = cache.getAdvancedCache().getComponentRegistry();
      this.wireConfiguration();
      GlobalComponentRegistry globalComponentRegistry = componentRegistry.getGlobalComponentRegistry();
      InboundInvocationHandlerWrapper invocationHandlerWrapper = rewireInvocationHandler(globalComponentRegistry);
      globalComponentRegistry.rewire();

      replaceFieldInTransport(componentRegistry, invocationHandlerWrapper);

      replaceRpcManager(componentRegistry);
      replaceLockManager(componentRegistry);
      componentRegistry.rewire();
   }

   private void wireConfiguration() {
      this.configuration = cache.getAdvancedCache().getCacheConfiguration();
   }

   private void replaceFieldInTransport(ComponentRegistry componentRegistry, InboundInvocationHandlerWrapper invocationHandlerWrapper) {
      JGroupsTransport t = (JGroupsTransport) componentRegistry.getComponent(Transport.class);
      CommandAwareRpcDispatcher card = t.getCommandAwareRpcDispatcher();
      try {
         Field f = card.getClass().getDeclaredField("inboundInvocationHandler");
         f.setAccessible(true);
         f.set(card, invocationHandlerWrapper);
      } catch (NoSuchFieldException e) {
         e.printStackTrace();
      } catch (IllegalAccessException e) {
         e.printStackTrace();
      }
   }

   private InboundInvocationHandlerWrapper rewireInvocationHandler(GlobalComponentRegistry globalComponentRegistry) {
      InboundInvocationHandler inboundHandler = globalComponentRegistry.getComponent(InboundInvocationHandler.class);
      InboundInvocationHandlerWrapper invocationHandlerWrapper = new InboundInvocationHandlerWrapper(inboundHandler,
              transactionTable);
      globalComponentRegistry.registerComponent(invocationHandlerWrapper, InboundInvocationHandler.class);
      return invocationHandlerWrapper;
   }

   private void replaceLockManager(ComponentRegistry componentRegistry) {
      LockManager lockManager = componentRegistry.getComponent(LockManager.class);
      // this.configuration.customStatsConfiguration().sampleServiceTimes());
      LockManagerWrapper lockManagerWrapper = new LockManagerWrapper(lockManager, StreamLibContainer.getOrCreateStreamLibContainer(cache), true);
      componentRegistry.registerComponent(lockManagerWrapper, LockManager.class);
   }

   private void replaceRpcManager(ComponentRegistry componentRegistry) {
      RpcManager rpcManager = componentRegistry.getComponent(RpcManager.class);
      RpcManagerWrapper rpcManagerWrapper = new RpcManagerWrapper(rpcManager);
      componentRegistry.registerComponent(rpcManagerWrapper, RpcManager.class);
      this.rpcManager = rpcManagerWrapper;
   }

   private TransactionStatistics initStatsIfNecessary(InvocationContext ctx) {
      if (ctx.isInTxScope()) {
         TransactionStatistics transactionStatistics = TransactionsStatisticsRegistry
                 .initTransactionIfNecessary((TxInvocationContext) ctx);
         if (transactionStatistics == null) {
            throw new IllegalStateException("Transaction Statistics cannot be null with transactional context");
         }
         return transactionStatistics;
      }
      return null;
   }

   private void updateWallClockTime(TransactionStatistics transactionStatistics, ExposedStatistic duration, ExposedStatistic counter, long initTime) {
      transactionStatistics.addValue(duration, System.nanoTime() - initTime);
      transactionStatistics.incrementValue(counter);
   }

   private void updateWallClockTimeWithoutCounter(TransactionStatistics transactionStatistics, ExposedStatistic duration, long initTime) {
      transactionStatistics.addValue(duration, System.nanoTime() - initTime);
   }

   private void updateServiceTimeWithoutCounter(TransactionStatistics transactionStatistics, ExposedStatistic time, long initTime) {
      transactionStatistics.addValue(time, TransactionsStatisticsRegistry.getThreadCPUTime() - initTime);
   }

   private long handleLong(Long value) {
      return value == null ? 0 : value;
   }

   private double handleDouble(Double value) {
      return value == null ? 0 : value;
   }

   private boolean isRemote(Object key) {
      return distributionManager != null && !distributionManager.getLocality(key).isLocal();
   }

}
