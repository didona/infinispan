package org.infinispan.stats;

import org.infinispan.stats.container.ExtendedStatisticsContainer;
import org.infinispan.stats.container.LocalExtendedStatisticsContainer;
import org.infinispan.stats.container.RemoteExtendedStatisticsContainer;
import org.infinispan.stats.exception.ExtendedStatisticNotFoundException;
import org.infinispan.stats.percentiles.PercentileStats;
import org.infinispan.stats.percentiles.PercentileStatsFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import static org.infinispan.stats.ExtendedStatistic.*;


/**
 * Websiste: www.cloudtm.eu Date: 01/05/12
 *
 * @author Diego Didona <didona@gsd.inesc-id.pt>
 * @author Pedro Ruivo
 * @since 5.2
 */
public class CacheStatisticCollector {
   private final static Log log = LogFactory.getLog(CacheStatisticCollector.class);
   private final TimeService timeService;
   private LocalExtendedStatisticsContainer localContainer;
   private RemoteExtendedStatisticsContainer remoteContainer;
   private PercentileStats localTransactionWrExecutionTime;
   private PercentileStats remoteTransactionWrExecutionTime;
   private PercentileStats localTransactionRoExecutionTime;
   private PercentileStats remoteTransactionRoExecutionTime;
   private long lastResetTime;

   public CacheStatisticCollector(TimeService timeService) {
      this.timeService = timeService;
      reset();
   }

   public final synchronized void reset() {
      if (log.isTraceEnabled()) {
         log.tracef("Resetting Node Scope Statistics");
      }
      this.localContainer = new LocalExtendedStatisticsContainer();
      this.remoteContainer = new RemoteExtendedStatisticsContainer();

      this.localTransactionRoExecutionTime = PercentileStatsFactory.createNewPercentileStats();
      this.localTransactionWrExecutionTime = PercentileStatsFactory.createNewPercentileStats();
      this.remoteTransactionRoExecutionTime = PercentileStatsFactory.createNewPercentileStats();
      this.remoteTransactionWrExecutionTime = PercentileStatsFactory.createNewPercentileStats();

      this.lastResetTime = timeService.now();
   }

   public final synchronized void merge(TransactionStatistics ts) {
      if (log.isTraceEnabled()) {
         log.tracef("Merge transaction statistics %s to the node statistics", ts);
      }
      ExtendedStatisticsContainer container;
      PercentileStats percentileStats;
      ExtendedStatistic percentileSample;
      if (ts.isLocalTransaction()) {
         container = localContainer;
         if (ts.isReadOnly()) {
            percentileStats = localTransactionRoExecutionTime;
            percentileSample = ts.isCommit() ? RO_TX_SUCCESSFUL_EXECUTION_TIME : RO_TX_ABORTED_EXECUTION_TIME;
         } else {
            percentileStats = localTransactionWrExecutionTime;
            percentileSample = ts.isCommit() ? WR_TX_SUCCESSFUL_EXECUTION_TIME : WR_TX_ABORTED_EXECUTION_TIME;
         }
      } else {
         container = remoteContainer;
         if (ts.isReadOnly()) {
            percentileStats = remoteTransactionRoExecutionTime;
            percentileSample = ts.isCommit() ? RO_TX_SUCCESSFUL_EXECUTION_TIME : RO_TX_ABORTED_EXECUTION_TIME;
         } else {
            percentileStats = remoteTransactionWrExecutionTime;
            percentileSample = ts.isCommit() ? WR_TX_SUCCESSFUL_EXECUTION_TIME : WR_TX_ABORTED_EXECUTION_TIME;
         }
      }
      doMerge(ts, container, percentileStats, percentileSample);
   }

   public final synchronized void addLocalValue(ExtendedStatistic stat, double value) {
      localContainer.addValue(stat, value);
   }

   public final synchronized void addRemoteValue(ExtendedStatistic stat, double value) {
      remoteContainer.addValue(stat, value);
   }

   public final synchronized double getPercentile(ExtendedStatistic param, int percentile) throws ExtendedStatisticNotFoundException {
      if (log.isTraceEnabled()) {
         log.tracef("Get percentile %s from %s", percentile, param);
      }
      switch (param) {
         case RO_LOCAL_PERCENTILE:
            return localTransactionRoExecutionTime.getKPercentile(percentile);
         case WR_LOCAL_PERCENTILE:
            return localTransactionWrExecutionTime.getKPercentile(percentile);
         case RO_REMOTE_PERCENTILE:
            return remoteTransactionRoExecutionTime.getKPercentile(percentile);
         case WR_REMOTE_PERCENTILE:
            return remoteTransactionWrExecutionTime.getKPercentile(percentile);
         default:
            throw new ExtendedStatisticNotFoundException("Invalid percentile " + param);
      }
   }

   public final synchronized double getAttribute(ExtendedStatistic stat) throws ExtendedStatisticNotFoundException {
      double value = 0;
      switch (stat) {
         case LOCAL_EXEC_NO_CONT:
            value = microAverageLocal(LOCAL_EXEC_NO_CONT, NUM_PREPARE_COMMAND);
            break;
         case LOCK_HOLD_TIME:
            value = microAverageLocalAndRemote(LOCK_HOLD_TIME, NUM_HELD_LOCKS);
            break;
         case RTT_PREPARE:
            value = microAverageLocal(RTT_PREPARE, NUM_RTTS_PREPARE);
            break;
         case RTT_COMMIT:
            value = microAverageLocal(RTT_COMMIT, NUM_RTTS_COMMIT);
            break;
         case RTT_ROLLBACK:
            value = microAverageLocal(RTT_ROLLBACK, NUM_RTTS_ROLLBACK);
            break;
         case RTT_GET:
            value = microAverageLocal(RTT_GET, NUM_RTTS_GET);
            break;
         case ASYNC_COMMIT:
            value = microAverageLocal(ASYNC_COMMIT, NUM_ASYNC_COMMIT);
            break;
         case ASYNC_COMPLETE_NOTIFY:
            value = microAverageLocal(ASYNC_COMPLETE_NOTIFY, NUM_ASYNC_COMPLETE_NOTIFY);
            break;
         case ASYNC_PREPARE:
            value = microAverageLocal(ASYNC_PREPARE, NUM_ASYNC_PREPARE);
            break;
         case ASYNC_ROLLBACK:
            value = microAverageLocal(ASYNC_ROLLBACK, NUM_ASYNC_ROLLBACK);
            break;
         case NUM_NODES_COMMIT:
            value = averageLocal(NUM_NODES_COMMIT, NUM_RTTS_COMMIT, NUM_ASYNC_COMMIT);
            break;
         case NUM_NODES_GET:
            value = averageLocal(NUM_NODES_GET, NUM_RTTS_GET);
            break;
         case NUM_NODES_PREPARE:
            value = averageLocal(NUM_NODES_PREPARE, NUM_RTTS_PREPARE, NUM_ASYNC_PREPARE);
            break;
         case NUM_NODES_ROLLBACK:
            value = averageLocal(NUM_NODES_ROLLBACK, NUM_RTTS_ROLLBACK, NUM_ASYNC_ROLLBACK);
            break;
         case NUM_NODES_COMPLETE_NOTIFY:
            value = averageLocal(NUM_NODES_COMPLETE_NOTIFY, NUM_ASYNC_COMPLETE_NOTIFY);
            break;
         case PUTS_PER_LOCAL_TX:
            value = averageLocal(NUM_SUCCESSFUL_PUTS, NUM_COMMITTED_WR_TX);
            break;
         case LOCAL_CONTENTION_PROBABILITY: {
            double numLocalPuts = localContainer.getValue(NUM_PUT);
            if (numLocalPuts != 0) {
               double numLocalLocalContention = localContainer.getValue(LOCK_CONTENTION_TO_LOCAL);
               double numLocalRemoteContention = localContainer.getValue(LOCK_CONTENTION_TO_REMOTE);
               value = (numLocalLocalContention + numLocalRemoteContention) / numLocalPuts;
            }
            break;
         }
         case REMOTE_CONTENTION_PROBABILITY: {
            double numRemotePuts = remoteContainer.getValue(NUM_PUT);
            if (numRemotePuts != 0) {
               double numRemoteLocalContention = remoteContainer.getValue(LOCK_CONTENTION_TO_LOCAL);
               double numRemoteRemoteContention = remoteContainer.getValue(LOCK_CONTENTION_TO_REMOTE);
               value = (numRemoteLocalContention + numRemoteRemoteContention) / numRemotePuts;
            }
            break;
         }
         case LOCK_CONTENTION_PROBABILITY: {
            double numLocalPuts = localContainer.getValue(NUM_PUT);
            double numRemotePuts = remoteContainer.getValue(NUM_PUT);
            double totalPuts = numLocalPuts + numRemotePuts;
            if (totalPuts != 0) {
               double localLocal = localContainer.getValue(LOCK_CONTENTION_TO_LOCAL);
               double localRemote = localContainer.getValue(LOCK_CONTENTION_TO_REMOTE);
               double remoteLocal = remoteContainer.getValue(LOCK_CONTENTION_TO_LOCAL);
               double remoteRemote = remoteContainer.getValue(LOCK_CONTENTION_TO_REMOTE);
               double totalCont = localLocal + localRemote + remoteLocal + remoteRemote;
               value = totalCont / totalPuts;
            }
            break;
         }
         case COMMIT_EXECUTION_TIME:
            value = convertNanosToMicro(averageLocal(COMMIT_EXECUTION_TIME, NUM_COMMITTED_WR_TX, NUM_COMMITTED_RO_TX));
            break;
         case ROLLBACK_EXECUTION_TIME:
            value = microAverageLocal(ROLLBACK_EXECUTION_TIME, NUM_ROLLBACKS);
            break;
         case LOCK_WAITING_TIME:
            value = microAverageLocalAndRemote(LOCK_WAITING_TIME, NUM_WAITED_FOR_LOCKS);
            break;
         case TX_WRITE_PERCENTAGE: {     //computed on the locally born txs
            double readTx = localContainer.getValue(NUM_COMMITTED_RO_TX) +
                  localContainer.getValue(NUM_ABORTED_RO_TX);
            double writeTx = localContainer.getValue(NUM_COMMITTED_WR_TX) +
                  localContainer.getValue(NUM_ABORTED_WR_TX);
            double total = readTx + writeTx;
            if (total != 0) {
               value = writeTx / total;
            }
            break;
         }
         case SUCCESSFUL_WRITE_PERCENTAGE: { //computed on the locally born txs
            double readSuxTx = localContainer.getValue(NUM_COMMITTED_RO_TX);
            double writeSuxTx = localContainer.getValue(NUM_COMMITTED_WR_TX);
            double total = readSuxTx + writeSuxTx;
            if (total != 0) {
               value = writeSuxTx / total;
            }
            break;
         }
         case APPLICATION_CONTENTION_FACTOR: {
            double localTakenLocks = localContainer.getValue(NUM_HELD_LOCKS);
            double remoteTakenLocks = remoteContainer.getValue(NUM_HELD_LOCKS);
            double totalLocksArrivalRate = (localTakenLocks + remoteTakenLocks) /
                  convertNanosToMicro(timeService.duration(lastResetTime));
            double holdTime = this.getAttribute(LOCK_HOLD_TIME);

            if ((totalLocksArrivalRate * holdTime) != 0) {
               double lockContProb = this.getAttribute(LOCK_CONTENTION_PROBABILITY);
               value = lockContProb / (totalLocksArrivalRate * holdTime);
            }
            break;
         }
         case NUM_SUCCESSFUL_GETS_RO_TX:
            value = averageLocal(NUM_COMMITTED_RO_TX, NUM_SUCCESSFUL_GETS_RO_TX);
            break;
         case NUM_SUCCESSFUL_GETS_WR_TX:
            value = averageLocal(NUM_COMMITTED_WR_TX, NUM_SUCCESSFUL_GETS_WR_TX);
            break;
         case NUM_SUCCESSFUL_REMOTE_GETS_RO_TX:
            value = averageLocal(NUM_COMMITTED_RO_TX, NUM_SUCCESSFUL_REMOTE_GETS_RO_TX);
            break;
         case NUM_SUCCESSFUL_REMOTE_GETS_WR_TX:
            value = averageLocal(NUM_COMMITTED_WR_TX, NUM_SUCCESSFUL_REMOTE_GETS_WR_TX);
            break;
         case REMOTE_GET_EXECUTION:
            value = microAverageLocal(REMOTE_GET_EXECUTION, NUM_REMOTE_GET);
            break;
         case NUM_SUCCESSFUL_PUTS_WR_TX:
            value = averageLocal(NUM_COMMITTED_WR_TX, NUM_SUCCESSFUL_PUTS_WR_TX);
            break;
         case NUM_SUCCESSFUL_REMOTE_PUTS_WR_TX:
            value = averageLocal(NUM_COMMITTED_WR_TX, NUM_SUCCESSFUL_REMOTE_PUTS_WR_TX);
            break;
         case REMOTE_PUT_EXECUTION:
            value = microAverageLocal(REMOTE_PUT_EXECUTION, NUM_REMOTE_PUT);
            break;
         case NUM_LOCK_FAILED_DEADLOCK:
         case NUM_LOCK_FAILED_TIMEOUT:
            value = localContainer.getValue(stat);
            break;
         case WR_TX_LOCAL_EXECUTION_TIME:
            value = microAverageLocal(WR_TX_LOCAL_EXECUTION_TIME, NUM_PREPARE_COMMAND);
            break;
         case WR_TX_SUCCESSFUL_EXECUTION_TIME:
            value = microAverageLocal(WR_TX_SUCCESSFUL_EXECUTION_TIME, NUM_COMMITTED_WR_TX);
            break;
         case WR_TX_ABORTED_EXECUTION_TIME:
            value = microAverageLocal(WR_TX_ABORTED_EXECUTION_TIME, NUM_ABORTED_WR_TX);
            break;
         case RO_TX_SUCCESSFUL_EXECUTION_TIME:
            value = microAverageLocal(RO_TX_SUCCESSFUL_EXECUTION_TIME, NUM_COMMITTED_RO_TX);
            break;
         case PREPARE_COMMAND_SIZE:
            value = averageLocal(PREPARE_COMMAND_SIZE, NUM_RTTS_PREPARE, NUM_ASYNC_PREPARE);
            break;
         case COMMIT_COMMAND_SIZE:
            value = averageLocal(COMMIT_COMMAND_SIZE, NUM_RTTS_COMMIT, NUM_ASYNC_COMMIT);
            break;
         case CLUSTERED_GET_COMMAND_SIZE:
            value = averageLocal(NUM_RTTS_GET, CLUSTERED_GET_COMMAND_SIZE);
            break;
         case NUM_LOCK_PER_LOCAL_TX:
            value = averageLocal(NUM_HELD_LOCKS, NUM_COMMITTED_WR_TX, NUM_ABORTED_WR_TX);
            break;
         case NUM_LOCK_PER_REMOTE_TX:
            value = averageRemote(NUM_HELD_LOCKS, NUM_COMMITTED_WR_TX, NUM_ABORTED_WR_TX);
            break;
         case NUM_LOCK_PER_SUCCESS_LOCAL_TX:
            value = averageLocal(NUM_COMMITTED_WR_TX, NUM_HELD_LOCKS_SUCCESS_TX);
            break;
         case LOCAL_ROLLBACK_EXECUTION_TIME:
            value = microAverageLocal(ROLLBACK_EXECUTION_TIME, NUM_ROLLBACKS);
            break;
         case REMOTE_ROLLBACK_EXECUTION_TIME:
            value = microAverageRemote(ROLLBACK_EXECUTION_TIME, NUM_ROLLBACKS);
            break;
         case LOCAL_COMMIT_EXECUTION_TIME:
            value = microAverageLocal(COMMIT_EXECUTION_TIME, NUM_COMMIT_COMMAND);
            break;
         case REMOTE_COMMIT_EXECUTION_TIME:
            value = microAverageRemote(COMMIT_EXECUTION_TIME, NUM_COMMIT_COMMAND);
            break;
         case LOCAL_PREPARE_EXECUTION_TIME:
            value = microAverageLocal(PREPARE_EXECUTION_TIME, NUM_PREPARE_COMMAND);
            break;
         case REMOTE_PREPARE_EXECUTION_TIME:
            value = microAverageRemote(PREPARE_EXECUTION_TIME, NUM_PREPARE_COMMAND);
            break;
         case TX_COMPLETE_NOTIFY_EXECUTION_TIME:
            value = microAverageRemote(TX_COMPLETE_NOTIFY_EXECUTION_TIME, NUM_TX_COMPLETE_NOTIFY_COMMAND);
            break;
         case ABORT_RATE:
            double totalAbort = localContainer.getValue(NUM_ABORTED_RO_TX) +
                  localContainer.getValue(NUM_ABORTED_WR_TX);
            double totalCommitAndAbort = localContainer.getValue(NUM_COMMITTED_RO_TX) +
                  localContainer.getValue(NUM_COMMITTED_WR_TX) + totalAbort;
            if (totalCommitAndAbort != 0) {
               value = totalAbort / totalCommitAndAbort;
            }
            break;
         case ARRIVAL_RATE:
            double localCommittedTx = localContainer.getValue(NUM_COMMITTED_RO_TX) +
                  localContainer.getValue(NUM_COMMITTED_WR_TX);
            double localAbortedTx = localContainer.getValue(NUM_ABORTED_RO_TX) +
                  localContainer.getValue(NUM_ABORTED_WR_TX);
            double remoteCommittedTx = remoteContainer.getValue(NUM_COMMITTED_RO_TX) +
                  remoteContainer.getValue(NUM_COMMITTED_WR_TX);
            double remoteAbortedTx = remoteContainer.getValue(NUM_ABORTED_RO_TX) +
                  remoteContainer.getValue(NUM_ABORTED_WR_TX);
            double totalBornTx = localAbortedTx + localCommittedTx + remoteAbortedTx + remoteCommittedTx;
            value = totalBornTx / convertNanosToSeconds(timeService.duration(lastResetTime));
            break;
         case THROUGHPUT:
            double totalLocalBornTx = localContainer.getValue(NUM_COMMITTED_RO_TX) +
                  localContainer.getValue(NUM_COMMITTED_WR_TX);
            value = totalLocalBornTx / convertNanosToSeconds(timeService.duration(lastResetTime));
            break;
         case LOCK_HOLD_TIME_LOCAL:
            value = microAverageLocal(LOCK_HOLD_TIME, NUM_HELD_LOCKS);
            break;
         case LOCK_HOLD_TIME_REMOTE:
            value = microAverageRemote(LOCK_HOLD_TIME, NUM_HELD_LOCKS);
            break;
         case NUM_COMMITS:
            value = localContainer.getValue(NUM_COMMITTED_RO_TX) + localContainer.getValue(NUM_COMMITTED_WR_TX) +
                  remoteContainer.getValue(NUM_COMMITTED_RO_TX) + remoteContainer.getValue(NUM_COMMITTED_WR_TX);
            break;
         case NUM_LOCAL_COMMITS:
            value = localContainer.getValue(NUM_COMMITTED_RO_TX) + localContainer.getValue(NUM_COMMITTED_WR_TX);
            break;
         case WRITE_SKEW_PROBABILITY:
            double totalTxs = localContainer.getValue(NUM_COMMITTED_RO_TX) +
                  localContainer.getValue(NUM_COMMITTED_WR_TX) +
                  localContainer.getValue(NUM_ABORTED_RO_TX) +
                  localContainer.getValue(NUM_ABORTED_WR_TX);
            if (totalTxs != 0) {
               double writeSkew = localContainer.getValue(NUM_WRITE_SKEW);
               value = writeSkew / totalTxs;
            }
            break;
         case NUM_GET:
            value = localContainer.getValue(NUM_SUCCESSFUL_GETS_WR_TX) +
                  localContainer.getValue(NUM_SUCCESSFUL_GETS_RO_TX);
            break;
         case NUM_REMOTE_GET:
            value = localContainer.getValue(NUM_SUCCESSFUL_REMOTE_GETS_WR_TX) +
                  localContainer.getValue(NUM_SUCCESSFUL_REMOTE_GETS_RO_TX);
            break;
         case NUM_PUT:
            value = localContainer.getValue(NUM_SUCCESSFUL_PUTS_WR_TX);
            break;
         case NUM_REMOTE_PUT:
            value = localContainer.getValue(NUM_SUCCESSFUL_REMOTE_PUTS_WR_TX);
            break;
         case LOCAL_GET_EXECUTION:
            double num = localContainer.getValue(NUM_GET);
            if (num != 0) {
               double local_get_time = localContainer.getValue(ALL_GET_EXECUTION) -
                     localContainer.getValue(RTT_GET);
               value = convertNanosToMicro(local_get_time) / num;
            }
            break;
         case RESPONSE_TIME:
            double succWrTot = convertNanosToMicro(localContainer.getValue(WR_TX_SUCCESSFUL_EXECUTION_TIME));
            double abortWrTot = convertNanosToMicro(localContainer.getValue(WR_TX_ABORTED_EXECUTION_TIME));
            double succRdTot = convertNanosToMicro(localContainer.getValue(RO_TX_SUCCESSFUL_EXECUTION_TIME));

            double numWr = localContainer.getValue(NUM_COMMITTED_WR_TX);
            double numRd = localContainer.getValue(NUM_COMMITTED_RO_TX);

            if ((numWr + numRd) > 0) {
               value = (succRdTot + succWrTot + abortWrTot) / (numWr + numRd);
            }
            break;
         default:
            if (log.isTraceEnabled()) {
               log.tracef("Attribute %s is not exposed via JMX. Calculating raw value", stat);
            }
            if (stat.isLocal()) {
               value += localContainer.getValue(stat);
            }
            if (stat.isRemote()) {
               value += remoteContainer.getValue(stat);
            }
      }
      if (log.isTraceEnabled()) {
         log.tracef("Get attribute %s = %s", stat, value);
      }
      return value;
   }

   /*
   Can I invoke this synchronized method from inside itself??
    */

   public static double convertNanosToMicro(double nanos) {
      return nanos / 1E3;
   }

   public static double convertNanosToSeconds(double nanos) {
      return nanos / 1E9;
   }

   private void doMerge(TransactionStatistics transactionStatistics, ExtendedStatisticsContainer container,
                        PercentileStats percentileStats, ExtendedStatistic percentileSample) {
      transactionStatistics.flushTo(container);
      try {
         percentileStats.insertSample(transactionStatistics.getValue(percentileSample));
      } catch (ExtendedStatisticNotFoundException e) {
         log.warnf("Extended Statistic not found while tried to add a percentile sample. %s", e.getLocalizedMessage());
      }
   }

   private double averageLocal(ExtendedStatistic numeratorStat, ExtendedStatistic... denominatorStats) throws ExtendedStatisticNotFoundException {
      double denominator = 0;
      for (ExtendedStatistic denominatorStat : denominatorStats) {
         denominator += localContainer.getValue(denominatorStat);
      }
      if (denominator != 0) {
         double numerator = localContainer.getValue(numeratorStat);
         return numerator / denominator;
      }
      return 0;
   }

   private double averageRemote(ExtendedStatistic numeratorStat, ExtendedStatistic... denominatorStats) throws ExtendedStatisticNotFoundException {
      double denominator = 0;
      for (ExtendedStatistic denominatorStat : denominatorStats) {
         denominator += remoteContainer.getValue(denominatorStat);
      }
      if (denominator != 0) {
         double numerator = remoteContainer.getValue(numeratorStat);
         return numerator / denominator;
      }
      return 0;
   }

   private double averageLocalAndRemote(ExtendedStatistic numeratorStat, ExtendedStatistic... denominatorStats) throws ExtendedStatisticNotFoundException {
      double denominator = 0;
      for (ExtendedStatistic denominatorStat : denominatorStats) {
         denominator += remoteContainer.getValue(denominatorStat);
         denominator += localContainer.getValue(denominatorStat);
      }
      if (denominator != 0) {
         double numerator = remoteContainer.getValue(numeratorStat);
         numerator += localContainer.getValue(numeratorStat);
         return numerator / denominator;
      }
      return 0;
   }

   private double microAverageLocal(ExtendedStatistic numerator, ExtendedStatistic denominator) throws ExtendedStatisticNotFoundException {
      return convertNanosToMicro(averageLocal(numerator, denominator));
   }

   private double microAverageRemote(ExtendedStatistic numerator, ExtendedStatistic denominator) throws ExtendedStatisticNotFoundException {
      return convertNanosToMicro(averageRemote(numerator, denominator));
   }

   private double microAverageLocalAndRemote(ExtendedStatistic numerator, ExtendedStatistic denominator) throws ExtendedStatisticNotFoundException {
      return convertNanosToMicro(averageLocalAndRemote(numerator, denominator));
   }

}
