package org.infinispan.distribution.wrappers;

import org.infinispan.stats.ExposedStatistic;
import org.infinispan.stats.container.LocalTransactionStatistics;
import org.infinispan.stats.container.RemoteTransactionStatistics;
import org.infinispan.stats.container.TransactionStatistics;
import xml.DXmlParser;

/**
 * @author Diego Didona
 * @email didona@gsd.inesc-id.pt
 */
public class StatHistogramContainer {

   private final static String rtt = "conf/rtt_histo.xml";
   private final static String ack = "conf/ack_histo.xml";
   private final static String lcq = "conf/lcq_histo.xml";
   private final Histogram rttHistogram;
   private final Histogram remoteAckToCommitHistogram;
   private final Histogram localCommitWaitTimeHistogram;

   public StatHistogramContainer() {
      DXmlParser<Histogram> parser = new DXmlParser<Histogram>();
      rttHistogram = parser.parse(rtt);
      rttHistogram.initBuckets();
      remoteAckToCommitHistogram = parser.parse(ack);
      remoteAckToCommitHistogram.initBuckets();
      localCommitWaitTimeHistogram = parser.parse(lcq);
      localCommitWaitTimeHistogram.initBuckets();
   }

   public void addSample(TransactionStatistics t) {
      final boolean isCommitUpdate = t.isCommit() && !t.isReadOnly();
      if (t instanceof LocalTransactionStatistics) {
         if (isCommitUpdate) {
            addLocalSample((LocalTransactionStatistics) t);
         }
      } else {
         if (isCommitUpdate) {
            addRemoteSample((RemoteTransactionStatistics) t);
         }
      }
   }

   private void addLocalSample(LocalTransactionStatistics lt) {
      rttHistogram.insertSample(nanoToMicro(lt.getValue(ExposedStatistic.RTT_PREPARE)));
      long waitedTime = lt.getValue(ExposedStatistic.WAIT_TIME_IN_COMMIT_QUEUE);
      if (waitedTime > 0)
         localCommitWaitTimeHistogram.insertSample(nanoToMicro(waitedTime));
   }

   private void addRemoteSample(RemoteTransactionStatistics rt) {
      remoteAckToCommitHistogram.insertSample(nanoToMicro((rt.getValue(ExposedStatistic.REMOTE_TIME_BETWEEN_ACK_AND_COMMIT))));
   }

   public void dump() {
      rttHistogram.dumpHistogram();
      remoteAckToCommitHistogram.dumpHistogram();
   }

   private double nanoToMicro(double d) {
      return d * 1e-3;
   }
}
