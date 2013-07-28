package org.infinispan.stats;

import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.marshall.Ids;
import org.infinispan.remoting.responses.AbstractResponse;
import org.infinispan.util.Util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

/**
 * // TODO: Document this
 *
 * @author diego
 * @since 4.0
 */
public class PiggyBackStat {

   private long TOPrepareWaitingTime;

   public long getTOPrepareWaitingTime() {
      return TOPrepareWaitingTime;
   }

   public void setTOPrepareWaitingTime(long TOPrepareWaitingTime) {
      this.TOPrepareWaitingTime = TOPrepareWaitingTime;
   }

   public PiggyBackStat(long TOPrepareWaitingTime) {
      this.TOPrepareWaitingTime = TOPrepareWaitingTime;
   }

   public static class Externalizer extends AbstractExternalizer<PiggyBackStat> {
      @Override
      public void writeObject(ObjectOutput output, PiggyBackStat piggyBackStat) throws IOException{
         output.writeLong(piggyBackStat.TOPrepareWaitingTime);
      }

      @Override
      public PiggyBackStat readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new PiggyBackStat(input.readLong());
      }

      @Override
      public Integer getId() {
         return Ids.PIGGY_BACK_RESPONSE;
      }

      @Override
      public Set<Class<? extends PiggyBackStat>> getTypeClasses() {
         return Util.<Class<? extends PiggyBackStat>>asSet(PiggyBackStat.class);
      }
   }
}
