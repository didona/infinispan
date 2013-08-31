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
package org.infinispan.stats.topK;

import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.interceptors.base.BaseCustomInterceptor;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.jmx.annotations.Parameter;
import org.infinispan.transaction.WriteSkewException;

import java.util.Map;

/**
 * @author Pedro Ruivo
 * @since 5.2
 */
@MBean(objectName = "StreamLibStatistics", description = "Show analytics for workload monitor")
public class StreamLibInterceptor extends BaseCustomInterceptor {

   private static final int DEFAULT_TOP_KEY = 10;
   private StreamLibContainer streamLibContainer;
   private boolean statisticEnabled = false;
   private DistributionManager distributionManager;

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {

      if (statisticEnabled && ctx.isOriginLocal()) {
         streamLibContainer.addGet(command.getKey(), isRemote(command.getKey()));
      }
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      try {
         if (statisticEnabled && ctx.isOriginLocal()) {
            streamLibContainer.addPut(command.getKey(), isRemote(command.getKey()));
         }
         return invokeNextInterceptor(ctx, command);
      } catch (WriteSkewException wse) {
         Object key = wse.getKey();
         if (key != null && ctx.isOriginLocal()) {
            streamLibContainer.addWriteSkewFailed(key);
         }
         throw wse;
      }
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      try {
         return invokeNextInterceptor(ctx, command);
      } catch (WriteSkewException wse) {
         Object key = wse.getKey();
         if (key != null && ctx.isOriginLocal()) {
            streamLibContainer.addWriteSkewFailed(key);
         }
         throw wse;
      } finally {
         streamLibContainer.tryFlushAll();
      }
   }

   @ManagedOperation(description = "Resets statistics gathered by this component",
                     displayName = "Reset Statistics (Statistics)")
   public void resetStatistics() {
      streamLibContainer.resetAll();
   }

   @ManagedOperation(description = "Set K for the top-K values",
                     displayName = "Set K")
   public void setTopKValue(@Parameter(name = "Top-K", description = "top-Kth to return") int value) {
      streamLibContainer.setCapacity(value);
   }


   @ManagedOperation(description = "Show the top n keys whose write skew check was failed",
                     displayName = "Top Keys whose Write Skew Check was failed")
   public void setStatisticsEnabled(@Parameter(name = "Enabled?") boolean enabled) {
      statisticEnabled = enabled;
      streamLibContainer.setActive(enabled);
   }

   @Override
   protected void start() {
      super.start();
      this.distributionManager = cache.getAdvancedCache().getDistributionManager();
      this.streamLibContainer = StreamLibContainer.getOrCreateStreamLibContainer(cache);
      setStatisticsEnabled(true);
   }

   private boolean isRemote(Object k) {
      return distributionManager != null && !distributionManager.getLocality(k).isLocal();
   }
}
