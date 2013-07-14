/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
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
package org.infinispan.remoting;

import org.infinispan.CacheException;
import org.infinispan.commands.CancellableCommand;
import org.infinispan.commands.CancellationService;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.remote.ConfigurationStateCommand;
import org.infinispan.commands.remote.GMUClusteredGetCommand;
import org.infinispan.commands.tx.GMUCommitCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.tx.totalorder.TotalOrderPrepareCommand;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.interceptors.totalorder.RetryPrepareException;
import org.infinispan.manager.NamedCacheNotFoundException;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.ResponseGenerator;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.stats.TransactionsStatisticsRegistry;
import org.infinispan.stats.translations.ExposedStatistics;
import org.infinispan.transaction.TotalOrderRemoteTransactionState;
import org.infinispan.transaction.totalorder.TotalOrderLatch;
import org.infinispan.transaction.totalorder.TotalOrderManager;
import org.infinispan.util.concurrent.BlockingRunnable;
import org.infinispan.util.concurrent.BlockingTaskAwareExecutorService;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.util.Buffer;

/**
 * Sets the cache interceptor chain on an RPCCommand before calling it to perform
 *
 * @author Manik Surtani
 * @since 4.0
 */
@Scope(Scopes.GLOBAL)
public class InboundInvocationHandlerImpl implements InboundInvocationHandler {
   private GlobalComponentRegistry gcr;
   private static final Log log = LogFactory.getLog(InboundInvocationHandlerImpl.class);
   private static final boolean trace = log.isTraceEnabled();
   private GlobalConfiguration globalConfiguration;
   private Transport transport;
   private CancellationService cancelService;
   private BlockingTaskAwareExecutorService totalOrderExecutorService;
   private BlockingTaskAwareExecutorService gmuExecutorService;

   private RpcDispatcher.Marshaller marshaller = null;


   @Inject
   public void inject(GlobalComponentRegistry gcr, Transport transport,
                      @ComponentName(KnownComponentNames.TOTAL_ORDER_EXECUTOR) BlockingTaskAwareExecutorService totalOrderExecutorService,
                      @ComponentName(KnownComponentNames.GMU_EXECUTOR) BlockingTaskAwareExecutorService gmuExecutorService,
                      GlobalConfiguration globalConfiguration, CancellationService cancelService) {
      this.gcr = gcr;
      this.transport = transport;
      this.globalConfiguration = globalConfiguration;
      this.cancelService = cancelService;
      this.totalOrderExecutorService = totalOrderExecutorService;
      this.gmuExecutorService = gmuExecutorService;
   }

   @Override
   public void handle(final CacheRpcCommand cmd, Address origin, org.jgroups.blocks.Response response) throws Throwable {
      cmd.setOrigin(origin);

      String cacheName = cmd.getCacheName();
      ComponentRegistry cr = gcr.getNamedComponentRegistry(cacheName);

      if (cr == null) {
         if (!globalConfiguration.transport().strictPeerToPeer() || cmd instanceof ConfigurationStateCommand) {
            if (trace)
               log.tracef("Strict peer to peer off, so silently ignoring that %s cache is not defined", cacheName);
            reply(response, null);
            return;
         }

         log.namedCacheDoesNotExist(cacheName);
         Response retVal = new ExceptionResponse(new NamedCacheNotFoundException(cacheName, "Cache has not been started on node " + transport.getAddress()));
         reply(response, retVal);
         return;
      }

      handleWithWaitForBlocks(cmd, cr, response);
   }


   private Response handleInternal(final CacheRpcCommand cmd, final ComponentRegistry cr) throws Throwable {
      try {
         if (trace) log.tracef("Calling perform() on %s", cmd);
         ResponseGenerator respGen = cr.getResponseGenerator();
         if (cmd instanceof CancellableCommand) {
            cancelService.register(Thread.currentThread(), ((CancellableCommand) cmd).getUUID());
         }
         Object retval = cmd.perform(null);
         Response response = respGen.getResponse(cmd, retval);
         log.tracef("About to send back response %s for command %s", response, cmd);
         return response;
      } catch (Exception e) {
         log.error("Exception executing command", e);
         return new ExceptionResponse(e);
      } finally {
         if (cmd instanceof CancellableCommand) {
            cancelService.unregister(((CancellableCommand) cmd).getUUID());
         }
      }
   }

   private void handleWithWaitForBlocks(final CacheRpcCommand cmd, final ComponentRegistry cr, final org.jgroups.blocks.Response response) throws Throwable {
      StateTransferManager stm = cr.getStateTransferManager();
      // We must have completed the join before handling commands
      // (even if we didn't complete the initial state transfer)
      if (!stm.isJoinComplete()) {
         reply(response, null);
         return;
      } else if (cmd instanceof TotalOrderPrepareCommand && !stm.ownsData()) {
         reply(response, null);
         return;
      }

      CommandsFactory commandsFactory = cr.getCommandsFactory();
      // initialize this command with components specific to the intended cache instance
      commandsFactory.initializeReplicableCommand(cmd, true);
      final long arrivalTime = System.nanoTime();
      if (cmd instanceof TotalOrderPrepareCommand) {
         final TotalOrderRemoteTransactionState state = ((TotalOrderPrepareCommand) cmd).getOrCreateState();
         final TotalOrderManager totalOrderManager = cr.getTotalOrderManager();
         totalOrderManager.ensureOrder(state, ((TotalOrderPrepareCommand) cmd).getKeysToLock());
         totalOrderExecutorService.execute(new BlockingRunnable() {
            @Override
            public boolean isReady() {
               for (TotalOrderLatch block : state.getConflictingTransactionBlocks()) {
                  if (block.isBlocked()) {
                     return false;
                  }
               }
               return true;
            }

            @Override
            public void run() {
               Response resp;
               try {
                  resp = handleInternal(cmd, cr);
               } catch (RetryPrepareException retry) {
                  log.debugf(retry, "Prepare [%s] conflicted with state transfer", cmd);
                  resp = new ExceptionResponse(retry);
               } catch (Throwable throwable) {
                  log.exceptionHandlingCommand(cmd, throwable);
                  resp = new ExceptionResponse(new CacheException("Problems invoking command.", throwable));
               }
               if (resp instanceof ExceptionResponse) {
                  totalOrderManager.release(state);
               }
               //the ResponseGenerated is null in this case because the return value is a Response
               reply(response, resp);
            }
         });
         return;
      } else if (cmd instanceof GMUClusteredGetCommand) {
         final GMUClusteredGetCommand gmuClusteredGetCommand = (GMUClusteredGetCommand) cmd;
         gmuClusteredGetCommand.init();
         final boolean hasWaited = !gmuClusteredGetCommand.isReady();

         gmuExecutorService.execute(new BlockingRunnable() {
            @Override
            public boolean isReady() {
               return gmuClusteredGetCommand.isReady();
            }

            @Override
            public void run() {
               Response resp;
               boolean sampleServiceTimes = TransactionsStatisticsRegistry.isSampleServiceTime();
               try {
                  long cpuInitTime = 0, initTime = 0;
                  if (sampleServiceTimes) {
                     //No xact is associated to this command, so we do not attach a TransactionStatistics and rely on the flushes
                     cpuInitTime = TransactionsStatisticsRegistry.getCurrentThreadCpuTime();
                     initTime = System.nanoTime();
                     if (hasWaited) {
                        TransactionsStatisticsRegistry.addValueAndFlushIfNeeded(ExposedStatistics.IspnStats.REMOTE_REMOTE_GET_WAITING_TIME, initTime - arrivalTime, false);
                        TransactionsStatisticsRegistry.incrementValueAndFlushIfNeeded(ExposedStatistics.IspnStats.NUM_WAITS_REMOTE_REMOTE_GETS, false);
                     }
                  }
                  resp = handleInternal(cmd, cr);
                  if (sampleServiceTimes) {
                     TransactionsStatisticsRegistry.incrementValueAndFlushIfNeeded(ExposedStatistics.IspnStats.NUM_REMOTE_REMOTE_GETS, false);
                     TransactionsStatisticsRegistry.addValueAndFlushIfNeeded(ExposedStatistics.IspnStats.REMOTE_REMOTE_GET_R, System.nanoTime() - initTime, false);
                     TransactionsStatisticsRegistry.addValueAndFlushIfNeeded(ExposedStatistics.IspnStats.REMOTE_REMOTE_GET_S, TransactionsStatisticsRegistry.getCurrentThreadCpuTime() - cpuInitTime, false);
                  }
               } catch (Throwable throwable) {
                  log.exceptionHandlingCommand(cmd, throwable);
                  resp = new ExceptionResponse(new CacheException("Problems invoking command.", throwable));
               }
               //the ResponseGenerated is null in this case because the return value is a Response
               reply(response, resp);
               if (sampleServiceTimes) {
                  TransactionsStatisticsRegistry.addValueAndFlushIfNeeded(ExposedStatistics.IspnStats.REMOTE_REMOTE_GET_REPLY_SIZE, getReplySize(resp), false);
               }
            }
         });
         return;
      } else if (cmd instanceof GMUCommitCommand) {
         final GMUCommitCommand gmuCommitCommand = (GMUCommitCommand) cmd;
         gmuCommitCommand.init();
         final boolean hasWaited = !gmuCommitCommand.isReady();

         gmuExecutorService.execute(new BlockingRunnable() {
            @Override
            public boolean isReady() {
               return gmuCommitCommand.isReady();
            }
            //TODO service and response time of remote commits?
            @Override
            public void run() {
               Response resp;
               try {
                  //RemoteTransactionStatistic has been attached and detached by the IIHW, so it has to be attached again to this thread
                  if (TransactionsStatisticsRegistry.attachRemoteTransactionStatistic(gmuCommitCommand.getGlobalTransaction(),true) && hasWaited) {
                     TransactionsStatisticsRegistry.addValue(ExposedStatistics.IspnStats.WAIT_TIME_IN_COMMIT_QUEUE, System.nanoTime() - arrivalTime);
                     TransactionsStatisticsRegistry.incrementValue(ExposedStatistics.IspnStats.NUM_WAITS_IN_COMMIT_QUEUE);
                  }
                  resp = handleInternal(cmd, cr);
               } catch (Throwable throwable) {
                  log.exceptionHandlingCommand(cmd, throwable);
                  resp = new ExceptionResponse(new CacheException("Problems invoking command.", throwable));
               }
               //the ResponseGenerated is null in this case because the return value is a Response
               reply(response, resp);
               //Before the gmuExecutorService continues handling other stuff, we have to detach the current xact.
               TransactionsStatisticsRegistry.detachRemoteTransactionStatistic(gmuCommitCommand.getGlobalTransaction(),true);
               gmuExecutorService.checkForReadyTasks();
            }
         });
         gmuExecutorService.checkForReadyTasks();
         return;
      }
      Response resp = handleInternal(cmd, cr);

      // A null response is valid and OK ...
      if (trace && resp != null && !resp.isValid()) {
         // invalid response
         log.tracef("Unable to execute command, got invalid response %s", resp);
      }
      reply(response, resp);
      if (cr.getComponent(Configuration.class).locking().isolationLevel() == IsolationLevel.SERIALIZABLE &&
            cmd instanceof RollbackCommand) {
         gmuExecutorService.checkForReadyTasks();
      }
   }

   private void reply(org.jgroups.blocks.Response response, Object retVal) {
      if (response != null) {
         response.send(retVal, false);
      }
   }

   private int getReplySize(Response r) {
      try {
         if (marshaller == null && transport instanceof JGroupsTransport) {
            marshaller = ((JGroupsTransport) transport).getCommandAwareRpcDispatcher().getMarshaller();
         }
         Buffer buffer = marshaller.objectToBuffer(r);
         return buffer != null ? buffer.getLength() : 0;
      } catch (Exception e) {
         return 0;
      }
   }

}

