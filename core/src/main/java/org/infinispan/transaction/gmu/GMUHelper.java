package org.infinispan.transaction.gmu;

import org.infinispan.CacheException;
import org.infinispan.commands.tx.GMUPrepareCommand;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.gmu.InternalGMUCacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.InequalVersionComparisonResult;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.container.versioning.gmu.GMUEntryVersion;
import org.infinispan.container.versioning.gmu.GMUVersionGenerator;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.gmu.ValidationException;
import org.infinispan.transaction.xa.GlobalTransaction;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class GMUHelper {

   public static void performReadSetValidation(GMUPrepareCommand prepareCommand,
                                               DataContainer dataContainer,
                                               ClusteringDependentLogic keyLogic) {
      EntryVersion prepareVersion = prepareCommand.getPrepareVersion();
      for (Object key : prepareCommand.getReadSet()) {
         if (keyLogic.localNodeIsOwner(key)) {
            InternalCacheEntry cacheEntry = dataContainer.get(key, null); //get the most recent
            EntryVersion currentVersion = cacheEntry.getVersion();
            if (currentVersion == null) {
               //this should only happens if the key does not exits. However, this can create some
               //consistency issues when eviction is enabled
               continue;
            }
            if (currentVersion.compareTo(prepareVersion) == InequalVersionComparisonResult.AFTER) {
               throw new ValidationException("Validation failed for key [" + key + "]", key);
            }
         }
      }
   }

   public static EntryVersion calculateCommitVersion(EntryVersion localPrepareVersion, EntryVersion remotePrepareVersion,
                                                     GMUVersionGenerator versionGenerator, Collection<Address> affectedOwners) {
      EntryVersion mergedVersion;
      if (localPrepareVersion == null) {
         mergedVersion = remotePrepareVersion;
      } else if (remotePrepareVersion == null) {
         mergedVersion = localPrepareVersion;
      } else {
         mergedVersion = versionGenerator.mergeAndMax(Arrays.asList(localPrepareVersion, remotePrepareVersion));
      }

      if (mergedVersion == null) {
         throw new NullPointerException("Null merged version is not allowed to calculate commit view");
      }

      return versionGenerator.calculateCommitVersion(mergedVersion, affectedOwners);
   }

   public static InternalGMUCacheEntry toInternalGMUCacheEntry(InternalCacheEntry entry) {
      return convert(entry, InternalGMUCacheEntry.class);
   }

   public static GMUEntryVersion toGMUEntryVersion(EntryVersion version) {
      return convert(version, GMUEntryVersion.class);
   }

   public static GMUVersionGenerator toGMUVersionGenerator(VersionGenerator versionGenerator) {
      return convert(versionGenerator, GMUVersionGenerator.class);
   }

   public static <T> T convert(Object object, Class<T> clazz) {
      try {
         return clazz.cast(object);
      } catch (ClassCastException cce) {
         throw new IllegalArgumentException("Expected " + clazz.getSimpleName() +
                                                  " and not " + object.getClass().getSimpleName());
      }
   }

   public static void joinAndSetTransactionVersion(Collection<Response> responses, TxInvocationContext ctx,
                                                   GMUVersionGenerator versionGenerator) {
      if (responses.isEmpty()) {
         throw new IllegalStateException("New Versions are expected from other nodes");
      }
      List<EntryVersion> allPreparedVersions = new LinkedList<EntryVersion>();
      allPreparedVersions.add(ctx.getTransactionVersion());
      GlobalTransaction gtx = ctx.getGlobalTransaction();

      //process all responses
      for (Response r : responses) {
         if (r instanceof SuccessfulResponse) {
            EntryVersion version = convert(((SuccessfulResponse) r).getResponseValue(), EntryVersion.class);
            allPreparedVersions.add(version);
         } else if(r instanceof ExceptionResponse) {
            throw new ValidationException(((ExceptionResponse) r).getException());
         } else if(!r.isSuccessful()) {
            throw new CacheException("Unsuccessful response received... aborting transaction " + gtx.prettyPrint());
         }
      }

      ctx.setTransactionVersion(versionGenerator.mergeAndMax(allPreparedVersions));
   }
}
