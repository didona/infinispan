package org.infinispan.tx.totalorder;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.TransactionProtocol;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertTrue;

/**
 * @author mircea.markus@jboss.com
 * @since 5.2.0
 */
@Test (groups = "functional", testName = "tx.totalorder.SimpleTotalOrder2PhaseTest")
public class SimpleTotalOrder2PhaseTest extends SimpleTotalOrderOnePhaseTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder dcc = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      dcc.transaction().transactionProtocol(TransactionProtocol.TOTAL_ORDER);
      dcc.locking().isolationLevel(IsolationLevel.REPEATABLE_READ).writeSkewCheck(true)
            .versioning().enable().scheme(VersioningScheme.SIMPLE);
      createCluster(dcc, 2);
      waitForClusterToForm();
   }

   public void testRequiresVersioning() {
      assert cache(0).getConfiguration().isRequireVersioning();
   }
}
