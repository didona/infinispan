package org.infinispan.api.tree;

import org.infinispan.Cache;
import org.infinispan.atomic.AtomicMap;
import org.infinispan.atomic.AtomicMapLookup;
import org.infinispan.config.Configuration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TransactionSetup;
import org.infinispan.tree.Fqn;
import org.infinispan.tree.Node;
import org.infinispan.tree.NodeKey;
import org.infinispan.tree.TreeCache;
import org.infinispan.tree.TreeCacheImpl;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import static org.testng.AssertJUnit.*;

import org.testng.annotations.Test;

import javax.transaction.TransactionManager;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Tests the {@link TreeCache} public API at a high level
 *
 * @author <a href="mailto:manik AT jboss DOT org">Manik Surtani</a>
 */

@Test(groups = "functional", testName = "api.tree.TreeCacheAPITest")
public class TreeCacheAPITest extends SingleCacheManagerTest {
   private TreeCache<String, String> cache;
   private TransactionManager tm;
   private Log log = LogFactory.getLog(TreeCacheAPITest.class);

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      // start a single cache instance
      Configuration c = new Configuration();
      c.setInvocationBatchingEnabled(true);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(c, true);

      Cache flatcache = cm.getCache();
      cache = new TreeCacheImpl(flatcache);

      tm = TestingUtil.getTransactionManager(flatcache);
      return cm;
   }

   public void testConvenienceMethods() {
      Fqn fqn = Fqn.fromString("/test/fqn");
      String key = "key", value = "value";
      Map<String, String> data = new HashMap<String, String>();
      data.put(key, value);

      assertNull(cache.get(fqn, key));

      cache.put(fqn, key, value);

      assertEquals(value, cache.get(fqn, key));

      cache.remove(fqn, key);

      assertNull(cache.get(fqn, key));

      cache.put(fqn, data);

      assertEquals(value, cache.get(fqn, key));
   }


   /**
    * Another convenience method that tests node removal
    */
   public void testNodeConvenienceNodeRemoval() {
      // this fqn is relative, but since it is from the root it may as well be absolute
      Fqn fqn = Fqn.fromString("/test/fqn");
      cache.getRoot().addChild(fqn);
      assertTrue(cache.getRoot().hasChild(fqn));

      assertEquals(true, cache.removeNode(fqn));
      assertFalse(cache.getRoot().hasChild(fqn));
      // remove should REALLY remove though and not just mark as deleted/invalid.
      Node n = cache.getNode(fqn);
      assert n == null;

      assertEquals(false, cache.removeNode(fqn));

      // remove should REALLY remove though and not just mark as deleted/invalid.
      n = cache.getNode(fqn);
      assert n == null;

      // Check that it's removed if it has a child
      Fqn child = Fqn.fromString("/test/fqn/child");
      log.error("TEST: Adding child " + child);
      cache.getRoot().addChild(child);
      assertStructure(cache, "/test/fqn/child");

      assertEquals(true, cache.removeNode(fqn));
      assertFalse(cache.getRoot().hasChild(fqn));
      assertEquals(false, cache.removeNode(fqn));
   }

   private void assertStructure(TreeCache tc, String fqnStr) {
      // make sure structure nodes are properly built and maintained
      Cache c = tc.getCache();
      Fqn fqn = Fqn.fromString(fqnStr);
      // loop thru the Fqn, starting at its root, and make sure all of its children exist in proper NodeKeys
      for (int i = 0; i < fqn.size(); i++) {
         Fqn parent = fqn.getSubFqn(0, i);
         Object childName = fqn.get(i);
         // make sure a data key exists in the cache
         assert c.containsKey(new NodeKey(parent, NodeKey.Type.DATA)) : "Node [" + parent + "] does not have a Data atomic map!";
         assert c.containsKey(new NodeKey(parent, NodeKey.Type.STRUCTURE)) : "Node [" + parent + "] does not have a Structure atomic map!";
         AtomicMap<Object, Fqn> am = AtomicMapLookup.getAtomicMap(c, new NodeKey(parent, NodeKey.Type.STRUCTURE));
         boolean hasChild = am.containsKey(childName);
         assert hasChild : "Node [" + parent + "] does not have a child [" + childName + "] in its Structure atomic map!";
      }
   }


   public void testStopClearsData() throws Exception {
      Fqn a = Fqn.fromString("/a");
      Fqn b = Fqn.fromString("/a/b");
      String key = "key", value = "value";
      cache.getRoot().addChild(a).put(key, value);
      cache.getRoot().addChild(b).put(key, value);
      cache.getRoot().put(key, value);

      assertEquals(value, cache.getRoot().get(key));
      assertEquals(value, cache.getRoot().getChild(a).get(key));
      assertEquals(value, cache.getRoot().getChild(b).get(key));

      cache.stop();

      cache.start();

      assertNull(cache.getRoot().get(key));
      assertTrue(cache.getRoot().getData().isEmpty());
      assertTrue(cache.getRoot().getChildren().isEmpty());
   }

   public void testPhantomStructuralNodesOnRemove() {
      assert cache.getNode(Fqn.fromString("/a/b/c")) == null;
      assert !cache.removeNode("/a/b/c");
      assert cache.getNode(Fqn.fromString("/a/b/c")) == null;
      assert cache.getNode(Fqn.fromString("/a/b")) == null;
      assert cache.getNode(Fqn.fromString("/a")) == null;
   }

   public void testPhantomStructuralNodesOnRemoveTransactional() throws Exception {
      assert cache.getNode(Fqn.fromString("/a/b/c")) == null;
      tm.begin();
      assert !cache.removeNode("/a/b/c");
      tm.commit();
      assert cache.getNode(Fqn.fromString("/a/b/c")) == null;
      assert cache.getNode(Fqn.fromString("/a/b")) == null;
      assert cache.getNode(Fqn.fromString("/a")) == null;
   }

   public void testRpcManagerElements() {
      assertEquals("CacheMode.LOCAL cache has no address", null, manager(cache.getCache()).getAddress());
      assertEquals("CacheMode.LOCAL cache has no members list", null, manager(cache.getCache()).getMembers());
   }
}
