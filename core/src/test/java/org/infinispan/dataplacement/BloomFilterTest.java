package org.infinispan.dataplacement;

import org.infinispan.dataplacement.c50.lookup.BloomFilter;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(groups = "functional", testName = "dataplacement.BloomFilterTest")
public class BloomFilterTest {

   private static final Log log = LogFactory.getLog(BloomFilterTest.class);

   private static final int START = 10;
   private static final int END = 10000000;
   private static final double PROB = 0.001;

   public void testBloomFilter() {

      for (int iteration = START; iteration <= END; iteration *= 10) {
         BloomFilter bloomFilter = new BloomFilter(PROB, iteration);
         for (int key = 0; key < iteration; ++key) {
            bloomFilter.add(getKey(key));
         }

         log.infof("=== Iterator with %s objects ===", iteration);
         log.infof("Bloom filter size = %s", bloomFilter.size());

         long begin = System.currentTimeMillis();
         for (int key = 0; key < iteration; ++key) {
            assert bloomFilter.contains(getKey(key)) : "False Negative should not happen!";
         }
         long end = System.currentTimeMillis();

         log.infof("Query duration:\n\ttotal=%s ms\n\tper-key=%s ms", end - begin, (end - begin) / iteration);
      }

   }

   public void testSerializable() throws IOException, ClassNotFoundException {
      int numberOfKeys = 1000;
      BloomFilter bloomFilter = new BloomFilter(PROB, numberOfKeys);

      for (int i = 0; i < numberOfKeys; ++i) {
         bloomFilter.add(getKey(i));
      }

      ByteArrayOutputStream arrayOutputStream = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(arrayOutputStream);

      oos.writeObject(bloomFilter);
      oos.flush();
      oos.close();

      byte[] bytes = arrayOutputStream.toByteArray();

      log.infof("Bloom filter size with %s keys is %s bytes", numberOfKeys, bytes.length);

      ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes));

      BloomFilter readBloomFilter = (BloomFilter) ois.readObject();
      ois.close();

      for (int i = 0; i < numberOfKeys; ++i) {
         assert bloomFilter.contains(getKey(i)) : "False negative should never happen. It happened in original " +
               "Bloom Filter";
         assert readBloomFilter.contains(getKey(i)) : "False negative should never happen. It happened in read " +
               "Bloom Filter";
      }
   }

   private String getKey(int index) {
      return "KEY_" + index;
   }

}
