package org.infinispan.dataplacement.c50.tree;

import org.infinispan.dataplacement.c50.keyfeature.Feature;
import org.infinispan.dataplacement.c50.keyfeature.FeatureValue;
import org.infinispan.dataplacement.c50.tree.node.DecisionTreeNode;

import java.util.Map;

/**
 * Represents a decision tree in which you can query based on the values of some attributes and returns the new owner
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class DecisionTree {

   private final DecisionTreeNode root;

   public DecisionTree(DecisionTreeNode root) {
      this.root = root;
   }

   /**
    * queries the decision tree looking for the value depending of the features value
    *
    * @param keyFeature the feature values
    * @return           the index of the new owner
    */
   public final int query(Map<Feature, FeatureValue> keyFeature) {
      DecisionTreeNode node = root.find(keyFeature);
      DecisionTreeNode result = node;

      while (node != null) {
         result = node;
         node = node.find(keyFeature);
      }

      if (result == null) {
         throw new IllegalStateException("Expected to find a decision");
      }

      return result.getValue();
   }
}
