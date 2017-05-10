package org.gradoop.flink.algorithms.fsm.cross_level.vector_mining;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.ArrayUtils;
import org.gradoop.common.util.IntArrayUtils;
import org.gradoop.flink.model.impl.tuples.WithCount;

import java.util.Arrays;
import java.util.Collection;

public class CrossLevelFrequentVectorsTopDown extends CrossLevelFrequentVectorsBase {

  protected final CrossLevelVectorComparator comparator = new CrossLevelVectorComparator();

  @Override
  public Collection<WithCount<int[][]>> mine(int[][][] vectors, int minFrequency) {
    extractSchema(vectors);

    Arrays.sort(vectors, comparator);

    SearchTreeNode root = getRoot(vectors);

    Collection<WithCount<int[][]>> frequentPatterns = Lists.newArrayList();
    frequentPatterns.add(new WithCount<int[][]>(root.getPattern(), root.getFrequency()));

    Collection<SearchTreeNode> dimParents = Lists.newArrayList();
    Collection<SearchTreeNode> levelParents;


    for (int dim = 0; dim < dimCount; dim++) {
      Collection<SearchTreeNode> dimChildren = Lists.newArrayList();
      dimParents.add(root);
      levelParents = dimParents;

      for (int level = 0; level < schema[dim]; level++) {
        Collection<SearchTreeNode> levelChildren = Lists.newArrayList();

        for (SearchTreeNode parent : levelParents) {
          int[][] pattern = IntArrayUtils.deepCopy(parent.getPattern());

          int lastValue = -1;
          int[] occurrences = new int[0];

          for (int vectorIdx : parent.getOccurrences()) {
            int value = vectors[vectorIdx][dim][level];

            if (value > lastValue) {
              if (occurrences.length >= minFrequency) {
                pattern = IntArrayUtils.deepCopy(pattern);
                pattern[dim][level] = value;

                levelChildren.add(new SearchTreeNode(pattern, occurrences));
                frequentPatterns.add(new WithCount<>(pattern, occurrences.length));
              }

              lastValue = value;
              occurrences = new int[] {vectorIdx};
            } else {
              occurrences = ArrayUtils.add(occurrences, vectorIdx);
            }
          }

          if (occurrences.length >= minFrequency) {
            pattern = IntArrayUtils.deepCopy(pattern);
            pattern[dim][level] = lastValue;

            levelChildren.add(new SearchTreeNode(pattern, occurrences));
            frequentPatterns.add(new WithCount<>(pattern, occurrences.length));
          }
        }

        dimChildren.addAll(levelChildren);
        levelParents = levelChildren;
      }

      dimParents = dimChildren;
    }

    for (WithCount<int[][]> fp : frequentPatterns) {
      System.out.println(IntArrayUtils.toString(fp.getObject()));
    }

    return frequentPatterns;
  }

  private SearchTreeNode getRoot(int[][][] vectors) {

    int[] occurrences = new int[vectors.length];

    for (int i = 0; i < vectors.length; i++) {
      occurrences[i] = i;
    }

    int[][] rootVector = new int[dimCount][];

    for (int dim = 0; dim < dimCount; dim++) {
      int[] value = new int[schema[dim]];

      for (int level = 0; level < schema[dim]; level++) {
        value[level] = 0;
      }

      rootVector[dim] = value;
    }

    return new SearchTreeNode(rootVector, occurrences);
  }
}
