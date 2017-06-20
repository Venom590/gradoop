/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

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
    frequentPatterns.add(new WithCount<>(root.getPattern(), root.getFrequency()));

    Collection<SearchTreeNode> dimParents = Lists.newArrayList(root);
    Collection<SearchTreeNode> parentLevel;

    for (int dim = 0; dim < dimCount; dim++) {
      Collection<SearchTreeNode> dimChildren = Lists.newArrayList();
      parentLevel = dimParents;

      int dimDepth = schema[dim];

      if (dimDepth > 0) {
        for (int level = 0; level < dimDepth; level++) {
          Collection<SearchTreeNode> childLevel = Lists.newArrayList();

          for (SearchTreeNode parent : parentLevel) {

            int[][] pattern = IntArrayUtils.deepCopy(parent.getPattern());

            int lastValue = -1;
            int[] occurrences = new int[0];

            for (int vectorIdx : parent.getOccurrences()) {
              int value = vectors[vectorIdx][dim][level];

              if (value > lastValue) {

                if (occurrences.length >= minFrequency) {
                  pattern = IntArrayUtils.deepCopy(pattern);
                  pattern[dim][level] = value;

                  childLevel.add(new SearchTreeNode(pattern, occurrences));
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

              childLevel.add(new SearchTreeNode(pattern, occurrences));
              frequentPatterns.add(new WithCount<>(pattern, occurrences.length));
            }
          }

          dimChildren.addAll(childLevel);
          parentLevel = childLevel;
        }
        dimParents = dimChildren;
        dimParents.add(root);
      }
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
