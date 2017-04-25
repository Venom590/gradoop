package org.gradoop.flink.algorithms.fsm.xmd.xvm;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.ArrayUtils;
import org.gradoop.common.util.IntArrayUtils;
import org.gradoop.flink.model.impl.tuples.WithCount;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public class CrossLevelFrequentVectorsBottomUp implements CrossLevelFrequentVectors {

  private int dimCount;
  private int[] schema;

  private final CrossLevelVectorWithCountComparator comparator =
    new CrossLevelVectorWithCountComparator();

  private List<WithCount<int[][]>> allLevels = Lists.newArrayList();
  private List<WithCount<int[][]>> currentLevel = Lists.newLinkedList();

  private int iteration;

  @Override
  public Collection<WithCount<int[][]>> mine(int[][][] data, int minFrequency) {
    extractSchema(data);

    allLevels.clear();
    currentLevel.clear();

    for (int[][] pattern : data) {
      currentLevel.add(new WithCount<>(pattern));
    }

    // while not reached root
    while (iteration > 0) {
      countCurrentLevelFrequencies();
      int currentLevelStartIndex = allLevels.size();
      int currentLevelSize = currentLevel.size();
      allLevels.addAll(currentLevel);
      currentLevel.clear();
      shiftToUpperLevel(currentLevelStartIndex, currentLevelSize);


      iteration--;
    }

//    System.out.println(IntArrayUtils.toString(patterns));
//    System.out.println(IntArrayUtils.toString(frequencies));


    Iterator<WithCount<int[][]>> iterator = allLevels.iterator();

    while (iterator.hasNext()) {
      WithCount<int[][]> pattern = iterator.next();
      System.out.println(IntArrayUtils.toString(pattern.getObject()) + "\t" + pattern.getCount());
    }

    return allLevels;
  }

  private void shiftToUpperLevel(int currentLevelStartIndex, int currentLevelSize) {

    for (int i = currentLevelStartIndex; i < currentLevelStartIndex + currentLevelSize; i++) {

      WithCount<int[][]> child = allLevels.get(i);

      generalize(child);
    }
  }

  private void countCurrentLevelFrequencies() {
    currentLevel.sort(comparator);

    Iterator<WithCount<int[][]>> iterator = currentLevel.iterator();
    WithCount<int[][]> last = iterator.next();

    while (iterator.hasNext()) {
      WithCount<int[][]> current = iterator.next();

      if (Objects.deepEquals(last.getObject(), current.getObject())) {
        last.setCount(last.getCount() + current.getCount());
        iterator.remove();
      } else {
        last = current;
      }
    }
  }

  private void extractSchema(int[][][] data) {
    int[][] sample = data[0];
    dimCount = sample.length;
    schema = new int[dimCount];
    iteration = 1;

    for (int dim = 0; dim < dimCount; dim++) {
      int levelCount = sample[dim].length;
      schema[dim] = levelCount;
      iteration += levelCount;
    }
  }

  private void generalize(WithCount<int[][]> childWithCount) {

    // for each dimension starting from the right hand side
    for (int dim = dimCount - 1; dim >= 0; dim--) {
      int levelCount = schema[dim];
      int[][] child = childWithCount.getObject();
      long frequency = childWithCount.getCount();
      int[] dimValues = child[dim];

      // check, if dimension was already generalized
      int lastGenLevel = ArrayUtils.indexOf(dimValues, 0);

      // if further generalization is possible
      if (lastGenLevel != 0) {

        // either next upper level (prior generalization) or base level (base value)
        int genLevel = (lastGenLevel > 0 ? lastGenLevel : levelCount) - 1;

        int[][] parent = IntArrayUtils.deepCopy(child);
        parent[dim][genLevel] = 0;

        currentLevel.add(new WithCount<>(parent, frequency));

      }

      // Pruning: stop, if dimension was already generalized before
      if (lastGenLevel >= 0) {
        break;
      }
    }
  }
}
