package org.gradoop.flink.algorithms.fsm.xmd.xvm;

import org.apache.commons.lang3.ArrayUtils;
import org.gradoop.common.util.IntArrayUtils;

import java.util.Arrays;
import java.util.Objects;

public class CrossLevelFrequentVectorsBottomUp implements CrossLevelFrequentVectors {

  private int dimCount;
  private int[] schema;

  private final CrossLevelVectorComparator vectorComparator = new CrossLevelVectorComparator();
  private int[][][] patterns;
  private int[] frequencies;
  private int iteration;

  @Override
  public int[][][] mine(int[][][] currentLevel, int minFrequency) {
    patterns = new int[0][][];
    frequencies = new int[0];

    extractSchema(currentLevel);

    // while not reached root
    while (iteration > 0) {
      currentLevel = countChildrenAndGetParents(currentLevel);
      iteration--;
    }

//    System.out.println(IntArrayUtils.toString(patterns));
//    System.out.println(IntArrayUtils.toString(frequencies));


    return patterns;
  }

  private void extractSchema(int[][][] data) {
    int[][] sample = data[0];
    dimCount = sample.length;
    schema = new int[dimCount];
    iteration = 0;

    for (int dim = 0; dim < dimCount; dim++) {
      int levelCount = sample[dim].length;
      schema[dim] = levelCount;
      iteration += levelCount;
    }
  }

  private int[][][] countChildrenAndGetParents(int[][][] children) {
    System.out.println("**** " + iteration + " : " + IntArrayUtils.toString(frequencies) + " ****");

    int[][][] parents = new int[0][][];

    Arrays.sort(children, vectorComparator);

    int p = patterns.length - 1;
    int[][] last = null;

    for (int[][] child : children) {
      if (Objects.deepEquals(last, child)) {
        frequencies[p]++;
      } else {
        last = child;
        parents = ArrayUtils.addAll(parents, generalize(last));
        patterns = ArrayUtils.add(patterns, last);
        frequencies = ArrayUtils.add(frequencies, 1);
        p++;
      }
    }

    return parents;
  }

  private int[][][] generalize(int[][] child) {
    int[][][] parents = new int[0][][];

    // for each dimension starting from the right hand side
    for (int dim = dimCount - 1; dim >= 0; dim--) {
      int levelCount = schema[dim];
      int[] dimValues = child[dim];

      // check, if dimension was already generalized
      int lastGenLevel = ArrayUtils.indexOf(dimValues, 0);

      // if further generalization is possible
      if (lastGenLevel != 0) {

        // either next upper level (prior generalization) or base level (base value)
        int genLevel = (lastGenLevel > 0 ? lastGenLevel : levelCount) - 1;

        int[][] parent = IntArrayUtils.deepCopy(child);
        parent[dim][genLevel] = 0;
        parents = ArrayUtils.add(parents, parent);

        // Pruning: stop, if dimension was already generalized before
        if (lastGenLevel > 0) {
          break;
        }
      }
    }

    System.out.println("----------------------------");
    System.out.println(IntArrayUtils.toString(child));
    System.out.println("=>");
    System.out.println(IntArrayUtils.toString(parents));


    return parents;
  }
}
