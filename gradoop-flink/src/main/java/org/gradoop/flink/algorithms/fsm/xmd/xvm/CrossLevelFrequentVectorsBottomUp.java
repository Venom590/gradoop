package org.gradoop.flink.algorithms.fsm.xmd.xvm;

import org.apache.commons.lang3.ArrayUtils;

import java.util.Arrays;
import java.util.Objects;

public class CrossLevelFrequentVectorsBottomUp implements CrossLevelFrequentVectors {

  private final CrossLevelVectorComparator vectorComparator = new CrossLevelVectorComparator();
  private int[][][] patterns;
  private int[] frequencies;

  @Override
  public int[][][] mine(int[][][] currentLevel, float minSupport) {
    patterns = new int[0][][];
    frequencies = new int[0];

    // while not reached root
    if (currentLevel.length > 1) {
      currentLevel = countChildrenAndGetParents(currentLevel);


      int[][] root = getRoot(currentLevel[0]);


    }

    return patterns;
  }

  private int[][][] countChildrenAndGetParents(int[][][] children) {
    int[][][] parents = new int[0][][];

    Arrays.sort(children, vectorComparator);

    int[][] last = children[0];
    int [][] current = new int[0][];
    int frequency = 1;

    for (int i = 1; i < children.length; i++) {
       current = children[i];

      if(Objects.deepEquals(last, current)) {
        frequency++;
      } else {
        store(last, frequency);

        parents = ArrayUtils.addAll(parents, generalize(current));

        last = current;
        frequency = 1;
      }
    }

    store(current, frequency);

    return parents;
  }

  private int[][][] generalize(int[][] child) {
    int[][][] parents = new int[0][][];

    // TODO: constrained generalizations

    return parents;
  }

  private void store(int[][] last, int frequency) {
    patterns = ArrayUtils.add(patterns, last);
    frequencies = ArrayUtils.add(frequencies, frequency);
  }

  private int[][] getRoot(int[][] sample) {

    int vectorLength = sample.length;
    int[][] vector = new int[vectorLength][];


    for (int i = 0; i < vectorLength; i++) {

      int fieldLength = sample[i].length;
      int[] field = new int[fieldLength];

      for (int j = 0; j < fieldLength; j++) {
        field[j] = 0;
      }
      vector[i] = field;
    }


    return vector;
  }
}
