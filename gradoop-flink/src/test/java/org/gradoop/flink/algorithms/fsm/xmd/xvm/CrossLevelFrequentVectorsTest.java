package org.gradoop.flink.algorithms.fsm.xmd.xvm;

import org.gradoop.flink.model.impl.tuples.WithCount;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class CrossLevelFrequentVectorsTest {

  private final CrossLevelVectorComparator patternComparator  = new CrossLevelVectorComparator();

  @Test
  public void mine() throws Exception {

    int[][][] database = getDatabase();

    int[][][] expectedResult = getExpectedResult();
    Arrays.sort(expectedResult, patternComparator);

    CrossLevelFrequentVectors miner = new CrossLevelFrequentVectorsBottomUp();

    Collection<WithCount<int[][]>> result = miner.mine(database, 3);

    assertEquals(expectedResult.length, result.size());

    for (int[][] expected : expectedResult) {
      boolean found = false;

      for (WithCount<int[][]> actual : result) {
        found = equal(expected, actual.getObject());
      }

      assertTrue(found);
    }
  }

  private boolean equal(int[][] a, int[][] b) {
    boolean equal = true;

    for (int i = 0; i < a.length; i++) {
      if (!Objects.deepEquals(a[i], b[i])) {
        equal = false;
        break;
      }
    }

    return equal;
  }

  public int[][][] getDatabase() {
    int[][] vector1 = new int[2][];
    vector1[0] = new int[] {1, 1};
    vector1[1] = new int[] {1, 1, 1};

    int[][] vector2 = new int[2][];
    vector2[0] = new int[] {1, 1};
    vector2[1] = new int[] {1, 1, 2};

    int[][] vector3 = new int[2][];
    vector3[0] = new int[] {1, 2};
    vector3[1] = new int[] {1, 1, 2};

    return new int[][][] {vector1, vector2, vector3};
  }

  public int[][][] getExpectedResult() {
    int[][] root = new int[2][];
    root[0] = new int[] {0, 0};
    root[1] = new int[] {0, 0, 0};

    int[][] pattern1 = new int[2][];
    pattern1[0] = new int[] {0, 0};
    pattern1[1] = new int[] {1, 0, 0};

    int[][] pattern2 = new int[2][];
    pattern2[0] = new int[] {0, 0};
    pattern2[1] = new int[] {1, 1, 0};

    int[][] pattern3 = new int[2][];
    pattern3[0] = new int[] {1, 0};
    pattern3[1] = new int[] {0, 0, 0};

    int[][] pattern4 = new int[2][];
    pattern4[0] = new int[] {1, 0};
    pattern4[1] = new int[] {1, 0, 0};

    int[][] pattern5 = new int[2][];
    pattern5[0] = new int[] {1, 0};
    pattern5[1] = new int[] {1, 1, 0};

    return new int[][][] {root, pattern1, pattern2, pattern3, pattern4, pattern5};
  }
}