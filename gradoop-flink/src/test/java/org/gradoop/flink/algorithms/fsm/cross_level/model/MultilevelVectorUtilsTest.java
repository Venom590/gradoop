package org.gradoop.flink.algorithms.fsm.cross_level.model;

import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;

/**
 * Created by peet on 11.05.17.
 */
public class MultilevelVectorUtilsTest extends GradoopFlinkTestBase {

  @Test
  public void test() throws Exception {

    int[][] vector1 = new int[2][];
    vector1[0] = new int[] {1, 1};
    vector1[1] = new int[] {1, 1, 1};

    int[][] vector2 = new int[2][];
    vector2[0] = new int[] {1, 1};
    vector2[1] = new int[] {1, 1, 2};

    int[][] vector3 = new int[2][];
    vector3[0] = new int[] {1, 2};
    vector3[1] = new int[] {1, 1, 2};

    int[][][] in = new int[][][] {vector1, vector2, vector3};

    int[][][] out = MultilevelVectorUtils.demux(MultilevelVectorUtils.mux(in));

    assertTrue(Arrays.deepEquals(in, out));
  }

}