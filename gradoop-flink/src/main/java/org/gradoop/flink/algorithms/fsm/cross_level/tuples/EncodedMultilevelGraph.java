package org.gradoop.flink.algorithms.fsm.cross_level.tuples;

import org.apache.flink.api.java.tuple.Tuple2;

/**
 * f0: int-array encoded graph
 * f1: dimensional attributes [[level1, level2, ..],..]
 */

public class EncodedMultilevelGraph extends Tuple2<int[], int[][]> {
  public EncodedMultilevelGraph() {
  }

  public EncodedMultilevelGraph(int[] graph, int[][] levels) {
    super(graph, levels);
  }

  public int[] getGraph() {
    return this.f0;
  }

  public static EncodedMultilevelGraph getEmptyOne() {
    return new EncodedMultilevelGraph(new int[0], new int[0][]);
  }
}
