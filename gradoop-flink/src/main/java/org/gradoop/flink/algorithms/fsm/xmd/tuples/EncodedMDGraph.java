package org.gradoop.flink.algorithms.fsm.xmd.tuples;

import org.apache.flink.api.java.tuple.Tuple2;

/**
 * f0: int-array encoded graph
 * f1: dimensional attributes [[vertexId, key, val0, val1, val2, ..],..]
 */

public class EncodedMDGraph extends Tuple2<int[], int[][]> {
  public EncodedMDGraph(int[] graph, int[][] dimensions) {
    super(graph, dimensions);
  }

  public EncodedMDGraph() {
  }

  public int[] getGraph() {
    return this.f0;
  }
}
