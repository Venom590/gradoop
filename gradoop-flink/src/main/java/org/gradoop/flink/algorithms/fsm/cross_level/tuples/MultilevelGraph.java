package org.gradoop.flink.algorithms.fsm.cross_level.tuples;

import org.apache.flink.api.java.tuple.Tuple2;

/**
 * f0: int-array encoded graph
 * f1: dimensional attributes [[level1, level2, ..],..]
 */

public class MultilevelGraph extends Tuple2<int[], int[][]> {
  public MultilevelGraph() {
  }

  public MultilevelGraph(int[] graph, int[][] levels) {
    super(graph, levels);
  }

  public int[] getGraph() {
    return this.f0;
  }

  public static MultilevelGraph getEmptyOne() {
    return new MultilevelGraph(new int[0], new int[0][]);
  }

  public int[][] getVector() {
    return this.f1;
  }

  public void setGraph(int[] graph) {
    this.f0 = graph;
  }

  public void setLevels(int[][] levels) {
    this.f1 = levels;
  }
}
