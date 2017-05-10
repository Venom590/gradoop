package org.gradoop.flink.algorithms.fsm.cross_level.vector_mining;

public abstract class CrossLevelFrequentVectorsBase implements CrossLevelFrequentVectors {

  protected int dimCount;

  protected int[] schema;

  protected void extractSchema(int[][][] data) {
    int[][] sample = data[0];
    dimCount = sample.length;
    schema = new int[dimCount];

    for (int dim = 0; dim < dimCount; dim++) {
      int levelCount = sample[dim].length;
      schema[dim] = levelCount;
    }
  }
}
