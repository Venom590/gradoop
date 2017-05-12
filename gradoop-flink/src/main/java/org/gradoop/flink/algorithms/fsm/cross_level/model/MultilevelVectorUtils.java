package org.gradoop.flink.algorithms.fsm.cross_level.model;

import org.gradoop.common.util.IntArrayUtils;

public class MultilevelVectorUtils {


  public static final int COUNTS_OFFSET = 2;

  /**
   * [vectorLength,depth0,..,depth_n,value01,..,value_nm]
   *
   * @param vectors
   * @return
   */
  public static int[] mux(int[][][] vectors) {
    int[][] sample = vectors[0];

    int vectorCount = vectors.length;
    int dimensionCount = sample.length;
    int[] schema = new int[dimensionCount];

    // determine schema
    int vectorLength = 0;
    for (int i = 0; i < dimensionCount; i++) {
      int dimensionLength = sample[i].length;
      schema[i] = dimensionLength;
      vectorLength += dimensionLength;
    }

    int[] mux = new int[COUNTS_OFFSET + schema.length + vectors.length * vectorLength];

    mux[0] = vectorCount;
    mux[1] = dimensionCount;

    int i = 2;

    for (int dimensionLength : schema) {
      mux[i] = dimensionLength;
      i++;
    }

    for (int[][] vector : vectors) {
      for (int[] dimension : vector) {
        for (int value : dimension) {
          mux[i] = value;
          i++;
        }
      }
    }

    return mux;
  }

  public static int[][][] demux(int[] mux) {

    int vectorCount = mux[0];
    int dimensionCount = mux[1];
    int headerOffset = COUNTS_OFFSET + dimensionCount;

    int[] schema = new int[dimensionCount];

    // read schema
    int vectorLength = 0;
    for (int i = 0; i < dimensionCount; i++) {
      int dimensionLength = mux[i + COUNTS_OFFSET];
      schema[i] = dimensionLength;
      vectorLength += dimensionLength;
    }

    int[][][] vectors = new int[vectorCount][][];

    for (int v = 0; v < vectorCount; v++) {
      int[][] vector = new int[dimensionCount][];

      int vectorOffset = headerOffset + v * vectorLength;
      int dimensionOffset = 0;

      for (int d = 0; d < dimensionCount; d++) {
        int depth = schema[d];
        int[] dimension = new int[depth];

        for (int l = 0; l < depth; l++) {
          dimension[l] = mux[vectorOffset + dimensionOffset + l];
        }

        dimensionOffset += depth;

        vector[d] = dimension;
      }

      vectors[v] = vector;
    }


    return vectors;
  }
}
