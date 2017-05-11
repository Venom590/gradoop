package org.gradoop.flink.algorithms.fsm.cross_level.model;

public class MultilevelVectorUtils {


  /**
   * [vectorLength,depth0,..,depth_n,value01,..,value_nm]
   *
   * @param vectors
   * @return
   */
  public static int[] mux(int[][][] vectors) {
    int[][] sample = vectors[0];

    int dimensionCount = sample.length;
    int[] schema = new int[dimensionCount];

    // determine schema
    int vectorLength = 0;
    for (int i = 0; i < dimensionCount; i++) {
      int dimensionLength = sample[i].length;
      schema[i] = dimensionLength;
      vectorLength += dimensionLength;
    }

    int[] mux = new int[1 + schema.length + vectors.length * vectorLength];

    int i = 0;

    mux[i] = dimensionCount;
    i++;

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

    int dimensionCount = mux[0];

    int[] schema = new int[dimensionCount];

    // read schema
    int vectorLength = 0;
    for (int i = 0; i < dimensionCount; i++) {
      int dimensionLength = mux[i + 1];
      schema[i] = dimensionLength;
      vectorLength += dimensionLength;
    }

    int headerOffset = dimensionCount + 1;
    int vectorCount = (mux.length - headerOffset) / vectorLength;

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
