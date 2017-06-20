/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.algorithms.fsm.cross_level.model;


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
