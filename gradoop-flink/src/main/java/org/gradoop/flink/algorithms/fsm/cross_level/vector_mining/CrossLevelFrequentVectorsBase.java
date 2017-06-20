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
