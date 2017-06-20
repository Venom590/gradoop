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
