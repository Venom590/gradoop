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

package org.gradoop.flink.algorithms.fsm.xmd.tuples;

import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Minimalistic model of a string-labeled graph.
 *
 * f0: [vertexData_0,..,vertexData_j]
 * f1: [edgeLabel_0,..,edgeLabel_k]
 * f2: [[edgeSource_0,edgeTarget_0],..,[edgeSource_k,edgeTarget_k]]
 *
 * vertexData : [[vertexLabel],[dim1,val0,val1,val2],[dim2,val0,val1,val2],..]
 */
public class MultidimensionalGraph extends Tuple3<String[][][], String[], int[][]> {

  public MultidimensionalGraph(String[][][] vertexData, String[] edgeLabels, int[][] edges) {
    super(vertexData, edgeLabels, edges);
  }

  public String[][][] getVertexData() {
    return this.f0;
  }

  public String[] getEdgeLabels() {
    return this.f1;
  }

  public int[][] getEdges() {
    return this.f2;
  }
}
