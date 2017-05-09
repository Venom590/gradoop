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

package org.gradoop.flink.algorithms.fsm.xmd.functions.conversion;

import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.flink.algorithms.fsm.xmd.config.XMDConfig;
import org.gradoop.flink.algorithms.fsm.xmd.tuples.MultidimensionalGraph;
import org.gradoop.flink.representation.transactional.GraphTransaction;

import java.util.Map;

/**
 * Gradoop Graph Transaction => lightweight labeled graph
 */
public class EPGMGraphTransactionToMultidimensionalGraph implements
  MapFunction<GraphTransaction, MultidimensionalGraph> {

  private final String dimKeyPrefix;
  private final String dimValueSeparator;

  public EPGMGraphTransactionToMultidimensionalGraph(XMDConfig config) {
    dimKeyPrefix = config.getDimensionKeyPrefix();
    dimValueSeparator = config.getDimensionValueSeparator();
  }

  @Override
  public MultidimensionalGraph map(GraphTransaction transaction) throws Exception {

    int vertexCount = transaction.getVertices().size();
    Map<GradoopId, Integer> vertexIdMap = Maps.newHashMapWithExpectedSize(vertexCount);
    String[][][] vertexData = new String[vertexCount][][];

    int vertexId = 0;
    for (Vertex vertex : transaction.getVertices()) {

      String[][] data = new String[][] {new String[] {vertex.getLabel()}};

      for (Property property : vertex.getProperties()) {
        String key = property.getKey();

        if (key.startsWith(dimKeyPrefix)) {
          String[] values = property.getValue().getString().split(dimValueSeparator);
          String[] dimension = new String[values.length + 1];
          dimension[0] = key;

          System.arraycopy(values, 0, dimension, 1, dimension.length - 1);

          vertexData[vertexId] = data;
        }
      }

      vertexIdMap.put(vertex.getId(), vertexId);
      vertexId++;
    }

    int edgeCount = transaction.getEdges().size();
    String[] edgeLabels = new String[edgeCount];
    int[][] edges = new int[edgeCount][];

    int edgeId = 0;
    for (Edge edge : transaction.getEdges()) {

      int sourceId = vertexIdMap.get(edge.getSourceId());
      int targetId = vertexIdMap.get(edge.getTargetId());

      edges[edgeId] = new int[] {sourceId, targetId};
      edgeLabels[edgeId] = edge.getLabel();

      edgeId++;
    }

    return new MultidimensionalGraph(vertexData, edgeLabels, edges);
  }
}
