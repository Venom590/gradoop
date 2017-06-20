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

package org.gradoop.flink.algorithms.fsm.cross_level.functions;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.algorithms.fsm.common.config.FSMConstants;
import org.gradoop.flink.representation.transactional.GraphTransaction;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

public class CollapseDimensionProperties
  implements FlatMapFunction<GraphTransaction, GraphTransaction> {

  @Override
  public void flatMap(GraphTransaction graph, Collector<GraphTransaction> out) throws Exception {

    Collection<GradoopId> dimVertexIds = Sets.newHashSet();

    Map<GradoopId, Edge> sourceDimEdges = Maps.newHashMap();
    Map<GradoopId, String> vertexLabels = Maps.newHashMap();

    for (Vertex vertex : graph.getVertices()) {
      vertexLabels.put(vertex.getId(), vertex.getLabel());
    }

    Iterator<Edge> edgeIterator = graph.getEdges().iterator();

    while (edgeIterator.hasNext()) {
      Edge edge = edgeIterator.next();

      if (edge.getLabel().startsWith(FSMConstants.DIMENSION_PREFIX)) {
        sourceDimEdges.put(edge.getSourceId(), edge);
        edgeIterator.remove();
      }
    }

    for (Vertex vertex : graph.getVertices()) {

      GradoopId sourceId = vertex.getId();

      while (sourceId != null) {
        Edge edge = sourceDimEdges.get(sourceId);

        if (edge == null) {
          sourceId = null;
        } else {
          dimVertexIds.add(edge.getTargetId());
          vertex.setProperty(edge.getLabel(), vertexLabels.get(edge.getTargetId()));
          sourceId = edge.getTargetId();
        }
      }
    }

    graph.getVertices().removeIf(vertex -> dimVertexIds.contains(vertex.getId()));

    if (graph.getEdges().size() > 0) {
      out.collect(graph);
    }
  }
}
