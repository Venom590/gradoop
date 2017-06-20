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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.flink.algorithms.fsm.common.config.FSMConstants;
import org.gradoop.flink.representation.transactional.GraphTransaction;

import java.util.Collection;
import java.util.Map;

public class ExpandDimensionProperties implements MapFunction<GraphTransaction, GraphTransaction> {

  private final Map<String, Integer> labelDepth = Maps.newHashMap();

  @Override
  public GraphTransaction map(GraphTransaction graph) throws Exception {

    Collection<Vertex> dimVertices = Lists.newArrayList();

    for (Vertex vertex : graph.getVertices()) {

      Integer depth = labelDepth.get(vertex.getLabel());

      if (depth == null) {
        depth = 0;

        for (Property property : vertex.getProperties()) {
          if (property.getKey().startsWith(FSMConstants.DIMENSION_PREFIX)) {
            depth++;
          }
        }

        labelDepth.put(vertex.getLabel(), depth);

      }

      if (depth > 0) {
        GradoopId sourceId = vertex.getId();

        for (int i = 0; i < depth; i++) {
          GradoopId targetId = GradoopId.get();
          String edgeLabel = FSMConstants.DIMENSION_PREFIX + String.valueOf(i);
          graph.getEdges().add(new Edge(GradoopId.get(), edgeLabel, sourceId, targetId, null, null));

          String vertexLabel = vertex.getPropertyValue(edgeLabel).toString();
          dimVertices.add(new Vertex(targetId, vertexLabel, null, null));

          sourceId = targetId;
        }
      }
    }

    graph.getVertices().addAll(dimVertices);

    return graph;
  }
}
