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

package org.gradoop.flink.algorithms.fsm.cross_level.functions.preprocessing;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.flink.algorithms.fsm.common.config.FSMConstants;
import org.gradoop.flink.representation.transactional.GraphTransaction;

import java.util.Set;

/**
 * graph -> (vertexLabel,1L),..
 */
public class ReportLabels
  implements MapFunction<GraphTransaction, Tuple3<String[], String[], String[]>> {

  /**
   * reuse tuple to avoid instantiations
   */
  private final Tuple3<String[], String[], String[]> reuseTuple = new Tuple3<>();

  private final Set<String> vertexLabels = Sets.newHashSet();
  private final Set<String> edgeLabels = Sets.newHashSet();
  private final Set<String> levelValues = Sets.newHashSet();

  @Override
  public Tuple3<String[], String[], String[]> map(GraphTransaction graph) throws Exception {

    vertexLabels.clear();
    edgeLabels.clear();
    levelValues.clear();

    for (Vertex vertex : graph.getVertices()) {
      vertexLabels.add(vertex.getLabel());

      for (Property property : vertex.getProperties()) {
        if (property.getKey().startsWith(FSMConstants.DIMENSION_PREFIX)) {
          levelValues.add(property.getValue().getString());
        }
      }
    }

    for (Edge edge : graph.getEdges()) {
      edgeLabels.add(edge.getLabel());
    }

    reuseTuple.f0 = vertexLabels.toArray(new String[vertexLabels.size()]);
    reuseTuple.f1 = edgeLabels.toArray(new String[edgeLabels.size()]);
    reuseTuple.f2 = levelValues.toArray(new String[levelValues.size()]);

    return reuseTuple;
  }

}
