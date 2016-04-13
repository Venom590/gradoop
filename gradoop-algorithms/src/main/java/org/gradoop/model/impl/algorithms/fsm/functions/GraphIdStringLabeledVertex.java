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

package org.gradoop.model.impl.algorithms.fsm.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.algorithms.fsm.tuples.LabeledVertex;
import org.gradoop.model.impl.algorithms.fsm.tuples.StringLabeledVertex;
import org.gradoop.model.impl.id.GradoopId;

/**
 * EPGMVertex => [(GraphId, LabeledVertex),..]
 * @param <V> vertex type
 */
public class GraphIdStringLabeledVertex<V extends EPGMVertex>
  implements FlatMapFunction<V, Tuple2<GradoopId, StringLabeledVertex>> {

  @Override
  public void flatMap(V vertex,
    Collector<Tuple2<GradoopId, StringLabeledVertex>> collector) throws Exception {

    StringLabeledVertex stringLabeledVertex = new StringLabeledVertex();
    stringLabeledVertex.setId(vertex.getId());
    stringLabeledVertex.setLabel(vertex.getLabel());

    for (GradoopId graphId : vertex.getGraphIds()) {


      collector.collect(
        new Tuple2<>(graphId, stringLabeledVertex));
    }
  }
}