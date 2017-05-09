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

package org.gradoop.flink.algorithms.fsm.xmd.functions.preprocessing;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.algorithms.fsm.xmd.tuples.MultidimensionalGraph;
import org.gradoop.flink.model.impl.tuples.WithCount;

import java.util.Set;

/**
 * graph -> (vertexLabel,1L),..
 */
public class ReportLabels
  implements FlatMapFunction<MultidimensionalGraph, WithCount<String>> {

  /**
   * reuse tuple to avoid instantiations
   */
  private WithCount<String> reuseTuple = new WithCount<>(null, 1);

  @Override
  public void flatMap(
    MultidimensionalGraph graph, Collector<WithCount<String>> out) throws Exception {

    String[][][] vertexData = graph.getVertexData();

    Set<String> values = Sets.newHashSet();

    for (String[][] labelAndDimensions : vertexData) {
      for (String[] labelOrDimension : labelAndDimensions) {
        for (String value : labelOrDimension) {
          values.add(value);
        }
      }
    }

    for (String label : graph.getEdgeLabels()) {
      values.add(label);
    }

    for (String label : values) {
      reuseTuple.setObject(label);
      out.collect(reuseTuple);
    }
  }
}
