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

package org.gradoop.flink.algorithms.fsm.cross_level.functions.mining;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.flink.algorithms.fsm.common.gspan.GSpanLogic;
import org.gradoop.flink.algorithms.fsm.common.tuples.PatternEmbeddingsMap;
import org.gradoop.flink.algorithms.fsm.cross_level.model.Simple16Compressor;
import org.gradoop.flink.algorithms.fsm.cross_level.tuples.MultilevelGraph;
import org.gradoop.flink.algorithms.fsm.cross_level.tuples.MultilevelGraphWithPatternEmbeddingsMap;


/**
 * graph => (graph, 1-edge pattern -> embeddings)
 */
public class InitSingleEdgePatternEmbeddingsMap
  implements MapFunction<MultilevelGraph, MultilevelGraphWithPatternEmbeddingsMap> {

  /**
   * pattern generation logic
   */
  private final GSpanLogic gSpan;

  /**
   * Constructor.
   *
   * @param gSpan pattern generation logic
   */
  public InitSingleEdgePatternEmbeddingsMap(GSpanLogic gSpan) {
    this.gSpan = gSpan;
  }

  @Override
  public MultilevelGraphWithPatternEmbeddingsMap map(MultilevelGraph graph) throws Exception {
    PatternEmbeddingsMap map = gSpan.getSingleEdgePatternEmbeddings(graph.getGraph());

    Simple16Compressor.compressPatterns(map);
    Simple16Compressor.compressEmbeddings(map);

    return new MultilevelGraphWithPatternEmbeddingsMap(graph, map);
  }

}
