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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.algorithms.fsm.common.tuples.PatternEmbeddingsMap;
import org.gradoop.flink.algorithms.fsm.cross_level.tuples.MultilevelGraph;
import org.gradoop.flink.algorithms.fsm.cross_level.tuples.MultilevelGraphWithPatternEmbeddingsMap;
import org.gradoop.flink.algorithms.fsm.cross_level.vector_mining.CrossLevelVectorComparator;
import org.gradoop.flink.algorithms.fsm.dimspan.model.Simple16Compressor;

import java.util.Arrays;

/**
 * (graph, pattern->embeddings) => (pattern, 1),..
 */
public class ReportSupportedPatterns
  implements FlatMapFunction<MultilevelGraphWithPatternEmbeddingsMap, MultilevelGraph> {

  private final MultilevelGraph reuseTuple = new MultilevelGraph();

  @Override
  public void flatMap(MultilevelGraphWithPatternEmbeddingsMap graphEmbeddings,
    Collector<MultilevelGraph> collector) throws Exception {

    if (! graphEmbeddings.isFrequentPatternCollector()) {
      PatternEmbeddingsMap map = graphEmbeddings.getMap();

      int[][] graphLevels = graphEmbeddings.getGraph().getVector();

      for (int p = 0; p < map.getPatternCount(); p++) {
        int[] pattern = map.getKeys()[p];

        pattern = Simple16Compressor.uncompress(pattern);

        int[][] embeddingData = map.getEmbeddings(p, true);
        int embeddingCount = embeddingData.length / 2;

        int[][][] embeddingLevels = new int[embeddingCount][][];

        int vertexCount = embeddingData[0].length;

        for (int e = 0; e < embeddingCount; e++) {
          int[] vertexMapping = embeddingData[2 * e];
          int[][] patternLevels = new int[vertexCount][];

          for (int v = 0; v < vertexCount; v++) {
            patternLevels[v] = graphLevels[vertexMapping[v]];
          }

          embeddingLevels[e] = patternLevels;
        }

        Arrays.sort(embeddingLevels, new CrossLevelVectorComparator());

        int[][] last = new int[0][];

        pattern = Simple16Compressor.compress(pattern);

        for (int[][] current : embeddingLevels) {
          if (! Arrays.equals(current, last)) {
            last = current;
            reuseTuple.setGraph(pattern);
            reuseTuple.setLevels(current);
            collector.collect(reuseTuple);
          }
        }
      }
    }
  }
}
