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

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.flink.algorithms.fsm.common.config.FSMConstants;
import org.gradoop.flink.algorithms.fsm.common.tuples.PatternEmbeddingsMap;
import org.gradoop.flink.algorithms.fsm.common.comparison.DFSCodeComparator;
import org.gradoop.flink.algorithms.fsm.common.gspan.GSpanLogic;
import org.gradoop.flink.algorithms.fsm.cross_level.model.MultilevelVectorUtils;
import org.gradoop.flink.algorithms.fsm.cross_level.model.Simple16Compressor;
import org.gradoop.flink.algorithms.fsm.cross_level.tuples.MultilevelGraphWithPatternEmbeddingsMap;
import org.gradoop.flink.algorithms.fsm.cross_level.tuples.PatternVectors;
import org.gradoop.flink.model.impl.tuples.WithCount;

import java.util.List;

/**
 * (graph, k-edge pattern -> embeddings) => (graph, k+1-edge pattern -> embeddings)
 */
public class GrowFrequentPatterns
  extends RichMapFunction<MultilevelGraphWithPatternEmbeddingsMap, MultilevelGraphWithPatternEmbeddingsMap> {

  /**
   * compressed k-edge frequent patterns for fast embedding map lookup
   */
  private List<int[]> compressedFrequentPatterns;

  /**
   * uncompressed k-edge frequent patterns for pattern growth
   */
  private List<int[]> frequentPatterns;

  /**
   * list of rightmost paths, index relates to frequent patterns
   */
  private List<int[]> rightmostPaths;

  /**
   * pattern growth logic (directed or undirected mode)
   */
  private final GSpanLogic gSpan;
  private List<PatternVectors> patternVectorsList;

  /**
   * Constructor.
   *
   * @param gSpan pattern growth logic
   */
  public GrowFrequentPatterns(GSpanLogic gSpan) {

    // set pattern growth logic for directed or undirected mode
    this.gSpan = gSpan;

  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    // broadcast reception

    patternVectorsList =
      getRuntimeContext().getBroadcastVariable(FSMConstants.FREQUENT_PATTERNS);

    int patternCount = patternVectorsList.size();

    frequentPatterns = Lists.newArrayListWithExpectedSize(patternCount);

    for (PatternVectors patternVectors : patternVectorsList) {
      frequentPatterns.add(Simple16Compressor.uncompress(patternVectors.getPattern()));
    }


    // sort
    frequentPatterns.sort(new DFSCodeComparator());

    // calculate rightmost paths
    this.rightmostPaths = Lists.newArrayListWithExpectedSize(patternCount);
    this.compressedFrequentPatterns = Lists.newArrayListWithExpectedSize(patternCount);

    for (int[] pattern : frequentPatterns) {
      rightmostPaths.add(gSpan.getRightmostPathTimes(pattern));
      compressedFrequentPatterns.add(Simple16Compressor.compress(pattern));
    }
  }

  @Override
  public MultilevelGraphWithPatternEmbeddingsMap map(
    MultilevelGraphWithPatternEmbeddingsMap pair) throws Exception {

    // union k-1 edge frequent patterns with k-edge ones
    if (pair.isFrequentPatternCollector()) {
      for (PatternVectors patternVectors : patternVectorsList) {
        WithCount<int[]> patternWithFrequency =
          new WithCount<>(patternVectors.getPattern(), patternVectors.getVectors().length);

        pair.getMap().put(
          patternWithFrequency.getObject(), MultilevelVectorUtils.mux(patternVectors.getVectors()));
      }
    } else {
      int[] graph = pair.getGraph().getGraph();

      // execute pattern growth for all supported frequent patterns
      PatternEmbeddingsMap childMap = gSpan.growPatterns(graph, pair.getMap(),
        frequentPatterns, rightmostPaths, true, compressedFrequentPatterns);

      // update pattern-embedding map
      pair.setPatternEmbeddings(childMap);

      // compress patterns and embedding, if configured
      // NOTE: graphs will remain compressed
      Simple16Compressor.compressPatterns(pair.getMap());
      Simple16Compressor.compressEmbeddings(pair.getMap());
    }

    return pair;
  }
}
