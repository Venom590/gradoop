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

package org.gradoop.flink.algorithms.fsm.xmd.functions.mining;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.flink.algorithms.fsm.xmd.comparison.DFSCodeComparator;
import org.gradoop.flink.algorithms.fsm.xmd.config.XMDConfig;
import org.gradoop.flink.algorithms.fsm.xmd.config.DIMSpanConstants;
import org.gradoop.flink.algorithms.fsm.xmd.config.DataflowStep;
import org.gradoop.flink.algorithms.fsm.xmd.gspan.GSpanLogic;
import org.gradoop.flink.algorithms.fsm.xmd.model.Simple16Compressor;
import org.gradoop.flink.algorithms.fsm.xmd.tuples.GraphWithPatternEmbeddingsMap;
import org.gradoop.flink.algorithms.fsm.xmd.tuples.PatternEmbeddingsMap;
import org.gradoop.flink.model.impl.tuples.WithCount;

import java.util.List;

/**
 * (graph, k-edge pattern -> embeddings) => (graph, k+1-edge pattern -> embeddings)
 */
public class GrowFrequentPatterns
  extends RichMapFunction<GraphWithPatternEmbeddingsMap, GraphWithPatternEmbeddingsMap> {

  /**
   * compressed k-edge frequent patterns for fast embedding map lookup
   */
  private List<int[]> compressedFrequentPatterns;

  /**
   * uncompressed k-edge frequent patterns for pattern growth
   */
  private List<int[]> frequentPatterns;

  /**
   * patterns with frequency for collector
   */
  private List<WithCount<int[]>> patternFrequencies;

  /**
   * list of rightmost paths, index relates to frequent patterns
   */
  private List<int[]> rightmostPaths;

  /**
   * pattern growth logic (directed or undirected mode)
   */
  private final GSpanLogic gSpan;

  /**
   * flag to enable pattern verification before counting (true=enabled)
   */
  private final boolean validatePatterns;

  /**
   * Constructor.
   *
   * @param gSpan pattern growth logic
   * @param fsmConfig FSM Configuration
   */
  public GrowFrequentPatterns(GSpanLogic gSpan, XMDConfig fsmConfig) {

    // set pattern growth logic for directed or undirected mode
    this.gSpan = gSpan;

    // cache validation flag
    validatePatterns = fsmConfig.getPatternVerificationInStep() == DataflowStep.MAP;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    // broadcast reception

    patternFrequencies =
      getRuntimeContext().getBroadcastVariable(DIMSpanConstants.FREQUENT_PATTERNS);

    int patternCount = patternFrequencies.size();

    this.frequentPatterns = Lists.newArrayListWithExpectedSize(patternCount);

    for (WithCount<int[]> patternWithCount : patternFrequencies) {
      int[] pattern = patternWithCount.getObject();

      // uncompress
      pattern = Simple16Compressor.uncompress(pattern);

      frequentPatterns.add(pattern);
    }

    // sort
    frequentPatterns.sort(new DFSCodeComparator());

    // calculate rightmost paths
    this.rightmostPaths = Lists.newArrayListWithExpectedSize(patternCount);
    this.compressedFrequentPatterns = Lists.newArrayListWithExpectedSize(patternCount);

    for (int[] pattern : frequentPatterns) {
      rightmostPaths.add(gSpan.getRightmostPathTimes(pattern));

      // TODO: directly store compressed patterns at reception
      compressedFrequentPatterns.add(Simple16Compressor.compress(pattern));
    }
  }

  @Override
  public GraphWithPatternEmbeddingsMap map(GraphWithPatternEmbeddingsMap pair) throws Exception {

    // union k-1 edge frequent patterns with k-edge ones
    if (pair.isFrequentPatternCollector()) {
      for (WithCount<int[]> patternWithFrequency : patternFrequencies) {
        pair.getMap().collect(patternWithFrequency);
      }
    } else {
      int[] graph = pair.getGraph();

      // execute pattern growth for all supported frequent patterns
      PatternEmbeddingsMap childMap = gSpan.growPatterns(graph, pair.getMap(),
        frequentPatterns, rightmostPaths, true, compressedFrequentPatterns);

      // drop non-minimal patterns if configured to be executed here
      if (validatePatterns) {
        PatternEmbeddingsMap validatedMap = PatternEmbeddingsMap.getEmptyOne();

        for (int i = 0; i < childMap.getPatternCount(); i++) {
          int[] pattern = childMap.getPattern(i);

          if (gSpan.isMinimal(pattern)) {
            int[] embeddingData = childMap.getValues()[i];
            validatedMap.put(pattern, embeddingData);
          }
        }

        childMap = validatedMap;
      }

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
