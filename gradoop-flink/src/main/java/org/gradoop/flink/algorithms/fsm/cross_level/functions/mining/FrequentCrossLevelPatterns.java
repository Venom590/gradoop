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

import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.gradoop.flink.algorithms.fsm.common.config.FSMConstants;
import org.gradoop.flink.algorithms.fsm.cross_level.tuples.PatternVectors;
import org.gradoop.flink.algorithms.fsm.cross_level.vector_mining.CrossLevelFrequentVectors;
import org.gradoop.flink.model.impl.tuples.WithCount;

import java.util.Collection;
import java.util.Iterator;

public class FrequentCrossLevelPatterns
  extends RichGroupReduceFunction<PatternVectors, PatternVectors> {

  private final CrossLevelFrequentVectors vectorMiner;
  /**
   * minimum frequency
   */
  private int minFrequency;

  public FrequentCrossLevelPatterns(CrossLevelFrequentVectors vectorMiner) {
    this.vectorMiner = vectorMiner;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    this.minFrequency = Math.toIntExact(
      getRuntimeContext().<Long>getBroadcastVariable(FSMConstants.MIN_FREQUENCY).get(0));
  }

  @Override
  public void reduce(Iterable<PatternVectors> values, Collector<PatternVectors> out) throws
    Exception {

    Iterator<PatternVectors> iterator = values.iterator();

    PatternVectors patternVectors = iterator.next();

    int[][][] vectors = patternVectors.getVectors();

    while (iterator.hasNext()) {
      vectors = ArrayUtils.addAll(vectors, iterator.next().getVectors());
    }

    if (vectors.length >= minFrequency) {
      Collection<WithCount<int[][]>> frequentVectorPatterns =
        vectorMiner.mine(vectors, minFrequency);

      int [][][] vectorPatterns = new int[frequentVectorPatterns.size()][][];


      int i =0;
      for (WithCount<int[][]> frequentVectorPattern : frequentVectorPatterns) {
        vectorPatterns[i] = frequentVectorPattern.getObject();

        i++;
      }

      patternVectors.setVectors(vectorPatterns);

      out.collect(patternVectors);
    }
  }
}
