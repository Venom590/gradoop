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
import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.algorithms.fsm.common.gspan.GSpanLogic;
import org.gradoop.flink.algorithms.fsm.cross_level.tuples.MultilevelGraph;
import org.gradoop.flink.algorithms.fsm.cross_level.tuples.PatternVectors;
import org.gradoop.flink.algorithms.fsm.dimspan.model.Simple16Compressor;

import java.util.Iterator;

public class CombineVectors
  implements GroupCombineFunction<MultilevelGraph, PatternVectors> {

  private final GSpanLogic gSpan;

  public CombineVectors(GSpanLogic gSpan) {
    this.gSpan = gSpan;
  }

  @Override
  public void combine(Iterable<MultilevelGraph> values, Collector<PatternVectors> out)
    throws Exception {

    Iterator<MultilevelGraph> iterator = values.iterator();

    MultilevelGraph multilevelPattern = iterator.next();

    int[] compressedPattern = multilevelPattern.getGraph();
    int[] pattern = Simple16Compressor.uncompress(compressedPattern);

    int[][][] vectors = new int[][][] {multilevelPattern.getVector()};

    if (pattern.length == 6 || gSpan.isMinimal(pattern)) {

      while (iterator.hasNext()) {
        vectors = ArrayUtils.add(vectors, iterator.next().getVector());
      }

      out.collect(new PatternVectors(compressedPattern, vectors));
    }
  }
}
