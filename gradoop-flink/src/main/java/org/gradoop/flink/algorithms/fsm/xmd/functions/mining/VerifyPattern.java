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

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.flink.algorithms.fsm.xmd.gspan.GSpanLogic;
import org.gradoop.flink.algorithms.fsm.xmd.model.GraphUtils;
import org.gradoop.flink.algorithms.fsm.xmd.model.GraphUtilsBase;
import org.gradoop.flink.algorithms.fsm.xmd.model.Simple16Compressor;
import org.gradoop.flink.model.impl.tuples.WithCount;

/**
 * DFS code => true, if minimal
 */
public class VerifyPattern implements FilterFunction<WithCount<int[]>> {

  /**
   * validation logic
   */
  private final GSpanLogic gSpan;

  /**
   * util methods ti interpret int-array encoded patterns
   */
  private GraphUtils graphUtils = new GraphUtilsBase();

  /**
   * Constructor.
   *
   * @param gSpan validation logic
   */
  public VerifyPattern(GSpanLogic gSpan) {
    this.gSpan = gSpan;
  }

  @Override
  public boolean filter(WithCount<int[]> traversalCodeWithCount) throws Exception {
    int[] pattern = traversalCodeWithCount.getObject();

    boolean valid = true;

    pattern = Simple16Compressor.uncompress(pattern);


    if (graphUtils.getEdgeCount(pattern) > 1) {
      valid = gSpan.isMinimal(pattern);
    }
    return valid;
  }
}
