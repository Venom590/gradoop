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

package org.gradoop.flink.algorithms.fsm.cross_level.config;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.gradoop.flink.algorithms.fsm.common.config.FSMConfigBase;

import java.util.List;

/**
 * Frequent subgraph mining configuration.
 */
public class CrossLevelTFSMConfig extends FSMConfigBase {

  private VectorMiningStrategy vectorMiningStrategy;

  /**
   * valued constructor
   * @param minSupport minimum relative support of a subgraph
   * @param directed direction mode
   */
  public CrossLevelTFSMConfig(float minSupport, boolean directed) {
    super(minSupport, directed);
    vectorMiningStrategy = VectorMiningStrategy.TOP_DOWN;
  }

  @Override
  public String toString() {
    List<String> parameters = Lists.newArrayList();

    parameters.add("s_min : " + String.valueOf(minSupport));
    parameters.add((directed ? "directed" : "undirected") + " mode");
    parameters.add("dictionary type : " + dictionaryType.toString());

    parameters.add(getParameterEnabled("branch constraint", branchConstraintEnabled));

    return StringUtils.join(parameters, "|");
  }

  public VectorMiningStrategy getVectorMiningStrategy() {
    return vectorMiningStrategy;
  }

  public void setVectorMiningStrategy(VectorMiningStrategy vectorMiningStrategy) {
    this.vectorMiningStrategy = vectorMiningStrategy;
  }
}
