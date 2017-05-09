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

package org.gradoop.flink.algorithms.fsm.xmd.config;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.gradoop.flink.algorithms.fsm.common.config.FSMConfigBase;

import java.util.List;

/**
 * Frequent subgraph mining configuration.
 */
public class XMDConfig extends FSMConfigBase {

  private final String dimensionKeyPrefix;
  private final String dimensionValueSeparator;

  /**
   * valued constructor
   * @param minSupport minimum relative support of a subgraph
   * @param directed direction mode
   */
  public XMDConfig(float minSupport, boolean directed) {
    super(minSupport, directed);
    dimensionKeyPrefix = "dim";
    dimensionValueSeparator = ".";
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

  public String getDimensionKeyPrefix() {
    return dimensionKeyPrefix;
  }

  public String getDimensionValueSeparator() {
    return dimensionValueSeparator;
  }
}
