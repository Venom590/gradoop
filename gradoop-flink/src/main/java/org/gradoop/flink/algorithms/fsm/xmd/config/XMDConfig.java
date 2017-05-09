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

import java.io.Serializable;
import java.util.List;

/**
 * Frequent subgraph mining configuration.
 */
public class XMDConfig implements Serializable {

  /**
   * support threshold for subgraphs to be considered to be frequenct
   */
  private float minSupport = 1.0f;

  /**
   * Direction mode (true=directed; false=undirected)
   */
  private boolean directed = true;

  /**
   * Dictionary type used for dictionary coding.
   */
  private DictionaryType
    dictionaryType = DictionaryType.INVERSE_PROPORTIONAL;

  /**
   * Flag to set dataflow position of pattern validation.
   */
  private DataflowStep patternVerificationInStep = DataflowStep.COMBINE;

  /**
   * Flag to enable branch constraint in pattern growth (true=enabled).
   */
  private boolean branchConstraintEnabled = true;

  /**
   * valued constructor
   * @param minSupport minimum relative support of a subgraph
   * @param directed direction mode
   */
  public XMDConfig(float minSupport, boolean directed) {
    this.minSupport = minSupport;
    this.directed = directed;
  }

  @Override
  public String toString() {
    List<String> parameters = Lists.newArrayList();

    parameters.add("s_min : " + String.valueOf(minSupport));
    parameters.add((directed ? "directed" : "undirected") + " mode");
    parameters.add("dictionary type : " + dictionaryType.toString());

    parameters.add(getParameterEnabled("branch constraint", branchConstraintEnabled));

    parameters.add("pattern validation @ " + patternVerificationInStep.toString());

    return StringUtils.join(parameters, "|");
  }

  /**
   * Convenience method for string formatting of enum parameters.
   *
   * @param parameter parameter name
   * @param enabled parameter value
   *
   * @return string representation of the parameter and its value
   */
  private String getParameterEnabled(String parameter, boolean enabled) {
    return parameter + " " + (enabled ? "enabled" : "disabled");
  }

  public float getMinSupport() {
    return minSupport;
  }

  public void setMinSupport(float minSupport) {
    this.minSupport = minSupport;
  }

  public boolean isDirected() {
    return directed;
  }

  public void setDirected(boolean directed) {
    this.directed = directed;
  }

  public DictionaryType getDictionaryType() {
    return dictionaryType;
  }

  public void setDictionaryType(DictionaryType dictionaryType) {
    this.dictionaryType = dictionaryType;
  }

  public DataflowStep getPatternVerificationInStep() {
    return patternVerificationInStep;
  }

  public void setPatternVerificationInStep(DataflowStep patternVerificationInStep) {
    this.patternVerificationInStep = patternVerificationInStep;
  }

  public void setBranchConstraintEnabled(boolean branchConstraintEnabled) {
    this.branchConstraintEnabled = branchConstraintEnabled;
  }

  public boolean isBranchConstraintEnabled() {
    return branchConstraintEnabled;
  }
}
