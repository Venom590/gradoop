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

package org.gradoop.flink.algorithms.fsm.dimspan.config;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.gradoop.flink.algorithms.fsm.common.config.DataflowStep;
import org.gradoop.flink.algorithms.fsm.common.config.FSMConfigBase;

import java.util.List;

/**
 * Frequent subgraph mining configuration.
 */
public class DIMSpanConfig extends FSMConfigBase {

  /**
   * Flag to enable embedding compression (true=enabled).
   */
  private boolean embeddingCompressionEnabled = true;

  /**
   * Flag to enable graph compression (true=enabled).
   */
  private boolean graphCompressionEnabled = true;

  /**
   * Flag to set dataflow position of pattern compression.
   */
  private DataflowStep patternCompressionInStep = DataflowStep.MAP;

  /**
   * Flag to set dataflow position of pattern validation.
   */
  private DataflowStep patternVerificationInStep = DataflowStep.COMBINE;

  /**
   * valued constructor
   * @param minSupport minimum relative support of a subgraph
   * @param directed direction mode
   */
  public DIMSpanConfig(float minSupport, boolean directed) {
    super(minSupport, directed);
  }

  @Override
  public String toString() {
    List<String> parameters = Lists.newArrayList();

    parameters.add("s_min : " + String.valueOf(minSupport));
    parameters.add((directed ? "directed" : "undirected") + " mode");
    parameters.add("dictionary type : " + dictionaryType.toString());

    parameters.add(getParameterEnabled("branch constraint", branchConstraintEnabled));
    parameters.add(getParameterEnabled("graph compression", graphCompressionEnabled));
    parameters.add(
      getParameterEnabled("embedding compression", embeddingCompressionEnabled));
    parameters.add("pattern compression @ " + patternCompressionInStep.toString());
    parameters.add("pattern validation @ " + patternVerificationInStep.toString());

    return StringUtils.join(parameters, "|");
  }

  public boolean isEmbeddingCompressionEnabled() {
    return embeddingCompressionEnabled;
  }

  public void setEmbeddingCompressionEnabled(boolean embeddingCompressionEnabled) {
    this.embeddingCompressionEnabled = embeddingCompressionEnabled;
  }

  public boolean isGraphCompressionEnabled() {
    return graphCompressionEnabled;
  }

  public void setGraphCompressionEnabled(boolean graphCompressionEnabled) {
    this.graphCompressionEnabled = graphCompressionEnabled;
  }

  public void setPatternCompressionInStep(DataflowStep patternCompressionInStep) {
    this.patternCompressionInStep = patternCompressionInStep;
  }

  public DataflowStep getPatternCompressionInStep() {
    return patternCompressionInStep;
  }

  public DataflowStep getPatternVerificationInStep() {
    return patternVerificationInStep;
  }

  public void setPatternVerificationInStep(DataflowStep patternVerificationInStep) {
    this.patternVerificationInStep = patternVerificationInStep;
  }

}
