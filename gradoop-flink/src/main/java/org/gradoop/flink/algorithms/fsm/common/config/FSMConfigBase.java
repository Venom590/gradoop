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

package org.gradoop.flink.algorithms.fsm.common.config;


public abstract class FSMConfigBase implements FSMConfig {
  /**
   * support threshold for subgraphs to be considered to be frequenct
   */
  protected float minSupport = 1.0f;
  /**
   * Direction mode (true=directed; false=undirected)
   */
  protected boolean directed = true;
  /**
   * Dictionary type used for dictionary coding.
   */
  protected DictionaryType dictionaryType = DictionaryType.INVERSE_PROPORTIONAL;
  /**
   * Flag to enable branch constraint in pattern growth (true=enabled).
   */
  protected boolean branchConstraintEnabled = true;

  public FSMConfigBase(float minSupport, boolean directed) {
    this.minSupport = minSupport;
    this.directed = directed;
  }

  /**
   * Convenience method for string formatting of enum parameters.
   *
   * @param parameter parameter name
   * @param enabled parameter value
   *
   * @return string representation of the parameter and its value
   */
  protected String getParameterEnabled(String parameter, boolean enabled) {
    return parameter + " " + (enabled ? "enabled" : "disabled");
  }

  @Override
  public float getMinSupport() {
    return minSupport;
  }

  @Override
  public void setMinSupport(float minSupport) {
    this.minSupport = minSupport;
  }

  @Override
  public boolean isDirected() {
    return directed;
  }

  @Override
  public void setDirected(boolean directed) {
    this.directed = directed;
  }

  @Override
  public DictionaryType getDictionaryType() {
    return dictionaryType;
  }

  @Override
  public void setDictionaryType(DictionaryType dictionaryType) {
    this.dictionaryType = dictionaryType;
  }

  @Override
  public void setBranchConstraintEnabled(boolean branchConstraintEnabled) {
    this.branchConstraintEnabled = branchConstraintEnabled;
  }

  @Override
  public boolean isBranchConstraintEnabled() {
    return branchConstraintEnabled;
  }
}
