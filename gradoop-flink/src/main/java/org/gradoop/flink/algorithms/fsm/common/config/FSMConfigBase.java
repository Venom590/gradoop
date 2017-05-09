package org.gradoop.flink.algorithms.fsm.common.config;

import org.gradoop.flink.algorithms.fsm.dimspan.config.DictionaryType;

import java.io.Serializable;

public abstract class FSMConfigBase implements Serializable {
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

  public void setBranchConstraintEnabled(boolean branchConstraintEnabled) {
    this.branchConstraintEnabled = branchConstraintEnabled;
  }

  public boolean isBranchConstraintEnabled() {
    return branchConstraintEnabled;
  }
}
