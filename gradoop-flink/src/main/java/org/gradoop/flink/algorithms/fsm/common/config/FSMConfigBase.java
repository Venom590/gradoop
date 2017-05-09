package org.gradoop.flink.algorithms.fsm.common.config;

import java.io.Serializable;

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
