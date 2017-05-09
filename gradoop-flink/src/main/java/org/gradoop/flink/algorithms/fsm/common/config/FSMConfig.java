package org.gradoop.flink.algorithms.fsm.common.config;

import java.io.Serializable;

/**
 * Created by peet on 09.05.17.
 */
public interface FSMConfig extends Serializable {
  float getMinSupport();

  void setMinSupport(float minSupport);

  boolean isDirected();

  void setDirected(boolean directed);

  DictionaryType getDictionaryType();

  void setDictionaryType(DictionaryType dictionaryType);

  void setBranchConstraintEnabled(boolean branchConstraintEnabled);

  boolean isBranchConstraintEnabled();
}
