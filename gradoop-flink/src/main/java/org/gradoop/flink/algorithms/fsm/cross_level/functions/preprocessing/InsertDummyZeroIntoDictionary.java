package org.gradoop.flink.algorithms.fsm.cross_level.functions.preprocessing;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * Created by peet on 11.05.17.
 */
public class InsertDummyZeroIntoDictionary implements MapFunction<String[], String[]> {

  public static final String DUMMY_VALUE = ":";

  @Override
  public String[] map(String[] dictionary) throws Exception {
    return ArrayUtils.addAll(new String[] {DUMMY_VALUE}, dictionary);
  }
}
