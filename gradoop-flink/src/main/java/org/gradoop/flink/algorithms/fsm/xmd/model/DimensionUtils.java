package org.gradoop.flink.algorithms.fsm.xmd.model;

/**
 * Created by peet on 09.05.17.
 */
public class DimensionUtils {
  private static final int LABEL_INDEX = 0;

  public static <T> T getVertexLabel(T[] data) {
    return data[LABEL_INDEX];
  }
}
