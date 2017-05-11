package org.gradoop.flink.util;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class ExpandArray<T> implements FlatMapFunction<T[], T> {

  @Override
  public void flatMap(T[] array, Collector<T> out) throws Exception {

    for (T value : array) {
      out.collect(value);
    }
  }
}
