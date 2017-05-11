package org.gradoop.flink.util;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.flink.model.impl.tuples.WithCount;


public class MakeCountable<T> implements MapFunction<T, WithCount<T>> {

  private final WithCount<T> reuseTuple = new WithCount<>(null, 1);

  @Override
  public WithCount<T> map(T value) throws Exception {
    reuseTuple.setObject(value);
    return reuseTuple;
  }
}
