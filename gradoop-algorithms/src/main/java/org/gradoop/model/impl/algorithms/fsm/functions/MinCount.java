package org.gradoop.model.impl.algorithms.fsm.functions;

import org.apache.flink.api.common.functions.MapFunction;

public class MinCount implements MapFunction<Long, Long> {

  private final float threshold;

  public MinCount(float threshold) {
    this.threshold = threshold;
  }

  @Override
  public Long map(Long totalCount) throws Exception {
    return (long) Math.round(((float) totalCount * threshold));
  }
}
