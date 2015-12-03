package org.gradoop.model.impl.algorithms.fsm.functions;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

/**
 * Created by peet on 02.12.15.
 */
public class Frequent<C> extends RichFilterFunction<Tuple2<C, Long>> {

  public static final String DS_NAME = "minCount";
  private Long minCount;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    this.minCount = getRuntimeContext()
      .<Long>getBroadcastVariable(DS_NAME)
      .get(0);
  }

  @Override
  public boolean filter(Tuple2<C, Long> c) throws Exception {
    return c.f1 >= minCount;
  }
}
