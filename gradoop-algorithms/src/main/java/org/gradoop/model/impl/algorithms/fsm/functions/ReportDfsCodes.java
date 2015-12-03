package org.gradoop.model.impl.algorithms.fsm.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.algorithms.fsm.tuples.CompressedDfsCode;
import org.gradoop.model.impl.algorithms.fsm.tuples.DfsCode;
import org.gradoop.model.impl.algorithms.fsm.tuples.GSpanGraph;

/**
 * Created by peet on 03.12.15.
 */
public class ReportDfsCodes implements FlatMapFunction<GSpanGraph, CompressedDfsCode> {


  @Override
  public void flatMap(GSpanGraph gSpanGraph,
    Collector<CompressedDfsCode> collector) throws Exception {

    for(DfsCode dfsCode : gSpanGraph.getDfsCodes()) {
      collector.collect(new CompressedDfsCode(dfsCode));
    }
}
}
