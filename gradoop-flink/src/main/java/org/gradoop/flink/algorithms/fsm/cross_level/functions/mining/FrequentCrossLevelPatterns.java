package org.gradoop.flink.algorithms.fsm.cross_level.functions.mining;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.gradoop.flink.algorithms.fsm.common.config.FSMConstants;
import org.gradoop.flink.algorithms.fsm.cross_level.tuples.PatternVectors;

import java.util.Iterator;

public class FrequentCrossLevelPatterns
  extends RichGroupReduceFunction<PatternVectors, PatternVectors> {

  /**
   * minimum frequency
   */
  private long minFrequency;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    this.minFrequency = getRuntimeContext()
      .<Long>getBroadcastVariable(FSMConstants.MIN_FREQUENCY).get(0);
  }

  @Override
  public void reduce(Iterable<PatternVectors> values, Collector<PatternVectors> out) throws
    Exception {

    Iterator<PatternVectors> iterator = values.iterator();

    PatternVectors patternVectors = iterator.next();

    while (iterator.hasNext()) {
      patternVectors.setVectors(
        ArrayUtils.addAll(patternVectors.getVectors(), iterator.next().getVectors()));
    }

    if (patternVectors.getVectors().length >= minFrequency) {
      out.collect(patternVectors);
    }
  }
}
