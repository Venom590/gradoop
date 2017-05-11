package org.gradoop.flink.algorithms.fsm.cross_level.functions.mining;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.flink.algorithms.fsm.common.gspan.GSpanLogic;
import org.gradoop.flink.algorithms.fsm.cross_level.tuples.MultilevelGraph;
import org.gradoop.flink.algorithms.fsm.cross_level.tuples.PatternVectors;
import org.gradoop.flink.algorithms.fsm.dimspan.model.Simple16Compressor;

import java.util.Iterator;

public class CombineVectors
  implements GroupCombineFunction<MultilevelGraph, PatternVectors> {

  private final GSpanLogic gSpan;

  public CombineVectors(GSpanLogic gSpan) {
    this.gSpan = gSpan;
  }

  @Override
  public void combine(Iterable<MultilevelGraph> values, Collector<PatternVectors> out)
    throws Exception {

    Iterator<MultilevelGraph> iterator = values.iterator();

    MultilevelGraph multilevelPattern = iterator.next();

    int[] compressedPattern = multilevelPattern.getGraph();
    int[] pattern = Simple16Compressor.uncompress(compressedPattern);

    int[][][] vectors = new int[][][] {multilevelPattern.getVector()};

    if (pattern.length == 6 || gSpan.isMinimal(pattern)) {

      while (iterator.hasNext()) {
        vectors = ArrayUtils.add(vectors, iterator.next().getVector());
      }

      out.collect(new PatternVectors(compressedPattern, vectors));
    }
  }
}
