package org.gradoop.flink.algorithms.fsm.cross_level.vector_mining;

import org.gradoop.flink.model.impl.tuples.WithCount;

import java.util.Collection;

public interface CrossLevelFrequentVectors {

  Collection<WithCount<int[][]>> mine(int[][][] vectors, int minFrequency);
}
