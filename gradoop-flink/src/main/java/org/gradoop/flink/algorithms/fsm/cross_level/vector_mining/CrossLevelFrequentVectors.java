package org.gradoop.flink.algorithms.fsm.cross_level.vector_mining;

import org.gradoop.flink.model.impl.tuples.WithCount;

import java.io.Serializable;
import java.util.Collection;

public interface CrossLevelFrequentVectors extends Serializable {

  Collection<WithCount<int[][]>> mine(int[][][] vectors, int minFrequency);
}
