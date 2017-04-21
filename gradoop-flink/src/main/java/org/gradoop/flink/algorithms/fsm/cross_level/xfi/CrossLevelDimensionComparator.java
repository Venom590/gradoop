package org.gradoop.flink.algorithms.fsm.cross_level.xfi;

import java.util.Comparator;

public class CrossLevelDimensionComparator implements Comparator<int[]>{

  @Override
  public int compare(int[] a, int[] b) {

    int comparison = a.length - b.length;

    if (comparison == 0) {
      for (int i = 0; i < a.length; i++) {
        comparison = a[i] - b[i];

        if (comparison != 0) {
          break;
        }
      }
    } else {
      throw new IllegalArgumentException("Fields must belong to hierarchies of same depth.");
    }

    return comparison;
  }
}
