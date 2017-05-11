package org.gradoop.flink.algorithms.fsm.cross_level.vector_mining;


import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;

import java.util.List;

public class SearchTreeNode {
  private final int[][] pattern;
  private final int[] occurrences;

  public SearchTreeNode(int[][] pattern, int[] occurrences) {
    this.pattern = pattern;
    this.occurrences = occurrences;
  }

  public int[] getOccurrences() {
    return occurrences;
  }

  public int[][] getPattern() {
    return pattern;
  }

  public int getFrequency() {
    return occurrences.length;
  }

  @Override
  public String toString() {

    List<String> dims = Lists.newArrayList();

    for (int[] dim : pattern) {
      List<String> levels = Lists.newArrayList();

      for (int l:dim) {
        levels.add(String.valueOf(l));
      }
      dims.add("[" + StringUtils.join(levels, ',') + "]");
    }

    return "[" + StringUtils.join(dims, ",") + "]";
  }
}
