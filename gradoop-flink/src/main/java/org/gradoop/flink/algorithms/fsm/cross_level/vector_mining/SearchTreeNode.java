/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

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
