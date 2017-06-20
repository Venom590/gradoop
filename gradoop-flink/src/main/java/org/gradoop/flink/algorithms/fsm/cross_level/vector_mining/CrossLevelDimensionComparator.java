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

import java.io.Serializable;
import java.util.Comparator;

public class CrossLevelDimensionComparator implements Comparator<int[]>, Serializable {

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
