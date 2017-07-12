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

package org.gradoop.flink.model.impl.operators.grouping.functions.edgecentric;

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.model.impl.operators.grouping.tuples.edgecentric.SuperEdgeGroupItem;

/**
 * Creates a local {@link SuperEdgeGroupItem} which represents one group of super edges based on the
 * grouping settings.
 */
public class CombineSuperEdgeGroupItems
  extends ReduceSuperEdgeGroupItems
  implements GroupCombineFunction<SuperEdgeGroupItem, SuperEdgeGroupItem> {

  /**
   * Creates group combine function.
   *
   * @param useLabel true, iff labels are used for grouping
   * @param sourceSpecificGrouping true if the source vertex shall be considered for grouping
   * @param targetSpecificGrouping true if the target vertex shall be considered for grouping
   */
  public CombineSuperEdgeGroupItems(boolean useLabel, boolean sourceSpecificGrouping,
    boolean targetSpecificGrouping) {
    super(useLabel, sourceSpecificGrouping, targetSpecificGrouping);
  }

  @Override
  public void combine(Iterable<SuperEdgeGroupItem> values,
    Collector<SuperEdgeGroupItem> out) throws Exception {
    reduce(values, out);
  }
}
