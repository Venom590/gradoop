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

package org.gradoop.flink.model.impl.operators.grouping.functions;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation.PropertyValueAggregator;
import org.gradoop.flink.model.impl.operators.grouping.tuples.VertexGroupItem;
import org.gradoop.common.model.impl.properties.PropertyValueList;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Base class for reducer/combiner implementations on vertices.
 */
abstract class ReduceVertexGroupItemBase extends BuildBase {
  /**
   * Reduce instantiations
   */
  private final VertexGroupItem reuseVertexGroupItem;

  /**
   * Creates build base.
   *
   * @param useLabel                        true, if element label shall be used for grouping
   */
  protected ReduceVertexGroupItemBase(boolean useLabel) {
    super(useLabel);
    this.reuseVertexGroupItem = new VertexGroupItem();
  }

  protected VertexGroupItem getReuseVertexGroupItem() {
    return this.reuseVertexGroupItem;
  }

  /**
   * Creates one super vertex tuple representing the whole group. This tuple is
   * later used to create a super vertex for each group.
   *
   * @param superVertexId       super vertex id
   * @param groupLabel          group label
   * @param groupPropertyValues group property values
   * @return vertex group item representing the super vertex
   */
  protected VertexGroupItem createSuperVertexTuple(
    GradoopId superVertexId, String groupLabel,
    PropertyValueList groupPropertyValues,
    List<PropertyValueAggregator> valueAggregators) throws IOException {
    reuseVertexGroupItem.setVertexId(superVertexId);
    reuseVertexGroupItem.setGroupLabel(groupLabel);
    reuseVertexGroupItem.setGroupingValues(groupPropertyValues);
    reuseVertexGroupItem.setAggregateValues(getAggregateValues(valueAggregators));
    reuseVertexGroupItem.setSuperVertex(true);
    return reuseVertexGroupItem;
  }
}
