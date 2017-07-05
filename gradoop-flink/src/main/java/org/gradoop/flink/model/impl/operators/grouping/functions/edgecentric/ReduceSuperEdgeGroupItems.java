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

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.grouping.tuples.edgecentric.SuperEdgeGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.tuples.edgecentric.SuperEdgeGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation.PropertyValueAggregator;
import org.gradoop.common.model.impl.properties.PropertyValueList;

import java.util.List;
import java.util.Set;

/**
   * Reduces a group of {@link SuperEdgeGroupItem} instances.
   */
 // @FunctionAnnotation.ForwardedFields(
 //   "f0;" + // edge id
 //     "f3;" + // label
 //     "f4"    // properties
 // )
  public class ReduceSuperEdgeGroupItems
    extends ReduceSuperEdgeGroupItemBase
    implements GroupReduceFunction<SuperEdgeGroupItem, SuperEdgeGroupItem> {

    private SuperEdgeGroupItem reuseSuperEdgeGroupItem;

    /**
     * Creates group reduce function.
     *
     * @param useLabel          true, iff labels are used for grouping
     */
    public ReduceSuperEdgeGroupItems(boolean useLabel, boolean sourceSpecificGrouping,
      boolean targetSpecificGrouping) {
      super(useLabel, sourceSpecificGrouping, targetSpecificGrouping);
    }

    @Override
    public void reduce(Iterable<SuperEdgeGroupItem> superEdgeGroupItems,
      Collector<SuperEdgeGroupItem> collector) throws Exception {

      boolean isFirst                       = true;

      Set<GradoopId> sources = Sets.newHashSet();
      Set<GradoopId> targets = Sets.newHashSet();

      for (SuperEdgeGroupItem groupItem : superEdgeGroupItems) {
        // grouped by source and target
        if (isSourceSpecificGrouping() && isTargetSpecificGrouping()) {
          if (isFirst) {
            sources.add(groupItem.getSourceId());
            targets.add(groupItem.getTargetId());
          }
        // grouped by source, targets may vary
        } else if (isSourceSpecificGrouping()) {
          if (isFirst) {
            sources.add(groupItem.getSourceId());
          }
          targets.add(groupItem.getTargetId());
        // grouped by target, sources may vary
        } else if (isTargetSpecificGrouping()) {
          if (isFirst) {
            targets.add(groupItem.getTargetId());
          }
          sources.add(groupItem.getSourceId());
        // source or target do not have influence to the grouping
        } else {
          sources.add(groupItem.getSourceId());
          targets.add(groupItem.getTargetId());
        }
        if (isFirst) {
          reuseSuperEdgeGroupItem = groupItem;
          reuseSuperEdgeGroupItem.setEdgeId(GradoopId.get());
          isFirst = false;
        }
        if (doAggregate(groupItem.getLabelGroup().getAggregators())) {
          aggregate(groupItem.getAggregateValues(), reuseSuperEdgeGroupItem.getLabelGroup().getAggregators());
        }

      }
      // collect single item representing the whole group
      reuseSuperEdgeGroupItem.setSourceIds(sources);
      reuseSuperEdgeGroupItem.setTargetIds(targets);
      reuseSuperEdgeGroupItem.setAggregateValues(getAggregateValues(reuseSuperEdgeGroupItem.getLabelGroup()
        .getAggregators()));
      resetAggregators(reuseSuperEdgeGroupItem.getLabelGroup().getAggregators());
      collector.collect(reuseSuperEdgeGroupItem);
    }
  }