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

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.api.entities.EPGMEdgeFactory;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.util.GConstants;
import org.gradoop.flink.model.impl.operators.grouping.tuples.EdgeGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation.PropertyValueAggregator;


import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Creates a new super edge representing an edge group. The edge stores the
 * group label, the group property value and the aggregate values for its group.
 */
@FunctionAnnotation.ForwardedFields("f0->sourceId;f1->targetId")
public class ReduceEdgeGroupItems
  extends BuildSuperEdge
  implements GroupReduceFunction<EdgeGroupItem, Edge>, ResultTypeQueryable<Edge> {

  /**
   * Edge factory.
   */
  private final EPGMEdgeFactory<Edge> edgeFactory;

  /**
   * Creates group reducer
   *
//   * @param groupPropertyKeys               edge property keys
   * @param useLabel                        use edge label
//   * @param valueAggregators                aggregate functions for edge values
   * @param edgeFactory                     edge factory
//   * @param labelWithAggregatorPropertyKeys stores all aggregator property keys for each label
   */
  public ReduceEdgeGroupItems(boolean useLabel, EPGMEdgeFactory<Edge> epgmEdgeFactory) {
    super(useLabel);
    this.edgeFactory = epgmEdgeFactory;
  }

  /**
   * Reduces edge group items to a single edge group item, creates a new
   * super EPGM edge and collects it.
   *
   * @param edgeGroupItems  edge group items
   * @param collector       output collector
   * @throws Exception
   */
  @Override
  public void reduce(Iterable<EdgeGroupItem> edgeGroupItems,
    Collector<Edge> collector) throws Exception {

    EdgeGroupItem edgeGroupItem = reduceInternal(edgeGroupItems);
    String groupLabel;

    if (useLabel()) {
      groupLabel = edgeGroupItem.getGroupLabel();
    } else {
      groupLabel = GConstants.DEFAULT_EDGE_LABEL;
    }

    Edge superEdge = edgeFactory.createEdge(
      groupLabel,
      edgeGroupItem.getSourceId(),
      edgeGroupItem.getTargetId());

    setGroupProperties(
      superEdge, edgeGroupItem.getGroupingValues(), edgeGroupItem.getLabelGroup());
    setAggregateValues(superEdge, edgeGroupItem.getLabelGroup().getAggregators());
    resetAggregators(edgeGroupItem.getLabelGroup().getAggregators());

    collector.collect(superEdge);
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public TypeInformation<Edge> getProducedType() {
    return TypeExtractor.createTypeInfo(edgeFactory.getType());
  }
}
