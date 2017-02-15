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

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation
  .PropertyValueAggregator;
import org.gradoop.flink.model.impl.operators.grouping.tuples.SuperEdgeGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.tuples.VertexWithSuperVertexAndEdge;

import java.util.List;

/**
 * Creates a new super edge representing an edge group. The edge stores the
 * group label, the group property value and the aggregate values for its group.
 */
@FunctionAnnotation.ForwardedFields("f1->id")
@FunctionAnnotation.ReadFields("f1;f4;f5;f6")
public class BuildSuperEdges
  extends BuildBase
  implements CoGroupFunction<SuperEdgeGroupItem, VertexWithSuperVertexAndEdge, Edge>, ResultTypeQueryable<Edge> {

  /**
   * Edge edgeFactory.
   */
  private final EdgeFactory edgeFactory;

  /**
   * Creates map function.
   *
   * @param groupPropertyKeys edge property key for grouping
   * @param useLabel          true, if vertex label shall be considered
   * @param valueAggregators  aggregate functions for edge values
   * @param edgeFactory     edge factory
   */
  public BuildSuperEdges(List<String> groupPropertyKeys,
    boolean useLabel,
    List<PropertyValueAggregator> valueAggregators,
    EdgeFactory edgeFactory) {
    super(groupPropertyKeys, useLabel, valueAggregators);
    this.edgeFactory = edgeFactory;
  }


  @Override
  public void coGroup(Iterable<SuperEdgeGroupItem> superEdgeGroupItems,
    Iterable<VertexWithSuperVertexAndEdge> vertexWithSuperVertexAndEdges, Collector<Edge> collector) throws
    Exception {

    // only one Edge per id
    SuperEdgeGroupItem superEdgeGroupItem = superEdgeGroupItems.iterator().next();

    GradoopId sourceId = null;
    GradoopId targetId = null;

    for (VertexWithSuperVertexAndEdge vertexWithSuperVertexAndEdge :
      vertexWithSuperVertexAndEdges) {
      if (vertexWithSuperVertexAndEdge.isSource()) {
        sourceId = vertexWithSuperVertexAndEdge.getSuperVertexId();
      } else {
        targetId = vertexWithSuperVertexAndEdge.getSuperVertexId();
      }
    }

    Edge superEdge = edgeFactory.initEdge(superEdgeGroupItem.getSuperEdgeId(), sourceId, targetId);

    setLabel(superEdge, superEdgeGroupItem.getGroupLabel());
    setGroupProperties(superEdge, superEdgeGroupItem.getGroupingValues());
    setAggregateValues(superEdge, superEdgeGroupItem.getAggregateValues());

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
