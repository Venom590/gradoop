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

package org.gradoop.flink.model.impl.operators.grouping;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.tuple.Value0Of2;
import org.gradoop.flink.model.impl.functions.tuple.Value1Of2;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation
  .PropertyValueAggregator;
import org.gradoop.flink.model.impl.operators.grouping.functions.labelspecific
  .BuildLabelSpecificVertexGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.functions.vertexcentric.BuildSuperVertex;
import org.gradoop.flink.model.impl.operators.grouping.functions.vertexcentric.BuildVertexGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.functions.vertexcentric
  .BuildVertexWithSuperVertex;
import org.gradoop.flink.model.impl.operators.grouping.functions.vertexcentric
  .BuildVertexWithSuperVertexBC;
import org.gradoop.flink.model.impl.operators.grouping.functions.vertexcentric
  .CombineVertexGroupItems;
import org.gradoop.flink.model.impl.operators.grouping.functions.vertexcentric
  .FilterRegularVertices;
import org.gradoop.flink.model.impl.operators.grouping.functions.vertexcentric.FilterSuperVertices;
import org.gradoop.flink.model.impl.operators.grouping.functions.vertexcentric
  .ReduceVertexGroupItems;
import org.gradoop.flink.model.impl.operators.grouping.functions.vertexcentric
  .TransposeVertexGroupItems;
import org.gradoop.flink.model.impl.operators.grouping.tuples.VertexWithSuperVertex;
import org.gradoop.flink.model.impl.operators.grouping.tuples.labelspecific.VertexLabelGroup;
import org.gradoop.flink.model.impl.operators.grouping.tuples.vertexcentric.VertexGroupItem;
import org.gradoop.flink.model.impl.tuples.IdWithIdSet;

import java.util.List;
import java.util.Objects;

/**
 *
 */
public class LabelSpecificGrouping extends VertexCentricalGrouping {


  private final List<VertexLabelGroup> vertexLabelGroups;

  public LabelSpecificGrouping(List<String> vertexGroupingKeys, boolean useVertexLabels,
    List<PropertyValueAggregator> vertexAggregators, List<String> edgeGroupingKeys,
    boolean useEdgeLabels, List<PropertyValueAggregator> edgeAggregators,
    GroupingStrategy groupingStrategy, List<VertexLabelGroup> vertexLabelGroups) {
    super(vertexGroupingKeys, useVertexLabels, vertexAggregators, edgeGroupingKeys,
      useEdgeLabels, edgeAggregators, groupingStrategy);

    this.vertexLabelGroups = vertexLabelGroups;
    Objects.requireNonNull(getVertexGroupingKeys(), "missing vertex grouping key(s)");
    Objects.requireNonNull(getGroupingStrategy(), "missing vertex grouping strategy");
  }


  public List<VertexLabelGroup> getVertexLabelGroups() {
    return vertexLabelGroups;
  }

  @Override
  protected DataSet<VertexGroupItem> getVerticesForGrouping(DataSet<Vertex> vertices) {
    return vertices
      // map vertex to vertex group item
      .flatMap(new BuildLabelSpecificVertexGroupItem(getVertexGroupingKeys(), useVertexLabels(),
        getVertexAggregators(), vertexLabelGroups));
  }

  protected boolean useVertexProperties() {
    return !getVertexGroupingKeys().isEmpty() || !getVertexLabelGroups().isEmpty();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return LabelSpecificGrouping.class.getName() + ":" + getGroupingStrategy();
  }
}
