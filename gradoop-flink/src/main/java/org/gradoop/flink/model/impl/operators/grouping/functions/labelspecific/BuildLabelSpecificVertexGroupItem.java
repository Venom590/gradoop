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

package org.gradoop.flink.model.impl.operators.grouping.functions.labelspecific;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.api.entities.EPGMAttributed;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.grouping.functions.BuildBase;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation.PropertyValueAggregator;

import org.gradoop.flink.model.impl.operators.grouping.tuples.labelspecific.VertexLabelGroup;
import org.gradoop.flink.model.impl.operators.grouping.tuples.vertexcentric.VertexGroupItem;
import org.gradoop.common.model.impl.properties.PropertyValueList;

import java.io.IOException;
import java.util.List;

/**
 * Creates a minimal representation of vertex data to be used for grouping.
 *
 * The output of that mapper is {@link VertexGroupItem} that contains
 * the vertex id, vertex label, vertex group properties and vertex aggregate
 * properties.
 */
//@FunctionAnnotation.ForwardedFields("id->f0")
//@FunctionAnnotation.ReadFields("label;properties")
public class BuildLabelSpecificVertexGroupItem
  extends BuildBase
  implements FlatMapFunction<Vertex, VertexGroupItem> {


  /**
   * Reduce object instantiations.
   */
  private final VertexGroupItem reuseVertexGroupItem;

  private final List<VertexLabelGroup> vertexLabelGroups;

  /**
   * Creates map function
   *
   * @param vertexLabelGroups vertex labels with property keys
   * @param useLabel          true, if label shall be considered
   * @param vertexAggregators aggregate functions for super vertices
   */
  public BuildLabelSpecificVertexGroupItem(List<String> groupPropertyKeys,
    boolean useLabel, List<PropertyValueAggregator> vertexAggregators,
    List<VertexLabelGroup> vertexLabelGroups) {
    super(groupPropertyKeys, useLabel, vertexAggregators);

    this.vertexLabelGroups = vertexLabelGroups;
    this.reuseVertexGroupItem = new VertexGroupItem();
    this.reuseVertexGroupItem.setSuperVertexId(GradoopId.NULL_VALUE);
    this.reuseVertexGroupItem.setSuperVertex(false);
    if (!doAggregate()) {
      this.reuseVertexGroupItem.setAggregateValues(
        PropertyValueList.createEmptyList());
    }
  }





  @Override
  public void flatMap(Vertex vertex, Collector<VertexGroupItem> collector) throws Exception {
    List<PropertyValue> values =
      Lists.newArrayListWithCapacity(vertex.getPropertyCount());

    boolean usedVertexLabelGroup = false;

    reuseVertexGroupItem.setVertexId(vertex.getId());
    reuseVertexGroupItem.setGroupLabel(getLabel(vertex));
    //TODO check if aggregate have to be separated for each label group
    if (doAggregate()) {
      reuseVertexGroupItem.setAggregateValues(getAggregateValues(vertex));
    }

    for (VertexLabelGroup vertexLabelGroup : vertexLabelGroups) {
//      System.out.println("vertex = " + vertex + " - " + vertexLabelGroup);
      if (vertexLabelGroup.getLabel().equals(vertex.getLabel())) {
        usedVertexLabelGroup = true;
        for (String groupPropertyKey : vertexLabelGroup.getPropertyKeys()) {
          if (vertex.hasProperty(groupPropertyKey)) {
//            System.out.println("vertex = " + vertex.getLabel() + " - " + groupPropertyKey);
            values.add(vertex.getPropertyValue(groupPropertyKey));
          } else {
            values.add(PropertyValue.NULL_VALUE);
          }
        }
        reuseVertexGroupItem.setGroupingValues(PropertyValueList.fromPropertyValues(values));
        collector.collect(reuseVertexGroupItem);
        values.clear();
        System.out.println("reuseVertexGroupItem = " + reuseVertexGroupItem);
      }
    }
    if (!usedVertexLabelGroup) {
      reuseVertexGroupItem.setGroupingValues(getGroupProperties(vertex));
      collector.collect(reuseVertexGroupItem);
      System.out.println("reuseVertexGroupItem = " + reuseVertexGroupItem);
    }
  }
}
