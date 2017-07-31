/**
 * Copyright © 2014 - 2017 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.model.impl.operators.drilling;

import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.drilling.functions.transformations.DrillUpTransformation;
import org.gradoop.flink.model.impl.operators.drilling.functions.drillfunctions.DrillFunction;

/**
 * Creates a graph with the same structure but a specified property of an element is drilled up
 * by the declared function. It is possible to drill up either on one vertex / edge type or all
 * vertex / edge types. Additionally the drilled up value can be stored under a new key. If the
 * original key shall be reused the old value is stored under the key 'key__x' where 'x' is a
 * version number. This number increases on every continuous drill up call where the highest
 * number is the level direct below the drilled up one..
 */
public class DrillUp extends Drill {

  /**
   * Valued constructor.
   *
   * @param label          label of the element whose property shall be drilled, or
   *                       see {@link Drill#DRILL_ALL_ELEMENTS}
   * @param propertyKey    property key
   * @param function       drill function which shall be applied to a property
   * @param newPropertyKey new property key, or see {@link Drill#KEEP_CURRENT_PROPERTY_KEY}
   * @param drillVertex    true, if vertices shall be drilled, false for edges
   */
  public DrillUp(
    String label, String propertyKey, DrillFunction function, String newPropertyKey,
    boolean drillVertex) {
    super(label, propertyKey, function, newPropertyKey, drillVertex);
  }


  @Override
  public LogicalGraph execute(LogicalGraph graph) {
    if (isDrillVertex()) {
      graph = graph.transformVertices(
        new DrillUpTransformation<Vertex>(getLabel(), getPropertyKey(), getFunction(),
          getNewPropertyKey()));
    } else {
      graph = graph.transformEdges(
        new DrillUpTransformation<Edge>(getLabel(), getPropertyKey(), getFunction(),
          getNewPropertyKey()));
    }
    return graph;
  }

  @Override
  public String getName() {
    return DrillUp.class.getName();
  }

}