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

package org.gradoop.flink.model.impl.operators.grouping.tuples.edgecentric;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;

public class SuperVertexIdWithVertex extends Tuple2<GradoopId, Vertex> {

  public void setSuperVertexid(GradoopId gradoopId) {
    f0 = gradoopId;
  }

  public GradoopId getSuperVertexid() {
    return f0;
  }

  public void setVertex(Vertex vertex) {
    f1 = vertex;
  }

  public Vertex getVertex() {
    return f1;
  }

}