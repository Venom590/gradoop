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

package org.gradoop.flink.io.impl.csv.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.impl.csv.pojos.CsvExtension;

import java.util.List;


public class EPGMElementToEPGMElementCSVExtension<T extends Element>
  implements MapFunction<T, Tuple2<T, CsvExtension>> {

  private List<CsvExtension> csvList;

  public EPGMElementToEPGMElementCSVExtension(List<CsvExtension> csvList) {
    this.csvList = csvList;
  }

  @Override
  public Tuple2<T, CsvExtension> map(T element) throws Exception {
    String key;
    if (element.hasProperty("key")) {
      key = element.getPropertyValue("key").getString();
      //remove property key
    } else {
      // try to create key
      key = "datasource;" + "domain;" + "class;" + element.getId().toString();
    }

    String csvKey;
    int type = -1;//0 = graph head, 1 = vertex, 2 = edge
    if (GraphHead.class.isInstance(element)) {
      type = 0;
    }
    if (Vertex.class.isInstance(element)) {
      type = 1;
    }
    if (Edge.class.isInstance(element)) {
      type = 2;
    }
    for (CsvExtension csvExtension : csvList) {
      switch (type) {
        case 0 :
          if (key.startsWith(csvExtension.getGraphhead().getKey().getClazz())) {
            return new Tuple2<T, CsvExtension>(element, csvExtension);
          }
          break;
      case 1 :
        if (key.startsWith(csvExtension.getVertex().getKey().getClazz())) {
          return new Tuple2<T, CsvExtension>(element, csvExtension);
        }
        break;
      case 2 :
        if (key.startsWith(csvExtension.getEdge().getKey().getClazz())) {
          return new Tuple2<T, CsvExtension>(element, csvExtension);
        }
        break;
      }
    }
    return null;
  }

}
