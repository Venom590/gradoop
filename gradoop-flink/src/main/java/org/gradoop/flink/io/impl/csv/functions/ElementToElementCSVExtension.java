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
import org.gradoop.flink.io.impl.csv.CSVConstants;
import org.gradoop.flink.io.impl.csv.pojos.CsvExtension;

import java.util.List;


public class ElementToElementCSVExtension<T extends Element>
  implements MapFunction<T, Tuple2<T, CsvExtension>> {

  private List<Tuple2<CsvExtension, String>> csvList;

  public ElementToElementCSVExtension(List<Tuple2<CsvExtension, String>> csvList) {
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

    for (Tuple2<CsvExtension, String> tuple : csvList) {
      CsvExtension csvExtension = tuple.f0;
      String baseKey = createBaseKey(
        csvExtension.getDatasourceName(), csvExtension.getDomainName(), tuple.f1);
      if (key.startsWith(baseKey)) {
        return new Tuple2<T, CsvExtension>(element, csvExtension);
      }
    }
    return null;
  }


  private String createBaseKey(String datasource, String domain, String clazz) {
    StringBuilder baseKey = new StringBuilder();
    baseKey.append(domain.replaceAll(CSVConstants.SEPARATOR_KEY,
      CSVConstants.ESCAPE_SEPARATOR_KEY));
    baseKey.append(CSVConstants.SEPARATOR_KEY);
    baseKey.append(datasource.replaceAll(
      CSVConstants.SEPARATOR_KEY, CSVConstants.ESCAPE_SEPARATOR_KEY));
    baseKey.append(CSVConstants.SEPARATOR_KEY);
    baseKey.append(clazz.replaceAll(
      CSVConstants.SEPARATOR_KEY, CSVConstants.ESCAPE_SEPARATOR_KEY));
    return baseKey.toString();
  }
}
