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

package org.gradoop.model.impl.datagen.foodbroker.foodbrokerage;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.datagen.foodbroker.config.FoodBrokerConfig;
import org.gradoop.util.GradoopFlinkConfig;


public class SalesOrderLineGenerator<V extends EPGMVertex> extends
  AbstractTransactionalDataGenerator<V> {

  private DataSet<V> salesOrders;
  private DataSet<V> salesQuotationLines;

  public SalesOrderLineGenerator(GradoopFlinkConfig gradoopFlinkConfig,
    FoodBrokerConfig foodBrokerConfig, DataSet<V> salesOrders, DataSet<V>
    salesQuotationLines) {
    super(gradoopFlinkConfig, foodBrokerConfig);

    this.salesOrders = salesOrders;
    this.salesQuotationLines = salesQuotationLines;
  }

  @Override
  public DataSet<V> generate() {
    return salesOrders
      .flatMap(new SalesOrderLine(vertexFactory))
      .withBroadcastSet(salesQuotationLines, "");
  }
}
