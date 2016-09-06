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
package org.gradoop.flink.datagen.foodbroker.masterdata;


import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.model.impl.properties.PropertyList;
import org.gradoop.flink.datagen.foodbroker.config.Constants;
import org.gradoop.flink.datagen.foodbroker.config.FoodBrokerConfig;
import org.gradoop.flink.datagen.foodbroker.tuples.MasterDataSeed;

import java.math.BigDecimal;
import java.util.List;
import java.util.Random;

public class Product
  extends RichMapFunction<MasterDataSeed, Vertex> {

  public static final String CLASS_NAME = "Product";
  public static final String NAMES_GROUPS_BC = "nameGroupPairs";
  public static final String ADJECTIVES_BC = "adjectives";
  private static final String ACRONYM = "PRD";

  private List<Tuple2<String, String>> nameGroupPairs;
  private List<String> adjectives;

  private Integer nameGroupPairCount;
  private Integer adjectiveCount;

  private final VertexFactory vertexFactory;

  private FoodBrokerConfig config;

  public Product(VertexFactory vertexFactory, FoodBrokerConfig config) {
    this.vertexFactory = vertexFactory;
    this.config = config;
  }


  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    nameGroupPairs = getRuntimeContext().getBroadcastVariable(NAMES_GROUPS_BC);
    adjectives = getRuntimeContext().getBroadcastVariable(ADJECTIVES_BC);

    nameGroupPairCount = nameGroupPairs.size();
    adjectiveCount = adjectives.size();
  }

  @Override
  public Vertex map(MasterDataSeed seed) throws  Exception {
    PropertyList properties = MasterData.createDefaultProperties(ACRONYM, seed);

    Random random = new Random();

    Tuple2<String, String> nameGroupPair = nameGroupPairs
      .get(random.nextInt(nameGroupPairCount));

    properties.set("category", nameGroupPair.f1);

    properties.set("name",
      adjectives.get(random.nextInt(adjectiveCount)) +
      " " + nameGroupPair.f0);
    this.setPrice(properties);

    return vertexFactory.createVertex(Product.CLASS_NAME, properties);
  }

  private void setPrice(PropertyList properties) {
//    PropertiesConfiguration config = null;
//    try {
//      config = new PropertiesConfiguration("config.properties");
//    } catch (ConfigurationException e) {
//      e.printStackTrace();
//    }

    float minPrice = config.getProductMinPrice();
    float maxPrice = config.getProductMaxPrice();

    BigDecimal price = BigDecimal.valueOf(
      minPrice + (float) (Math.random() * ((1 + maxPrice) - minPrice))
    ).setScale(2, BigDecimal.ROUND_HALF_UP);

    properties.set(Constants.PRICE,price);
  }
}