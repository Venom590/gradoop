package org.gradoop.examples.datagen;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.flink.api.common.ProgramDescription;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.datagen.foodbroker.FoodBroker;
import org.gradoop.flink.datagen.foodbroker.config.FoodBrokerConfig;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.util.GradoopFlinkConfig;


public class FoodBrokerRunner extends AbstractRunner
  implements ProgramDescription {

  public static void main(String[] args) throws Exception {

    String configFile = System.getProperty("user.home") +
      "/foodbroker/config.json";
    FoodBrokerConfig config = FoodBrokerConfig.fromFile(configFile);

    Integer scaleFactor = Integer.parseInt(args[0]);
    config.setScaleFactor(scaleFactor);

    FoodBroker foodBroker =
      new FoodBroker(getExecutionEnvironment(),
        GradoopFlinkConfig.createConfig(getExecutionEnvironment()), config);


    GraphCollection cases = foodBroker.execute();

    System.out.println("cases: " + cases.getGraphHeads().count());
  }

  @Override
  public String getDescription() {
    return FoodBrokerRunner.class.getName();
  }
}
