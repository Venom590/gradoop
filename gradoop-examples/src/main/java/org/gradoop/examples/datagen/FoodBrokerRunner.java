package org.gradoop.examples.datagen;

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

    long time = 0;

    for (int i = 0; i < 10; i++) {
      FoodBroker foodBroker =
        new FoodBroker(getExecutionEnvironment(),
          GradoopFlinkConfig.createConfig(getExecutionEnvironment()), config);

      GraphCollection cases = foodBroker.execute();
      cases.getGraphHeads().count();
      time += getExecutionEnvironment().getLastJobExecutionResult()
        .getNetRuntime();
    }

    System.out.println("Average runtime = " + (time / 10));


  }

  @Override
  public String getDescription() {
    return FoodBrokerRunner.class.getName();
  }
}
