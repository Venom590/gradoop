package org.gradoop.flink.datagen.transactions.foodbroker.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;

import java.util.Iterator;

/**
 * Created by peet on 20.06.17.
 */
public class TargetGraphIdList
  implements GroupReduceFunction<Tuple2<GradoopId, GradoopId>, Tuple2<GradoopId, GradoopIdList>> {

  @Override
  public void reduce(Iterable<Tuple2<GradoopId, GradoopId>> values,
    Collector<Tuple2<GradoopId, GradoopIdList>> out) throws Exception {

    Iterator<Tuple2<GradoopId, GradoopId>> iterator = values.iterator();

    Tuple2<GradoopId, GradoopId> pair = iterator.next();

    GradoopId targetId = pair.f0;
    GradoopIdList graphIds = GradoopIdList.fromExisting(pair.f1);

    while (iterator.hasNext()) {
      graphIds.add(iterator.next().f1);
    }

    out.collect(new Tuple2<>(targetId, graphIds));
  }
}
