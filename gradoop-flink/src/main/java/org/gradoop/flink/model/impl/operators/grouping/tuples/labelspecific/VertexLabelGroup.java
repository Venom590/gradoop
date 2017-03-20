package org.gradoop.flink.model.impl.operators.grouping.tuples.labelspecific;

import com.google.common.collect.Sets;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Set;

public class VertexLabelGroup extends Tuple2<String, Set<String>> {

  public VertexLabelGroup(String label, Set<String> propertyKeys) {
    super(label, propertyKeys);
  }

  public VertexLabelGroup(String label, String propertyKey) {
    super(label, Sets.newHashSet(propertyKey));
  }

  public String getLabel() {
    return f0;
  }

  public void setLabel(String label) {
    f0 = label;
  }

  public Set<String> getPropertyKeys() {
    return f1;
  }

  public void setPropertyKeys(Set<String> propertyKeys) {
    f1 = propertyKeys;
  }
}
