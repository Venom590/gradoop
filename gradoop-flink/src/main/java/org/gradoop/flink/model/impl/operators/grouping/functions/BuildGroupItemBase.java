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

package org.gradoop.flink.model.impl.operators.grouping.functions;

import com.google.common.collect.Lists;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.common.model.impl.properties.PropertyValueList;
import org.gradoop.flink.model.impl.operators.grouping.Grouping;
import org.gradoop.flink.model.impl.operators.grouping.tuples.GroupItem;
import org.gradoop.flink.model.impl.operators.grouping.tuples.LabelGroup;

import java.io.IOException;
import java.util.List;

public class BuildGroupItemBase extends BuildBase {

  /**
   * Stores grouping properties and aggregators for vertex labels.
   */
  private final List<LabelGroup> labelGroups;
  /**
   * Stores the information about the default label group, this is either the vertex or the
   * edge default label group.
   */
  private final LabelGroup defaultLabelGroup;

  private List<PropertyValue> groupingValues;

  public BuildGroupItemBase(
    boolean useLabel, List<LabelGroup> labelGroups) {
    super(useLabel);
    this.labelGroups = labelGroups;
    groupingValues = Lists.newArrayList();

    LabelGroup standardLabelGroup = null;
    for (LabelGroup labelGroup : labelGroups) {
      if (labelGroup.getGroupingLabel().equals(Grouping.DEFAULT_VERTEX_LABEL_GROUP)) {
        standardLabelGroup = labelGroup;
      } else if (labelGroup.getGroupingLabel().equals(Grouping.DEFAULT_EDGE_LABEL_GROUP)) {
        standardLabelGroup = labelGroup;
      }
    }

    defaultLabelGroup = standardLabelGroup;

  }

  protected void setGroupItem (GroupItem groupItem, EPGMElement element, LabelGroup labelGroup)
    throws IOException {
    for (String groupPropertyKey : labelGroup.getPropertyKeys()) {
      if (element.hasProperty(groupPropertyKey)) {
        groupingValues.add(element.getPropertyValue(groupPropertyKey));
      } else {
        groupingValues.add(PropertyValue.NULL_VALUE);
      }
    }
    if (labelGroup.getGroupingLabel().equals(Grouping.DEFAULT_VERTEX_LABEL_GROUP) ||
      labelGroup.getGroupingLabel().equals(Grouping.DEFAULT_EDGE_LABEL_GROUP)) {
//      groupItem.setGroupLabel(element.getLabel());
      labelGroup.setGroupLabel(element.getLabel());
    }
//    } else {
      groupItem.setGroupLabel(labelGroup.getGroupLabel());
//    }
    if (doAggregate(labelGroup.getAggregators())) {
      groupItem.setAggregateValues(
        getAggregateValues(element, labelGroup.getAggregators()));
    } else {
      groupItem.setAggregateValues(PropertyValueList.createEmptyList());
    }
    groupItem.setLabelGroup(labelGroup);
    groupItem.setGroupingValues(PropertyValueList.fromPropertyValues(groupingValues));
    groupingValues.clear();
  }



  protected List<LabelGroup> getLabelGroups() {
    return labelGroups;
  }

  protected LabelGroup getDefaultLabelGroup() {
    return defaultLabelGroup;
  }
}
