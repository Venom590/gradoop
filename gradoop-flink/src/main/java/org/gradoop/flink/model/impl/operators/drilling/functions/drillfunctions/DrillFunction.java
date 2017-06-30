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

package org.gradoop.flink.model.impl.operators.drilling.functions.drillfunctions;

import org.gradoop.common.model.impl.properties.PropertyValue;

import java.io.Serializable;

/**
 * Interface for all drill functions which are used for drill down / roll up operations.
 */
public interface DrillFunction extends Serializable {

  /**
   * Returns a changed property value based in the property before drilling.
   *
   * @param property current property
   * @return property after drilling
   */
  PropertyValue execute(PropertyValue property);

}