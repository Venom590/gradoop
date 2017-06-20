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

package org.gradoop.flink.algorithms.fsm.cross_level.functions.preprocessing;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * Created by peet on 11.05.17.
 */
public class InsertDummyZeroIntoDictionary implements MapFunction<String[], String[]> {

  public static final String DUMMY_VALUE = ":";

  @Override
  public String[] map(String[] dictionary) throws Exception {
    return ArrayUtils.addAll(new String[] {DUMMY_VALUE}, dictionary);
  }
}
