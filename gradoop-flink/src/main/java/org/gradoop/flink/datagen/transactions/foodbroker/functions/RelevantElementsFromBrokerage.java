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

package org.gradoop.flink.datagen.transactions.foodbroker.functions;

/**
 * Filters all vertices and edges from the graph transaction which are relevant for the complaint
 * handling process.
 */
public class RelevantElementsFromBrokerage {
//  implements MapFunction<GraphTransaction, GraphTransaction> {
//
//  @Override
//  public GraphTransaction map(GraphTransaction graph) throws Exception {
//    //take only vertices which have the needed label
//    Set<Vertex> vertices = Sets.newHashSet();
//    for (Vertex vertex : graph.getVertices()) {
//      if (vertex.getLabel().equals(Constants.DELIVERYNOTE_VERTEX_LABEL) ||
//        vertex.getLabel().equals(Constants.SALESORDER_VERTEX_LABEL)) {
//        vertices.add(vertex);
//      }
//    }
//    //take only edges which have the needed label
//    Set<String> edgelabels = Sets.newHashSet(
//      Constants.CONTAINS_EDGE_LABEL,
//      Constants.OPERATEDBY_EDGE_LABEL,
//      Constants.PLACEDAT_EDGE_LABEL,
//      Constants.RECEIVEDFROM_EDGE_LABEL,
//      Constants.SALESORDERLINE_EDGE_LABEL,
//      Constants.PURCHORDERLINE_EDGE_LABEL);
//    Set<Edge> edges = Sets.newHashSet();
//    for (Edge edge : graph.getEdges()) {
//      if (edgelabels.contains(edge.getLabel())) {
//        edges.add(edge);
//      }
//    }
//    //vertices is empty if the sales quotation has not been confirmed during the brokerage process
//    if (!vertices.isEmpty()) {
//      //store only relevant information in the graph transaction
//      graph.setVertices(vertices);
//      graph.setEdges(edges);
//      collector.collect(graph);
//    }
//  }
}
