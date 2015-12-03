package org.gradoop.model.impl.algorithms.fsm.tuples;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.flink.api.java.tuple.Tuple1;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * represents a compressed DFS code
 */
public class CompressedDfsCode extends Tuple1<byte[]> {

  /**
   * default constructor
   */
  public CompressedDfsCode() {

  }

  /**
   * constructor compressing an existing DFS code
   * @param dfsCode existing DFS code
   */
  public CompressedDfsCode(DfsCode dfsCode) {

    dfsCode.setPaths(new DfsPath[0]);

    try {
      ByteArrayOutputStream byteArrayOS = new ByteArrayOutputStream();
      GZIPOutputStream gzipOS = new GZIPOutputStream(byteArrayOS);
      ObjectOutputStream objectOS = new ObjectOutputStream(gzipOS);
      objectOS.writeObject(dfsCode);
      objectOS.close();
      this.f0 = byteArrayOS.toByteArray();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * uncompressed the compressed DFS code
   * @return original DFS code
   */
  public DfsCode decompress() {

    DfsCode dfsCode = null;

    try {
      ByteArrayInputStream byteArrayIS = new ByteArrayInputStream(this.f0);
      GZIPInputStream gzipIn = new GZIPInputStream(byteArrayIS);
      ObjectInputStream objectIn = new ObjectInputStream(gzipIn);
      dfsCode = (DfsCode) objectIn.readObject();
      objectIn.close();
    } catch (IOException | ClassNotFoundException e) {
      e.printStackTrace();
    }

    return dfsCode;
  }

  @Override
  public int hashCode() {

    HashCodeBuilder builder = new HashCodeBuilder();

    for (byte b : this.f0) {
      builder.append(b);
    }

    return builder.hashCode();

  }

  @Override
  public boolean equals(Object o) {
    return this.hashCode() == o.hashCode();
  }


  @Override
  public String toString() {
    return decompress().toString();
  }
}