package com.salesforce.hbase.index.builder.covered;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 */
public class KeyValueUtils {
  /**
   * The KeyValue for the most recent for a given column. If the column does not exist in the result
   * set - if it wasn't selected in the query (Get/Scan) or just does not exist in the row the
   * return value is null.
   * @param family
   * @param qualifier
   * @return KeyValue for the column or null
   */
  public static KeyValue getColumnLatest(KeyValue[] kvs, byte[] family, byte[] qualifier) {
    if (kvs == null || kvs.length == 0) {
      return null;
    }
    int pos = binarySearch(kvs, family, qualifier);
    if (pos == -1) {
      return null;
    }
    KeyValue kv = kvs[pos];
    if (kv.matchingColumn(family, qualifier)) {
      return kv;
    }
    return null;
  }

  protected static int binarySearch(final KeyValue[] kvs, final byte[] family,
      final byte[] qualifier) {
    KeyValue searchTerm = KeyValue.createFirstOnRow(kvs[0].getRow(), family, qualifier);

    // pos === ( -(insertion point) - 1)
    int pos = Arrays.binarySearch(kvs, searchTerm, KeyValue.COMPARATOR);
    // never will exact match
    if (pos < 0) {
      pos = (pos + 1) * -1;
      // pos is now insertion point
    }
    if (pos == kvs.length) {
      return -1; // doesn't exist
    }
    return pos;
  }

  /**
   * Compose the final index row key
   * @param pk primary key of the original row
   * @param length number of bytes in all the values that should be added
   * @param values to use when building the key
   * @return
   */
  protected static byte[] composeRowKey(byte[] pk, int length, List<byte[]> values) {
    // now build up expected row key, each of the values, in order, followed by the PK and then some
    // info about lengths so we can deserialize each value
    byte[] output = new byte[length + pk.length];
    int pos = 0;
    int[] lengths = new int[values.size()];
    int i = 0;
    for (byte[] v : values) {
      System.arraycopy(v, 0, output, pos, v.length);
      lengths[i++] = v.length;
      pos += v.length;
    }

    // add the primary key to the end of the row key
    System.arraycopy(pk, 0, output, pos, pk.length);

    // add the lengths as suffixes so we can deserialize the elements again
    for (int l : lengths) {
      output = ArrayUtils.addAll(output, Bytes.toBytes(l));
    }

    // and the last integer is the number of values
    return ArrayUtils.addAll(output, Bytes.toBytes(values.size()));
  }

  /**
   * Check to see if a row key created with {@link #composeRowKey(byte[], int, List)} just contains
   * a list of null values.
   * @return <tt>true</tt> if all the values are zero-length, <tt>false</tt> otherwise
   */
  public static boolean checkRowKeyForAllNulls(byte[] bytes) {
    int keyCount = getPreviousInteger(bytes, bytes.length);
    int pos = bytes.length - Bytes.SIZEOF_INT;
    for (int i = 0; i < keyCount; i++) {
      int next = getPreviousInteger(bytes, pos);
      if (next > 0) {
        return false;
      }
      pos -= Bytes.SIZEOF_INT;
    }

    return true;
  }

  /**
   * Read an integer from the preceding {@value Bytes#SIZEOF_INT} bytes
   * @param bytes array to read from
   * @param start start point, backwards from which to read. For example, if specifying "25", we
   *          would try to read an integer from 21 -> 25
   * @return an integer from the proceeding {@value Bytes#SIZEOF_INT} bytes, if it exists.
   */
  private static int getPreviousInteger(byte[] bytes, int start) {
    return Bytes.toInt(bytes, start - Bytes.SIZEOF_INT);
  }
}
