package com.salesforce.hbase.index.builder.covered.util;

import java.util.Arrays;

import org.apache.hadoop.hbase.KeyValue;

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
}
