
package com.salesforce.hbase.index.builder.covered;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

/**
 *
 */
public class ColumnGroup implements Iterable<CoveredColumn> {

  private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
  private List<CoveredColumn> columns = new ArrayList<CoveredColumn>();
  private String table;

  public ColumnGroup(String tableName) {
    this.table = tableName;
  }

  public void add(CoveredColumn column) {
    this.columns.add(column);
  }

  public String getTable() {
    return table;
  }

  public boolean matches(String family) {
    for (CoveredColumn column : columns) {
      if (column.matchesFamily(family)) {
        return true;
      }
    }

    return false;
  }

  public boolean matches(byte[] family, byte[] qualifier) {
    // families are always printable characters
    String fam = Bytes.toString(family);
    for (CoveredColumn column : columns) {
      if (column.matchesFamily(fam)) {
        // check the qualifier
          if (column.matchesQualifier(qualifier)) {
            return true;
        }
      }
    }
    return false;
  }

  /**
   * Build the row key for the current group based on the updated key-values and the given current
   * row.
   * @param p the pending update
   * @param currentRow the state of the current row, possibly a lazy version
   * @return expected row key, each of the values, in order, followed by the PK and then some //
   *         info about lengths so we can deserialize each value
   */
  public byte[] buildRowKey(Mutation p, Result currentRow) {
    // for each column, find the matching value, if one exists. Each column will get a value, even
    // if one isn't currently stored in the table or the passed element
    List<byte[]> values = new ArrayList<byte[]>();
    int length = 0;
    for (CoveredColumn column : columns) {
      //check the put first
      boolean found = false;
      byte[] familyBytes = Bytes.toBytes(column.family);
      // filter down the keyvalues based on matching families
      for (KeyValue kv : Iterables.filter(p.getFamilyMap().get(familyBytes),
        column.getColumnQualifierPredicate())) {
          byte[] value = kv.getValue();
          if (value == null) {
            value = EMPTY_BYTE_ARRAY;
          }
          values.add(value);
          length += value.length;
          found = true;
      }
      
      // found the update in the put, so don't need to search the current state
      if(found){
        continue;
      }

      // get the latest value for the column from the current row
      byte[] value = currentRow.getColumnLatest(familyBytes, column.qualifier).getValue();
      if (value == null) {
        value = EMPTY_BYTE_ARRAY;
      }
      values.add(value);
      length += value.length;
    }
    return KeyValueUtils.composeRowKey(p.getRow(), length, values);
  }

  /**
   * Build a row key for a delete to the index where all keys must be <= the specified timestamps
   * @param pending
   * @param current
   * @param newestTs
   * @return
   */
  public byte[] buildOlderDeleteRowKey(Put pending, Result current, final long newestTs) {
    OlderTimestamps pred = new OlderTimestamps(newestTs);
    // for each column, find the matching value, if one exists. Each column will get a value, even
    // if one isn't currently stored in the table or the passed element
    List<byte[]> values = new ArrayList<byte[]>();
    int length = 0;
    for (CoveredColumn column : columns) {
      // check the put first
      boolean found = false;
      byte[] familyBytes = Bytes.toBytes(column.family);
      // filter down the keyvalues based on matching families
      Predicate<KeyValue> qualPredicate = column.getColumnQualifierPredicate();
      for (KeyValue kv : Iterables.filter(pending.getFamilyMap().get(familyBytes), qualPredicate)) {
        byte[] value = kv.getValue();
        if (value == null) {
          value = EMPTY_BYTE_ARRAY;
        }
        values.add(value);
        length += value.length;
        found = true;
      }

      // found the update in the put, so don't need to search the current state
      if (found) {
        continue;
      }

      // this is the different bit from above - we have to find the newest matching column with an
      // older that the one specified
      // first filter one family
      Iterable<KeyValue> kvs = Iterables.filter(current.list(), column.getColumnFamilyPredicate());
      // then on qualifier
      kvs = Iterables.filter(kvs, qualPredicate);
      // finally on timestamp
      kvs = Iterables.filter(kvs, pred);
      // anything left must match, we just need to find the most recent
      KeyValue first = null;
      for (KeyValue kv : kvs) {
        first = kv;
        break;
      }
      byte[] value;
      // didn't find a keyvalue for the family/qualifier at that timestamp, so must be a null
      // specifier
      if (first == null) {
        value = EMPTY_BYTE_ARRAY;
      }
      else{
        value = first.getValue();
      }
      values.add(value);
      length += value.length;
    }
    return KeyValueUtils.composeRowKey(pending.getRow(), length, values);
  }

  private static class OlderTimestamps implements Predicate<KeyValue> {
    private final long ts;
    public OlderTimestamps(long ts){
      this.ts = ts;
    }
    @Override
    public boolean apply(@Nullable KeyValue input) {
      return input != null && input.getTimestamp() <= ts;
    }
  };

  public int size() {
    return this.columns.size();
  }

  @Override
  public Iterator<CoveredColumn> iterator() {
    return columns.iterator();
  }
}