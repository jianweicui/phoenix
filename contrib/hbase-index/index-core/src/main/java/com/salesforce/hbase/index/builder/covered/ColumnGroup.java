
package com.salesforce.hbase.index.builder.covered;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

/**
 * A collection of {@link CoveredColumn}s that should be included in a covered index.
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

  public int size() {
    return this.columns.size();
  }

  @Override
  public Iterator<CoveredColumn> iterator() {
    return columns.iterator();
  }

  public CoveredColumn get(int index) {
    return this.columns.get(index);
  }
}