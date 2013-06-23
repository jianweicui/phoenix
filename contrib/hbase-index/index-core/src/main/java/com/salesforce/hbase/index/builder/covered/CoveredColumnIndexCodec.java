package com.salesforce.hbase.index.builder.covered;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.MemStore;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

/**
 * Handle serialization to/from a column-covered index.
 * @see CoveredColumnIndexer
 */
public class CoveredColumnIndexCodec {
  private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
  public static final byte[] INDEX_ROW_COLUMN_FAMILY = Bytes.toBytes("ROW");
  private static Configuration conf = HBaseConfiguration.create();
  {
    // keep it all on the heap - hopefully this should be a bit faster and shouldn't need to grow
    // very large as we are just handling a single row.
    conf.setBoolean("hbase.hregion.memstore.mslab.enabled", false);
  }
  private ValueMap valueMap;
  private List<ImmutableBytesWritable> sorted;
  private ColumnGroup group;
  private MemStore memstore;

  public CoveredColumnIndexCodec(Result currentRow, ColumnGroup group) {
    this.group = group;
    this.valueMap = new ValueMap(group, currentRow.getRow());
    this.valueMap.addToMap(currentRow.list());
    // sort the keys so we get the correct ordering in the returned values
    // this is a little bit heavy weight, but we shouldn't have a ton and we can come back and fix
    // this later if its too expensive.
    this.sorted = valueMap.getSortedKeys();
    // then set properties to get the expected behavior
    this.memstore = new MemStore();
    // add the current row the memstore
    this.memstore.upsert(currentRow.list());
  }

  /**
   * Add a {@link Put} to the values stored for the current row
   * @param pendingUpdate pending update to the current row - will appear first in the
   *          {@link ValueMap}.
   */
  public void addUpdate(Put pendingUpdate) {
    // add just the index update for the group to the map
    for (CoveredColumn column : group) {
      // this already filters out things that don't match the specified group, so we don't need to
      // do any extra filtering
      valueMap.addToMap(pendingUpdate.getFamilyMap().get(Bytes.toBytes(column.family)));
    }
  }

  /**
   * Get the most recent value for each column group, in the order of the columns stored in the
   * group and then build them into a single byte array to use as the prefix for an index update for
   * the column group.
   * @return
   */
  public byte[] toIndexRowKey() {
    int length = 0;
    List<byte[]> topValues = new ArrayList<byte[]>();
    for (CoveredColumn column : group) {
      List<byte[]> values = valueMap.getValues(sorted, column);
      for (int i = 0; i < values.size(); i++) {
        byte[] value = values.get(i);
        if (value == null) {
          value = EMPTY_BYTE_ARRAY;
        }
        length += value.length;
        topValues.add(value);
      }
    }

    return CoveredColumnIndexCodec.composeRowKey(valueMap.primaryKey(), length, topValues);
  }

  /**
   * @param timestamp
   * @return
   */
  public Put getPutToIndex(long timestamp) {
    byte[] indexRow = this.toIndexRowKey();
    Put indexInsert = new Put(indexRow);
    this.addColumnsToIndexUpdate(indexInsert, timestamp);
    return indexInsert;
  }

  /**
   * Add each {@link ColumnGroup} to a {@link Put} under a single column family. Each value stored
   * in the key is matched to a column group - value 1 matches family:qualfier 1. This holds true
   * even if the {@link ColumnGroup} matches all columns in the family.
   * <p>
   * Columns are added as:
   * 
   * <pre>
   * &ltFAMILY&gt | &lti&gt[covered column family]:[covered column qualifier] | &lttimestamp&gt | <tt>null</tt>
   * </pre>
   * 
   * where "i" is the integer index matching the index of the value in the row key, serialized as a
   * byte, and [covered column family]:[covered column qualifier] is the serialization returned by
   * {@link CoveredColumnIndexCodec#toIndexQualfier(CoveredColumn)}
   * @param indexInsert {@link Put} to update with the family:qualifier of each matching value.
   * @param family column family under which to store the columns. The same column is used for all
   *          columns
   * @param timestamp timestamp at which to include the columns in the {@link Put}
   * @param sortedKeys a collection of the keys from the {@link ValueMap} that can be used to search
   *          the value may for a given group.
   */
  private void addColumnsToIndexUpdate(final Put indexInsert, final long timestamp) {
    final int[] count = new int[] { 0 };
    // for each valid match, add a column qualifier under the specified family to the put that is:
    // <i><FAMILY><QUALIFIER>
    Function<Pair<ImmutableBytesWritable, CoveredColumn>, Void> apply = new Function<Pair<ImmutableBytesWritable, CoveredColumn>, Void>() {
      @Override
      @Nullable
      public Void apply(@Nullable Pair<ImmutableBytesWritable, CoveredColumn> input) {
        indexInsert.add(INDEX_ROW_COLUMN_FAMILY,
          ArrayUtils.addAll(Bytes.toBytes(count[0]++), toIndexQualifier(input.getSecond())),
          timestamp, null);
        return null;
      }
    };

    // do the application for matches
    for (CoveredColumn column : group) {
      ValueMap
.applyChangesForMatchingColumns(apply, sorted, column);
    }
  }

  private static byte[] toIndexQualifier(CoveredColumn column) {
    return ArrayUtils.addAll(Bytes.toBytes(column.family + CoveredColumn.SEPARATOR),
      column.qualifier);
  }

  /**
   * Compose the final index row key
   * @param pk primary key of the original row
   * @param length number of bytes in all the values that should be added
   * @param values to use when building the key
   * @return
   */
  private static byte[] composeRowKey(byte[] pk, int length, List<byte[]> values) {
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
   * Check to see if a row key created with {@link composeRowKey} just contains
   * a list of null values.
   * @return <tt>true</tt> if all the values are zero-length, <tt>false</tt> otherwise
   */
  public static boolean checkRowKeyForAllNulls(byte[] bytes) {
    int keyCount = CoveredColumnIndexCodec.getPreviousInteger(bytes, bytes.length);
    int pos = bytes.length - Bytes.SIZEOF_INT;
    for (int i = 0; i < keyCount; i++) {
      int next = CoveredColumnIndexCodec.getPreviousInteger(bytes, pos);
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
  static int getPreviousInteger(byte[] bytes, int start) {
    return Bytes.toInt(bytes, start - Bytes.SIZEOF_INT);
  }

  /**
   * Build the row key for the current group based on the updated key-values and the given current
   * row.
   * @param p the pending update
   * @param currentRow the state of the current row, possibly a lazy version
   * @return expected row key, each of the values, in order, followed by the PK and then some info
   *         about lengths so we can deserialize each value
   */
  public byte[] buildRowKey(Mutation p, Result currentRow, ColumnGroup columns) {
    // for each column, find the matching value, if one exists. Each column will get a value, even
    // if one isn't currently stored in the table or the passed element
    List<byte[]> values = new ArrayList<byte[]>();
    int length = 0;
    for (CoveredColumn column : columns) {
      // check the put first
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
      if (found) {
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
    return CoveredColumnIndexCodec.composeRowKey(p.getRow(), length, values);
  }

  /**
   * Build a row key for a delete to the index where all keys must be <= the specified timestamps
   * @param pending
   * @param current
   * @param newestTs
   * @return
   */
  public static byte[] buildOlderDeleteRowKey(Put pending, Result current, final long newestTs,
      ColumnGroup columns) {
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
      } else {
        value = first.getValue();
      }
      values.add(value);
      length += value.length;
    }
    return CoveredColumnIndexCodec.composeRowKey(pending.getRow(), length, values);
  }

  private static class OlderTimestamps implements Predicate<KeyValue> {
    private final long ts;

    public OlderTimestamps(long ts) {
      this.ts = ts;
    }

    @Override
    public boolean apply(@Nullable KeyValue input) {
      return input != null && input.getTimestamp() <= ts;
    }
  }
}