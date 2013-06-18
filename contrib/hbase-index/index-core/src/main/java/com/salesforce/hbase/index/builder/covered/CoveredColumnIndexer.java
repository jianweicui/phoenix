package com.salesforce.hbase.index.builder.covered;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.salesforce.hbase.index.builder.BaseIndexBuilder;

/**
 * Index maintainer that maintains multiple indexes based on '{@link ColumnGroup}s'. Each group is a
 * fully covered within itself and stores the fully 'pre-joined' version of that values for that
 * group of columns.
 * <p>
 * <h2>Index Layout</h2> The row key for a given index entry is the current state of the all the
 * values of the columns in a column group, followed by the primary key (row key) of the original
 * row, and then the length of each value and then finally the total number of values. This is then
 * enough information to completely rebuild the latest value of row for each column in the group.
 * <p>
 * The family is always {@value #INDEX_ROW_COLUMN_FAMILY}.
 * <p>
 * The qualifier is prepended with the integer index (serialized with {@link Bytes#toBytes(int)}) of
 * the column in the group. This index corresponds the index of the value for the group in the row
 * key.
 * 
 * <pre>
 *         ROW                            ||   FAMILY     ||    QUALIFIER     ||   VALUE
 * (v1)(v2)...(vN)(pk)(L1)(L2)...(Ln)(#V) || INDEX_FAMILY ||     1Cf1:Cq1     ||  null
 * (v1)(v2)...(vN)(pk)(L1)(L2)...(Ln)(#V) || INDEX_FAMILY ||     2Cf2:Cq2     ||  null
 * ...
 * (v1)(v2)...(vN)(pk)(L1)(L2)...(Ln)(#V) || INDEX_FAMILY ||     NCfN:CqN     ||  null
 * </pre>
 * 
 * <h2>Index Maintenance</h2>
 * <p>
 * When making an insertion into the table, we also attempt to cleanup the index. This means that we
 * need to remove the previous entry from the index. Generally, this is completed by inserting a
 * delete at the previous value of the previous row.
 * <p>
 * The main caveat here is when dealing with custom timestamps. If there is no special timestamp
 * specified, we can just insert the proper {@link Delete} at the current timestamp and move on.
 * However, when the client specifies a timestamp, we could see updates out of order. In that case,
 * we can do an insert using the specified timestamp, but a delete is different...
 * <p>
 * Taking the simple case, assume we do a single column in a group. Then if we get an out of order
 * update, we need to check the current state of that column in the current row. If the current row
 * is older, we can issue a delete as normal. If the current row is newer, however, we then have to
 * issue a delete for the index update at the time of the current row. This ensures that the index
 * update made for the 'future' time still covers the existing row.
 * <p>
 * We can extend this to multiple columns by picking the latest update of any column in group as the
 * delete point.
 * <p>
 * <b>NOTE:</b> this means that we need to do a lookup (point {@link Get}) of the entire row
 * <i>every time there is a write to the table</i>.
 */
public class CoveredColumnIndexer extends BaseIndexBuilder {

  private static final Log LOG = LogFactory.getLog(CoveredColumnIndexer.class);

  public static final byte[] INDEX_ROW_COLUMN_FAMILY = Bytes.toBytes("ROW");
  // static final byte[] INDEX_REMAINING_COLUMN_FAMILY = Bytes.toBytes("REMAINING");

  /** Empty put that has no information - used to build index delete markers */
  private static final Put EMPTY_PUT = new Put();

  /**
   * Create the specified index table with the necessary columns
   * @param admin {@link HBaseAdmin} to use when creating the table
   * @param indexTable name of the index table. Should be specified in
   *          {@link setupColumnFamilyIndex} as an index target
   */
  public static void createIndexTable(HBaseAdmin admin, String indexTable) throws IOException {
    HTableDescriptor index = new HTableDescriptor(indexTable);
    // index.addFamily(new HColumnDescriptor(INDEX_REMAINING_COLUMN_FAMILY));
    index.addFamily(new HColumnDescriptor(INDEX_ROW_COLUMN_FAMILY));

    admin.createTable(index);
  }
  
  protected HTableInterface localTable;
  private List<ColumnGroup> groups;

  @Override
  public void setup(RegionCoprocessorEnvironment env) throws IOException {
    groups = CoveredColumnIndexSpecifierBuilder.getColumns(env.getConfiguration());
    localTable = env.getTable(env.getRegion().getTableDesc().getName());
  }

  // TODO we loop through all the keyvalues for the row a few times - we should be able to do better

  @Override
  public Map<Mutation, String> getIndexUpdate(Put p) throws IOException {
    // if not columns to index, we are done and don't do anything special
    if (groups == null || groups.size() == 0) {
      return Collections.emptyMap();
    }

    // get the current state of the row in our table. We will always need to do this to cleanup the
    // index, so we might as well do this up front
    final byte[] sourceRow = p.getRow();
    Result r = localTable.get(new Get(sourceRow));

    // override the timestamp to the current time for all edits, if one hasn't been specified
    long ts = p.getTimeStamp();
    boolean customTime = false;
    if (ts == HConstants.LATEST_TIMESTAMP) {
      customTime = true;
      ts = EnvironmentEdgeManager.currentTimeMillis();
    }
    byte[] timestamp = Bytes.toBytes(ts);

    // build the index updates for each group
    Map<Mutation, String> updateMap = new HashMap<Mutation, String>();
    List<ColumnGroup> matches = findMatchingGroupsAndUpdateTimestamps(p, timestamp);

    // build up the index entries for each group
    for (ColumnGroup group : matches) {
      getMutationsForPut(updateMap, group, ts, r, p, customTime);
    }
    return updateMap;
  }

  /**
   * @param timestamp
   * @return
   */
  private List<ColumnGroup> findMatchingGroupsAndUpdateTimestamps(Mutation m, byte[] timestamp) {
    List<ColumnGroup> matches = new ArrayList<ColumnGroup>();
    for (Entry<byte[], List<KeyValue>> entry : m.getFamilyMap().entrySet()) {
      // get the keys for this family that we are indexing
      List<KeyValue> kvs = entry.getValue();
      if (kvs == null || kvs.isEmpty()) {
        // should never be the case, but just to be careful
        continue;
      }

      // figure out the groups we need to index
      String family = Bytes.toString(entry.getKey());
      for (ColumnGroup column : groups) {
        if (column.matches(family)) {
          matches.add(column);
        }
      }
      // update the timestamps for all the kvs that go into the primary table so they match the
      // index entries
      for (KeyValue kv : entry.getValue()) {
        kv.updateLatestStamp(timestamp);
      }
    }
    return matches;
  }

  /**
   * Adds the necessary index updates for the column group to the map of mutations
   * @param group for which to build the index update
   * @param timestamp timestamp of the current update
   * @param curerntRow current state of the row from the table
   * @param pendingUpdate update being added to the table
   */
  private void getMutationsForPut(Map<Mutation, String> mutations, ColumnGroup group,
      long timestamp, Result currentRow, Put pendingUpdate, boolean customTimestamp) {
    String table = group.getTable();
    // Start by building the Put that we need to make to the index
    byte[] row = group.buildRowKey(pendingUpdate, currentRow);
    // the put row actually contains all the values for the columns in a column group - we only need
    // to get their columns and add them to the put so we can get back the full row specification
    Put indexInsert = new Put(row);
    int i = 0;
    for (CoveredColumn column : group) {
      // create the update for the new columns at the given timestamp
      indexInsert.add(CoveredColumnIndexer.INDEX_ROW_COLUMN_FAMILY,
        ArrayUtils.addAll(Bytes.toBytes(i), column.toIndexQualifier()), timestamp, null);
    }
    mutations.put(indexInsert, table);

    // now we need to cleanup the index at the right timestamp. In short, if things arrive out of
    // order, we should always still only see the most recent update, even if we are making a put
    // back in time (out of order). Unfortunately, this gets a little hairy...

    Delete indexCleanup = null;
    byte[] deleteRow = group.buildRowKey(EMPTY_PUT, currentRow);
    long deleteTs = timestamp;
    // just latest time, so we can build a delete from the current state of the row (the same as the
    // put, but without the pending updates)
    if (customTimestamp) {
      // there is a custom timestamp, so we need to figure out which is the latest timestamp -
      // either the put or the current row
      long maxTs = Long.MIN_VALUE;
      for (final CoveredColumn column : group) {
        // check the put and the result for the max ts
        Iterable<KeyValue> all = Iterables.concat(pendingUpdate.getFamilyMap()
          .get(Bytes.toBytes(column.family)), currentRow.list());
        // filter out kvs that don't match this column
        Predicate<KeyValue> predicate = column.getColumnQualifierPredicate();
        // find the max ts
        for (KeyValue kv : Iterables.filter(all,predicate ))
          if (kv.getTimestamp() > maxTs) {
            maxTs = kv.getTimestamp();
          }
        }

      // there are some columns that are newer than the pending update, so we need to figure out
      // update to make upto (and including) the pending write, but no further
      if (timestamp != maxTs) {
        deleteRow = group.buildOlderDeleteRowKey(pendingUpdate, currentRow, maxTs);
        deleteTs = maxTs;
      }
    }

    indexCleanup = new Delete(deleteRow);
    indexCleanup.setTimestamp(deleteTs);
    mutations.put(indexCleanup, table);
  }

  @Override
  public Map<Mutation, String> getIndexUpdate(Delete d) throws IOException {
    // if not columns to index, we are done and don't do anything special
    if (groups == null || groups.size() == 0) {
      return Collections.emptyMap();
    }

    // get the current state of the row in our table. We will always need to do this to cleanup the
    // index, so we might as well do this up front
    final byte[] sourceRow = d.getRow();
    Result r = localTable.get(new Get(sourceRow));

    // override the timestamp to the current time for all edits, if one hasn't been specified
    long ts = d.getTimeStamp();
    boolean customTime = false;
    if (ts == HConstants.LATEST_TIMESTAMP) {
      customTime = true;
      ts = EnvironmentEdgeManager.currentTimeMillis();
    }
    d.setTimestamp(ts);
    byte[] timestamp = Bytes.toBytes(ts);
    
    Map<Mutation, String> updateMap = new HashMap<Mutation, String>();
    // We have to figure out which kind of delete it is, since we need to do different things if its
    // a general (row) delete, versus a delete of just a single column or family
    Map<byte[], List<KeyValue>> families = d.getFamilyMap();
    // its a row delete marker, so we just need to delete the most recent state for each group
    if (families.size() == 0) {
      for (ColumnGroup group : groups) {
        byte[] row = group.buildRowKey(EMPTY_PUT, r);
        Delete indexUpdate = new Delete(row);
        indexUpdate.setTimestamp(ts);
        updateMap.put(indexUpdate, group.getTable());
      }

      return updateMap;
    }

    // check the map to see if we are affecting any of the groups
    List<ColumnGroup> matches = findMatchingGroupsAndUpdateTimestamps(d, timestamp);

    // build up the index entries for each group
    for (ColumnGroup group : matches) {
      getMutationsForDelete(updateMap, group, ts, r, d, customTime);
    }
    return updateMap;
  }
  
  /**
   * Adds the necessary index updates for the column group to the map of mutations
   * @param group for which to build the index update
   * @param timestamp timestamp of the current update
   * @param curerntRow current state of the row from the table
   * @param pendingUpdate update being added to the table
   */
  private void getMutationsForDelete(Map<Mutation, String> mutations, ColumnGroup group,
      long timestamp, Result currentRow, Delete pendingUpdate, boolean customTimestamp) {
    String table = group.getTable();
    // Start by building the Put that we need to make to the index
    byte[] row = group.buildRowKey(pendingUpdate, currentRow);
    // the put row actually contains all the values for the columns in a column group - we only need
    // to get their columns and add them to the put so we can get back the full row specification
    Put indexInsert = new Put(row);
    int i = 0;
    for (CoveredColumn column : group) {
      // create the update for the new columns at the given timestamp
      indexInsert.add(CoveredColumnIndexer.INDEX_ROW_COLUMN_FAMILY,
        ArrayUtils.addAll(Bytes.toBytes(i), column.toIndexQualifier()), timestamp, null);
    }
    mutations.put(indexInsert, table);

    // now we need to cleanup the index at the right timestamp. In short, if things arrive out of
    // order, we should always still only see the most recent update, even if we are making a put
    // back in time (out of order). Unfortunately, this gets a little hairy...

    Delete indexCleanup = null;
    byte[] deleteRow = group.buildRowKey(EMPTY_PUT, currentRow);
    long deleteTs = timestamp;
    // just latest time, so we can build a delete from the current state of the row (the same as the
    // put, but without the pending updates)
    if (customTimestamp) {
      // there is a custom timestamp, so we need to figure out which is the latest timestamp -
      // either the put or the current row
      long maxTs = Long.MIN_VALUE;
      for (final CoveredColumn column : group) {
        // check the put and the result for the max ts
        Iterable<KeyValue> all = Iterables.concat(pendingUpdate.getFamilyMap()
          .get(Bytes.toBytes(column.family)), currentRow.list());
        // filter out kvs that don't match this column
        Predicate<KeyValue> predicate = column.getColumnQualifierPredicate();
        // find the max ts
        for (KeyValue kv : Iterables.filter(all,predicate ))
          if (kv.getTimestamp() > maxTs) {
            maxTs = kv.getTimestamp();
          }
        }

      // there are some columns that are newer than the pending update, so we need to figure out
      // update to make upto (and including) the pending write, but no further
      if (timestamp != maxTs) {
        deleteRow = group.buildOlderDeleteRowKey(pendingUpdate, currentRow, maxTs);
        deleteTs = maxTs;
      }
    }

    indexCleanup = new Delete(deleteRow);
    indexCleanup.setTimestamp(deleteTs);
    mutations.put(indexCleanup, table);
  }
}