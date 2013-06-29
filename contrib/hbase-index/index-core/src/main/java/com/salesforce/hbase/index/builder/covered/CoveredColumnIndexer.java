package com.salesforce.hbase.index.builder.covered;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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
 * <b>ASSUMPTION:</b> all key-values in a single {@link Delete}/{@link Put} have the same timestamp.
 * This dramatically simplifies the logic needed to manage updating the index for out-of-order
 * {@link Put}s as we don't need to manage multiple levels of timestamps across multiple columns.
 * <p>
 * We can extend this to multiple columns by picking the latest update of any column in group as the
 * delete point.
 * <p>
 * <b>NOTE:</b> this means that we need to do a lookup (point {@link Get}) of the entire row
 * <i>every time there is a write to the table</i>.
 */
public class CoveredColumnIndexer extends BaseIndexBuilder {

  private static final Log LOG = LogFactory.getLog(CoveredColumnIndexer.class);

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
    index.addFamily(new HColumnDescriptor(CoveredColumnIndexCodec.INDEX_ROW_COLUMN_FAMILY));

    admin.createTable(index);
  }

  private volatile HTableInterface localTable;
  private List<ColumnGroup> groups;
  private RegionCoprocessorEnvironment env;

  @Override
  public void setup(RegionCoprocessorEnvironment env) throws IOException {
    groups = CoveredColumnIndexSpecifierBuilder.getColumns(env.getConfiguration());
    this.env = env;
  }

  // TODO we loop through all the keyvalues for the row a few times - we should be able to do better

  /**
   * Ensure we have a connection to the local table. We need to do this after
   * {@link #setup(RegionCoprocessorEnvironment)} because we are created on region startup and the
   * table isn't actually accessible until later.
   * @throws IOException if we can't reach the table
   */
  private void ensureLocalTable() throws IOException {
    if (this.localTable == null) {
      synchronized (this) {
        if (this.localTable == null) {
          localTable = env.getTable(env.getRegion().getTableDesc().getName());
        }
      }
    }
  }

  @Override
  public Map<Mutation, String> getIndexUpdate(Put p) throws IOException {
    // if not columns to index, we are done
    if (groups == null || groups.size() == 0) {
      return Collections.emptyMap();
    }

    ensureLocalTable();

    // get the current state of the row in our table. We will always need to do this to cleanup the
    // index, so we might as well do this up front
    final byte[] sourceRow = p.getRow();
    Result r = localTable.get(new Get(sourceRow));

    // override the timestamp to the current time for all edits, if one hasn't been specified
    long ts = p.getTimeStamp();
    boolean overrideTimestamp = false;
    if (ts == HConstants.LATEST_TIMESTAMP) {
      overrideTimestamp = true;
      ts = EnvironmentEdgeManager.currentTimeMillis();
    }
    byte[] timestamp = Bytes.toBytes(ts);

    // build the index updates for each group
    Map<Mutation, String> updateMap = new HashMap<Mutation, String>();
    List<ColumnGroup> matches = findMatchingGroups(p);

    // build up the index entries for each group
    for (ColumnGroup group : matches) {
      CoveredColumnIndexCodec codec = new CoveredColumnIndexCodec(sourceRow, r, group);
      getMutationsForPut(updateMap, group, ts, codec, p);
    }

    // update all the put timestamps to match the index entries, if we are not specifying a custom
    // timestamp
    if (overrideTimestamp) {
      for (Entry<byte[], List<KeyValue>> entry : p.getFamilyMap().entrySet()) {
        for (KeyValue kv : entry.getValue()) {
          kv.updateLatestStamp(timestamp);
        }
      }
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
  private void getMutationsForPut(Map<Mutation, String> mutations, ColumnGroup group,
      long timestamp, CoveredColumnIndexCodec codec, Put pendingUpdate) {
    /*
     * Generally, the current Put will be the most recent thing to be added. In that case, all we
     * need to is issue a delete for the previous index row (the state of the row, without the put
     * applied) at the Put's current timestamp. This gets rid of anything currently in the index for
     * the current state of the row (at the timestamp).
     *
     * If things arrive out of order (we are using custom timestamps) we should always still only
     * see the most recent update in the index, even if we are making a put back in time (out of
     * order). Therefore, we need to issue a delete for the index update but at the next most recent
     * timestamp
     */

    Delete cleanup = getIndexCleanupForCurrentRow(codec, timestamp);
    String table = group.getTable();
    if (cleanup != null) {
      mutations.put(cleanup, table);
    }

    // we could try building the index update from just the put, but all the components are probably
    // not covered by the Put, so we spend a bunch of time checking the Put when we are going to
    // need to look at the current row anyways.

    // update the index with the added data. We do this second so we don't need to copy over the
    // keys after applying the put
    codec.addUpdate(pendingUpdate);
    Put indexInsert = codec.getPutToIndex(timestamp);
    mutations.put(indexInsert, table);
  }

  @Override
  public Map<Mutation, String> getIndexUpdate(Delete d) throws IOException {
    // if not columns to index, we are done
    if (groups == null || groups.size() == 0) {
      return Collections.emptyMap();
    }

    ensureLocalTable();

    // stores all the return values
    Map<Mutation, String> updateMap = new HashMap<Mutation, String>();

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

    // We have to figure out which kind of delete it is, since we need to do different things if its
    // a general (row) delete, versus a delete of just a single column or family
    Map<byte[], List<KeyValue>> families = d.getFamilyMap();

    // Option 1: its a row delete marker, so we just need to delete the most recent state for each
    // group, as of the specified timestamp in the delete
    if (families.size() == 0) {
      for (ColumnGroup group : groups) {
        CoveredColumnIndexCodec codec = new CoveredColumnIndexCodec(sourceRow, r, group);
        byte[] row = codec.getIndexRowKey(ts).getFirst();
        Delete indexUpdate = new Delete(row);
        indexUpdate.setTimestamp(ts);
        updateMap.put(indexUpdate, group.getTable());
      }

      return updateMap;
    }

    // check the map to see if we are affecting any of the groups
    List<ColumnGroup> matches = findMatchingGroups(d);

    // build up the index entries for each group
    for (ColumnGroup group : matches) {
      CoveredColumnIndexCodec codec = new CoveredColumnIndexCodec(sourceRow, r, group);
      getMutationsForDelete(updateMap, group, ts, codec, d);
    }

    // update all the delete timestamps to match the index entries, if we are not specifying a
    // custom timestamp
    if (customTime) {
      for (Entry<byte[], List<KeyValue>> entry : d.getFamilyMap().entrySet()) {
        for (KeyValue kv : entry.getValue()) {
          kv.updateLatestStamp(timestamp);
        }
      }
    }
    return updateMap;
  }

  /**
   * Get the mutations for a delete where we aren't deleting the entire row, but rather just a
   * subset of the the columns in the row.
   * @param group which is at least partially covered by the pending delete.
   * @param timestamp timestamp of the current update
   * @param sourceRow
   * @param curerntRow current state of the row from the table
   * @param pendingUpdate update being added to the table
   */
  private void getMutationsForDelete(Map<Mutation, String> mutations, ColumnGroup group,
      final long timestamp, CoveredColumnIndexCodec codec, Delete pendingUpdate) {
    /*
     * we need to create a bunch of inserts/deletes to manage the change in state. If the delete
     * covers the entire group (i.e. group of just one family 'fam' and the Delete deletes that
     * family) we don't need to update the old entries. However, if we only cover part of thegroup
     * we need to make a delete for the existing value and then _an insert for the new value_.
     */
    
    // get the current row from the map
    // always need to delete the current row key because we know that this group is included in the
    // delete when this method is called.
    Delete cleanup = getIndexCleanupForCurrentRow(codec, timestamp);
    String table = group.getTable();
    if (cleanup != null) {
      mutations.put(cleanup, table);
    }

    // apply the delete to the internal value map
    codec.addUpdate(pendingUpdate);
    byte[] deleteKey = codec.getIndexRowKey(timestamp).getFirst();

    // its a covering delete key, so we can just delete the old row and we are done
    if (CoveredColumnIndexCodec.checkRowKeyForAllNulls(deleteKey)) {
      return;
    }

    // delete didn't completely cover the group, so we need to update the index
    Put indexInsert = codec.getPutToIndex(timestamp);
    mutations.put(indexInsert, table);
  }

  /**
   * Make a delete for the state of the current row, at the given timestamp.
   * @param codec to form the row key
   * @param timestamp
   * @return the delete to apply or <tt>null</tt>, if no {@link Delete} is necessary
   */
  private Delete getIndexCleanupForCurrentRow(CoveredColumnIndexCodec codec, long timestamp) {
    byte[] currentRowkey = codec.getIndexRowKey(timestamp).getFirst();
    // no previous state for the current group, so don't create a delete
    if (CoveredColumnIndexCodec.checkRowKeyForAllNulls(currentRowkey)) {
      return null;
    }

    Delete cleanup = new Delete(currentRowkey);
    cleanup.setTimestamp(timestamp);

    return cleanup;
  }

  /**
   * Find all the {@link ColumnGroup}s that match this {@link Mutation} to the primary table.
   * @param m mutation to match against
   * @return the {@link ColumnGroup}s that should be updated with this {@link Mutation}.
   */
  private List<ColumnGroup> findMatchingGroups(Mutation m) {
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
    }
    return matches;
  }

  /**
   * Exposed for testing! Set the local table that should be used to lookup the state of the current
   * row.
   * @param table
   */
  public void setTableForTesting(HTableInterface table) {
    this.localTable = table;
  }
}