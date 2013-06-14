package com.salesforce.hbase.index.builder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

import com.salesforce.hbase.index.IndexUtil;

/**
 * Simple indexer that just indexes rows based on their column families in a fully covered manner.  
 */
public class CoveredColumnFamilyIndexer extends ConsistentIndexBuilder {

  private static final Log LOG = LogFactory.getLog(CoveredColumnFamilyIndexer.class);

  private static final String INDEX_TO_TABLE_CONF_PREFX = "hbase.index.family.";
  private static final String INDEX_TO_TABLE_COUNT_KEY = INDEX_TO_TABLE_CONF_PREFX + "families";
  private static final String SEPARATOR = ",";

  static final byte[] INDEX_ROW_COLUMN_FAMILY = Bytes.toBytes("ROW");
  static final byte[] INDEX_REMAINING_COLUMN_FAMILY = Bytes.toBytes("REMAINING");

  public static void enableIndexing(HTableDescriptor desc, Map<byte[], String> familyMap)
      throws IOException {
    // not indexing any families, so we shouldn't add the indexer
    if (familyMap == null || familyMap.size() == 0) {
      return;
    }
    Map<String, String> opts = new HashMap<String, String>();
    List<String> families = new ArrayList<String>(familyMap.size());

    for (Entry<byte[], String> family : familyMap.entrySet()) {
      String fam = Bytes.toString(family.getKey());
      opts.put(INDEX_TO_TABLE_CONF_PREFX + fam, family.getValue());
      families.add(fam);
    }

    // add the list of families so we can deserialize each
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < families.size(); i++) {
      sb.append(families.get(i));
      if (i < families.size() - 1) {
        sb.append(SEPARATOR);
      }
    }
    opts.put(INDEX_TO_TABLE_COUNT_KEY, sb.toString());
    IndexUtil.enableIndexing(desc, CoveredColumnFamilyIndexer.class, opts);
  }

  /**
   * Create the specified index table with the necessary columns
   * @param admin {@link HBaseAdmin} to use when creating the table
   * @param indexTable name of the index table. Should be specified in
   *          {@link setupColumnFamilyIndex} as an index target
   */
  public static void createIndexTable(HBaseAdmin admin, String indexTable) throws IOException {
    HTableDescriptor index = new HTableDescriptor(indexTable);
    index.addFamily(new HColumnDescriptor(INDEX_REMAINING_COLUMN_FAMILY));
    index.addFamily(new HColumnDescriptor(INDEX_ROW_COLUMN_FAMILY));

    admin.createTable(index);
  }
  
  private Map<ImmutableBytesWritable, String> columnTargetMap;

  public void setup(Configuration conf) {
    String[] families = conf.get(INDEX_TO_TABLE_COUNT_KEY).split(SEPARATOR);

    // build up our mapping of column - > index table
    columnTargetMap = new HashMap<ImmutableBytesWritable, String>(families.length);
    for (int i = 0; i < families.length; i++) {
      byte[] fam = Bytes.toBytes(families[i]);
      String indexTable = conf.get(INDEX_TO_TABLE_CONF_PREFX + families[i]);
      columnTargetMap.put(new ImmutableBytesWritable(fam), indexTable);
    }
  }

  @Override
  public Map<Mutation, String> getIndexUpdate(Put p) throws IOException {
    // if not columns to index, we are done and don't do anything special
    if (columnTargetMap == null || columnTargetMap.size() == 0) {
      return null;
    }

    return super.getIndexUpdate(p);
  }

  public Map<Mutation, String> getIndexUpdate(Result r, Put p) {
    // override the timestamp to the current time for all edits
    long ts = p.getTimeStamp();
    if (ts == HConstants.LATEST_TIMESTAMP) {
      ts = EnvironmentEdgeManager.currentTimeMillis();
    }
    byte[] timestamp = Bytes.toBytes(ts);

    // pull out the columns we need to index
    Map<Mutation, String> updateMap = new HashMap<Mutation, String>();
    Set<byte[]> keys = p.getFamilyMap().keySet();
    for (Entry<byte[], List<KeyValue>> entry : p.getFamilyMap().entrySet()) {
      String ref = columnTargetMap
          .get(new ImmutableBytesWritable(entry.getKey()));
      // no index maintained for that column, skip it
      if (ref == null) {
        continue;
      }

      // get the keys for this family
      List<KeyValue> kvs = entry.getValue();
      if (kvs == null || kvs.isEmpty()) {
        // should never be the case, but just to be careful
        continue;
      }

      // We need to pay special attention to the timestamps
      // If the user doesn't specify a special timestamp, then we can just use the system provided
      // timestamps and move on. However, if the user specifies a timestamp we may get
      // 'out-of-order' updates across the wire, but we still want to preserve the index for 'back
      // in time' queries. Therefore, the algorithm we need to follow is:
      // 1. an index insert at the passed timestamp (expected)
      // If the data for the indexed column has an *older timestamp than the current row*
      // 2. insert a delete in the index for the column at the *latest* timestamp

      // Create the updates for the index
      // Let's start with the Put - we just do an update into the index based on the current put and
      // the state of the rest of the row

      // Get the row key for the index update == the current family to index
      byte[] row = kvs.get(0).getFamily();
      Put indexInsert = new Put(row);
      Delete indexCleanup = null;

      // got through each of the family's key-values and add it to the put
      for (KeyValue kv : entry.getValue()) {
        // create the update for the new columns at the given timestamp
        indexInsert.add(CoveredColumnFamilyIndexer.INDEX_ROW_COLUMN_FAMILY,
          ArrayUtils.addAll(kv.getRow(), kv.getQualifier()), ts, kv.getValue());

        // update the timestamp on the indexed columns in the primary table
        kv.updateLatestStamp(timestamp);
      }

      // go through the rest of the families and add them to the put, under the special columnfamily
      for (KeyValue kv : r.list()) {
        // if the family is being indexed, then we need to check to see if we need to do deletes
        if (Bytes.equals(row, kv.getFamily())) {
          // already have a delete for the current row
          if (indexCleanup != null) {
            continue;
          }
          //we are always going to do a delete on the row
          indexCleanup = new Delete(row);
          //but we have to figure out when to insert the delete
          if (kv.getTimestamp() > ts) {
            // stored data's timestamp is newer, we need to do index cleanup for the update row
            indexCleanup.setTimestamp(ts);
          } else {
            // our timestamp is newer, so we can delete the old row
            indexCleanup.setTimestamp(kv.getTimestamp());
          }
          continue;
        }
        
        //its not an index family, so do through and add them to the index
        indexInsert.add(CoveredColumnFamilyIndexer.INDEX_REMAINING_COLUMN_FAMILY,
          ArrayUtils.addAll(kv.getFamily(), kv.getQualifier()), ts, kv.getValue());
        }

      // add the mapping
      updateMap.put(indexInsert, ref);
    }
    return updateMap;
  }

  // TODO build the full delete map for the given delete

  @Override
  public Map<Mutation, String> getIndexUpdate(Delete d) throws IOException {
    // if no columns to index, we are done and don't do anything special
    if (columnTargetMap == null || columnTargetMap.size() == 0) {
      return null;
    }
    
    return super.getIndexUpdate(d);
  }
  
  @Override
  public Map<Mutation,String> getIndexUpdate(Result r, Delete d){
    // override the timestamp to the current time for all edits
    long ts = d.getTimeStamp();
    if (ts == HConstants.LATEST_TIMESTAMP) {
      ts = EnvironmentEdgeManager.currentTimeMillis();
    }
    d.setTimestamp(ts);
    
    Map<Mutation, String> updateMap = new HashMap<Mutation, String>();
    for (Entry<byte[], List<KeyValue>> entry : d.getFamilyMap().entrySet()) {
      String ref = columnTargetMap
          .get(new ImmutableBytesWritable(entry.getKey()));
      // no reference for that column, skip it
      if (ref == null) {
        continue;
      }

      // no kvs for the column for some reason... that's odd, but let's just ignore it
      List<KeyValue> kvs = entry.getValue();
      if (kvs == null || kvs.isEmpty()) {
        LOG.info("Found a family in the Delete, but no associated key-values!");
        continue;
      }

      // each family that we are indexing has a new put in the index table for a single row. All we
      // need to do then is to insert a delete for the family at the current timestamp.

      // swap the row key and the column family, just like in put
      Delete delete = new Delete(kvs.get(0).getFamily());
      // set the timestamp to the time of the delete
      delete.setTimestamp(ts);

      // add the mapping
      updateMap.put(delete, ref);
    }
    return updateMap;
  }
}