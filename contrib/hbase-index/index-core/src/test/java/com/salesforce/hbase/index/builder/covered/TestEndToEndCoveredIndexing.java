package com.salesforce.hbase.index.builder.covered;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test Covered Column indexing in an 'end-to-end' manner on a minicluster. This covers cases where
 * we manage custom timestamped updates that arrive in and out of order as well as just using the
 * generically timestamped updates.
 */
public class TestEndToEndCoveredIndexing {
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final String FAM_STRING = "FAMILY";
  private static final byte[] FAM = Bytes.toBytes(FAM_STRING);
  private static final String FAM2_STRING = "FAMILY2";
  private static final byte[] FAM2 = Bytes.toBytes(FAM2_STRING);
  private static final String INDEX_TABLE = "INDEX_TABLE";
  private static final String INDEX_TABLE2 = "INDEX_TABLE2";
  private static final byte[] EMPTY_BYTES = new byte[0];
  private static final byte[] indexed_qualifer = Bytes.toBytes("indexed_qual");
  private static final byte[] regular_qualifer = Bytes.toBytes("reg_qual");

  // setup a couple of index columns
  private static final ColumnGroup fam1 = new ColumnGroup(INDEX_TABLE);
  private static final ColumnGroup fam2 = new ColumnGroup(INDEX_TABLE2);
  // match a single family:qualifier pair
  private static final CoveredColumn col1 = new CoveredColumn(FAM_STRING, indexed_qualifer);
  // matches the family2:* columns
  private static final CoveredColumn col2 = new CoveredColumn(FAM2_STRING, null);
  private static final CoveredColumn col3 = new CoveredColumn(FAM2_STRING, indexed_qualifer);
  static {
    // values are [col1][col2_1]...[col2_n]
    fam1.add(col1);
    fam1.add(col2);
    // value is [col2]
    fam2.add(col3);
  }

  @BeforeClass
  public static void setupCluster() throws Exception {
    UTIL.startMiniCluster();
  }

  @AfterClass
  public static void teardownCluster() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  /**
   * Test that a bunch of puts with a single timestamp across all the puts builds and inserts index
   * entries as expected
   * @throws Exception on failure
   */
  @Test
  public void testSimpleTimestampedUpdates() throws Exception {
    byte[] indexed_qualifer = Bytes.toBytes("indexed_qual");
    byte[] regular_qualifer = Bytes.toBytes("reg_qual");

    //setup the index 
    CoveredColumnIndexSpecifierBuilder builder = new CoveredColumnIndexSpecifierBuilder();
    builder.addIndexGroup(fam1);

    // setup the primary table
    String indexedTableName = "testSimpleTimestampedUpdates";
    HTableDescriptor pTable = new HTableDescriptor(indexedTableName);
    pTable.addFamily(new HColumnDescriptor(FAM));
    pTable.addFamily(new HColumnDescriptor(FAM2));
    builder.build(pTable);

    // create the primary table
    HBaseAdmin admin = UTIL.getHBaseAdmin();
    admin.createTable(pTable);
    HTable primary = new HTable(UTIL.getConfiguration(), indexedTableName);

    // create the index tables
    CoveredColumnIndexer.createIndexTable(admin, INDEX_TABLE);

    // do a put to the primary table
    byte[] row1 = Bytes.toBytes("row1");
    byte[] value1 = Bytes.toBytes("val1");
    byte[] value2 = Bytes.toBytes("val2");
    Put p = new Put(row1);
    long ts = 10;
    p.add(FAM, indexed_qualifer, ts, value1);
    p.add(FAM, regular_qualifer, ts, value2);
    primary.put(p);
    primary.flushCommits();

    // read the index for the expected values
    HTable index1 = new HTable(UTIL.getConfiguration(), INDEX_TABLE);
    Scan s = new Scan(value1);
    List<KeyValue> received = new ArrayList<KeyValue>();
    ResultScanner scanner = index1.getScanner(s);
    for (Result r : scanner) {
      received.addAll(r.list());
    }

    // build the expected kvs
    List<Pair<byte[], CoveredColumn>> pairs = new ArrayList<Pair<byte[], CoveredColumn>>();
    pairs.add(new Pair<byte[], CoveredColumn>(value1, col1));
    pairs.add(new Pair<byte[], CoveredColumn>(EMPTY_BYTES, col2));
    List<KeyValue> expected = CoveredColumnIndexCodec.getIndexKeyValueForTesting(row1, ts, pairs);

    assertEquals("Didn't get the expected kvs from the index table!", expected, received);

    // cleanup
    primary.close();
    index1.close();
    UTIL.deleteTable(Bytes.toBytes(INDEX_TABLE));
    UTIL.deleteTable(Bytes.toBytes(indexedTableName));
  }

  /**
   * Test that the multiple timestamps in a single put build the correct index updates.
   * @throws Exception on failure
   */
  @Test
  public void testMultipleTimestampsInSinglePut() throws Exception {
    byte[] indexed_qualifer = Bytes.toBytes("indexed_qual");
    byte[] regular_qualifer = Bytes.toBytes("reg_qual");

    // setup the index
    CoveredColumnIndexSpecifierBuilder builder = new CoveredColumnIndexSpecifierBuilder();
    builder.addIndexGroup(fam1);

    // setup the primary table
    String indexedTableName = "testMultipleTimestampsInSinglePut";
    HTableDescriptor pTable = new HTableDescriptor(indexedTableName);
    pTable.addFamily(new HColumnDescriptor(FAM));
    pTable.addFamily(new HColumnDescriptor(FAM2));
    builder.build(pTable);

    // create the primary table
    HBaseAdmin admin = UTIL.getHBaseAdmin();
    admin.createTable(pTable);
    HTable primary = new HTable(UTIL.getConfiguration(), indexedTableName);

    // create the index tables
    CoveredColumnIndexer.createIndexTable(admin, INDEX_TABLE);

    // do a put to the primary table
    byte[] row1 = Bytes.toBytes("row1");
    byte[] value1 = Bytes.toBytes("val1");
    byte[] value2 = Bytes.toBytes("val2");
    byte[] value3 = Bytes.toBytes("val3");
    Put p = new Put(row1);
    long ts1 = 10;
    long ts2 = 11;
    p.add(FAM, indexed_qualifer, ts1, value1);
    p.add(FAM, regular_qualifer, ts1, value2);
    // our group indexes all columns in the this family, so any qualifier here is ok
    p.add(FAM2, regular_qualifer, ts2, value3);
    primary.put(p);
    primary.flushCommits();

    // read the index for the expected values
    HTable index1 = new HTable(UTIL.getConfiguration(), INDEX_TABLE);

    // build the expected kvs
    List<Pair<byte[], CoveredColumn>> pairs = new ArrayList<Pair<byte[], CoveredColumn>>();
    pairs.add(new Pair<byte[], CoveredColumn>(value1, col1));
    pairs.add(new Pair<byte[], CoveredColumn>(EMPTY_BYTES, col2));

    // check the first entry at ts1
    List<KeyValue> expected = CoveredColumnIndexCodec.getIndexKeyValueForTesting(row1, ts1, pairs);
    verifyIndexTableAtTimestamp(index1, expected, ts1, value1);

    // check the second entry at ts2
    pairs.clear();
    pairs.add(new Pair<byte[], CoveredColumn>(value1, col1));
    pairs.add(new Pair<byte[], CoveredColumn>(value3, col2));
    expected = CoveredColumnIndexCodec.getIndexKeyValueForTesting(row1, ts2, pairs);
    verifyIndexTableAtTimestamp(index1, expected, ts2, value1);

    // cleanup
    primary.close();
    index1.close();
    UTIL.deleteTable(Bytes.toBytes(INDEX_TABLE));
    UTIL.deleteTable(Bytes.toBytes(indexedTableName));
  }

  /**
   * Test that we make updates to multiple {@link ColumnGroup}s across a single put/delete 
   * @throws Exception on failure
   */
  @Test
  public void testMultipleConcurrentGroupsUpdated() throws Exception {
    // setup the index
    CoveredColumnIndexSpecifierBuilder builder = new CoveredColumnIndexSpecifierBuilder();
    builder.addIndexGroup(fam1);
    builder.addIndexGroup(fam2);

    // setup the primary table
    String indexedTableName = "testMultipleConcurrentGroupsUpdated";
    HTableDescriptor pTable = new HTableDescriptor(indexedTableName);
    pTable.addFamily(new HColumnDescriptor(FAM));
    pTable.addFamily(new HColumnDescriptor(FAM2));
    builder.build(pTable);

    // create the primary table
    HBaseAdmin admin = UTIL.getHBaseAdmin();
    admin.createTable(pTable);
    HTable primary = new HTable(UTIL.getConfiguration(), indexedTableName);

    // create the index tables
    CoveredColumnIndexer.createIndexTable(admin, INDEX_TABLE);
    CoveredColumnIndexer.createIndexTable(admin, INDEX_TABLE2);

    // do a put to the primary table
    byte[] row1 = Bytes.toBytes("row1");
    byte[] value1 = Bytes.toBytes("val1");
    byte[] value2 = Bytes.toBytes("val2");
    byte[] value3 = Bytes.toBytes("val3");
    Put p = new Put(row1);
    long ts = 10;
    p.add(FAM, indexed_qualifer, ts, value1);
    p.add(FAM, regular_qualifer, ts, value2);
    p.add(FAM2, indexed_qualifer, ts, value3);
    primary.put(p);
    primary.flushCommits();

    // read the index for the expected values
    HTable index1 = new HTable(UTIL.getConfiguration(), INDEX_TABLE);
    HTable index2 = new HTable(UTIL.getConfiguration(), INDEX_TABLE2);

    // build the expected kvs
    List<Pair<byte[], CoveredColumn>> pairs = new ArrayList<Pair<byte[], CoveredColumn>>();
    pairs.add(new Pair<byte[], CoveredColumn>(value1, col1));
    pairs.add(new Pair<byte[], CoveredColumn>(value3, col2));
    List<KeyValue> expected = CoveredColumnIndexCodec.getIndexKeyValueForTesting(row1, ts, pairs);
    verifyIndexTableAtTimestamp(index1, expected, ts, value1);

    // and check the second index as well
    pairs.clear();
    pairs.add(new Pair<byte[], CoveredColumn>(value3, col3));
    expected = CoveredColumnIndexCodec.getIndexKeyValueForTesting(row1, ts, pairs);
    verifyIndexTableAtTimestamp(index2, expected, ts, value3);

    // cleanup
    primary.close();
    index1.close();
    index2.close();
    UTIL.deleteTable(Bytes.toBytes(INDEX_TABLE));
    UTIL.deleteTable(Bytes.toBytes(INDEX_TABLE2));
    UTIL.deleteTable(Bytes.toBytes(indexedTableName));
  }

  private void verifyIndexTableAtTimestamp(HTable index1, List<KeyValue> expected, long ts,
      byte[] startKey) throws IOException {
    Scan s = new Scan(startKey);
    s.setTimeStamp(ts);
    // s.setRaw(true);
    List<KeyValue> received = new ArrayList<KeyValue>();
    ResultScanner scanner = index1.getScanner(s);
    for (Result r : scanner) {
      received.addAll(r.list());
    }
    scanner.close();
    assertEquals("Didn't get the expected kvs from the index table!", expected, received);
  }
}
