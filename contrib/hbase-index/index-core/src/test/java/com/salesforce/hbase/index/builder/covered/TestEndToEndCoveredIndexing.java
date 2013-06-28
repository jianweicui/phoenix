package com.salesforce.hbase.index.builder.covered;

import static org.junit.Assert.assertEquals;

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
  private static final String INDEXED_TABLE = "INDEXED_TABLE";
  private static final String INDEX_TABLE = "INDEX_TABLE";
  private static final String INDEX_TABLE2 = "INDEX_TABLE2";
  private static final byte[] EMPTY_BYTES = new byte[0];

  @BeforeClass
  public static void setupCluster() throws Exception {
    UTIL.startMiniCluster();
  }

  @AfterClass
  public static void teardownCluster() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testSimpleTimestampedUpdates() throws Exception {
    byte[] indexed_qualifer = Bytes.toBytes("indexed_qual");
    byte[] regular_qualifer = Bytes.toBytes("reg_qual");

    //setup the index 
    CoveredColumnIndexSpecifierBuilder builder = new CoveredColumnIndexSpecifierBuilder();
    ColumnGroup fam1 = new ColumnGroup(INDEX_TABLE);
    // match a single family:qualifier pair
    CoveredColumn col1 = new CoveredColumn(FAM_STRING, indexed_qualifer);
    fam1.add(col1);
    // matches the family2:* columns
    CoveredColumn col2 = new CoveredColumn(FAM2_STRING, null);
    fam1.add(col2);
    builder.addIndexGroup(fam1);
    ColumnGroup fam2 = new ColumnGroup(INDEX_TABLE2);
    // match a single family2:qualifier pair
    CoveredColumn col3 = new CoveredColumn(FAM2_STRING, indexed_qualifer);
    fam2.add(col3);
    builder.addIndexGroup(fam2);

    // setup the primary table
    HTableDescriptor pTable = new HTableDescriptor(INDEXED_TABLE);
    pTable.addFamily(new HColumnDescriptor(FAM));
    pTable.addFamily(new HColumnDescriptor(FAM2));
    builder.build(pTable);

    // create the primary table
    HBaseAdmin admin = UTIL.getHBaseAdmin();
    admin.createTable(pTable);
    HTable primary = new HTable(UTIL.getConfiguration(), INDEXED_TABLE);

    // create the index tables
    CoveredColumnIndexer.createIndexTable(admin, INDEX_TABLE);
    CoveredColumnIndexer.createIndexTable(admin, INDEX_TABLE2);

    // do a put to the primary table
    byte[] row1 = Bytes.toBytes("row1");
    byte[] row2 = Bytes.toBytes("row2");
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
    UTIL.deleteTable(Bytes.toBytes(INDEXED_TABLE));
  }
}
