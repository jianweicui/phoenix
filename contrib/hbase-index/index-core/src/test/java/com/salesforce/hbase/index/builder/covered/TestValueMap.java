package com.salesforce.hbase.index.builder.covered;

import static org.junit.Assert.assertArrayEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 *
 */
public class TestValueMap {
  private static final byte[] PK = new byte[] { 'a' };
  private static final String FAMILY_STRING = "family";
  private static final byte[] FAMILY = Bytes.toBytes(FAMILY_STRING);
  private static final byte[] QUAL = Bytes.toBytes("qual");

  @Test
  public void testFullColumnSpecification() {
    ColumnGroup group = new ColumnGroup("testFullColumnSpecification");
    CoveredColumn column = new CoveredColumn(FAMILY_STRING, QUAL);
    group.add(column);

    ValueMap map = new ValueMap(group, PK);
    List<KeyValue> kvs = new ArrayList<KeyValue>();
    byte[] v1 = Bytes.toBytes("v1");
    KeyValue kv = new KeyValue(PK, FAMILY, QUAL, 1, v1);
    kvs.add(kv);
    byte[] v2 = Bytes.toBytes("v2");
    kv = new KeyValue(PK, Bytes.toBytes("family2"), QUAL, 1, v2);
    kvs.add(kv);
    map.addToMap(kvs);

    // simple case - no deletes
    byte[] indexValue = map.toIndexValue();
    byte[] expected = KeyValueUtils.composeRowKey(PK, v1.length, Arrays.asList(v1));
    assertArrayEquals("Didn't get expected index value", expected, indexValue);

    // now add a delete that covers that column family
    Delete d = new Delete(PK);
    d.deleteFamily(FAMILY);
    map.applyDelete(d);
    indexValue = map.toIndexValue();
    expected = KeyValueUtils.composeRowKey(PK, 0, Lists.newArrayList(new byte[0]));
    assertArrayEquals("Deleting family didn't specify null value as expected", expected, indexValue);

    // reset the map
    map = new ValueMap(group, PK);
    map.addToMap(kvs);

    // now try with a full column delete
    d = new Delete(PK);
    d.deleteColumns(FAMILY, QUAL);
    map.applyDelete(d);
    indexValue = map.toIndexValue();
    expected = KeyValueUtils.composeRowKey(PK, 0, Lists.newArrayList(new byte[0]));
    assertArrayEquals("Deleting family didn't specify null value as expected", expected, indexValue);

    // reset the map
    map = new ValueMap(group, PK);
    map.addToMap(kvs);

    // now try by deleting the single value
    d = new Delete(PK);
    d.deleteColumn(FAMILY, QUAL);
    map.applyDelete(d);
    indexValue = map.toIndexValue();
    expected = KeyValueUtils.composeRowKey(PK, 0, Lists.newArrayList(new byte[0]));
    assertArrayEquals("Deleting family didn't specify null value as expected", expected, indexValue);
  }

  @Test
  public void testOnlySpecifyFamilyWithSingleStoredColumn() {
    ColumnGroup group = new ColumnGroup("testOnlySpecifyFamilyWithSingleStoredColumn");
    CoveredColumn column = new CoveredColumn(FAMILY_STRING, null);
    group.add(column);

    ValueMap map = new ValueMap(group, PK);
    List<KeyValue> kvs = new ArrayList<KeyValue>();
    byte[] v1 = Bytes.toBytes("v1");
    KeyValue kv = new KeyValue(PK, FAMILY, QUAL, 1, v1);
    kvs.add(kv);
    byte[] v2 = Bytes.toBytes("v2");
    kv = new KeyValue(PK, Bytes.toBytes("family2"), QUAL, 1, v2);
    kvs.add(kv);
    kvs.add(kv);
    map.addToMap(kvs);

    // simple case - no deletes, all columns
    byte[] indexValue = map.toIndexValue();
    byte[] expected = KeyValueUtils.composeRowKey(PK, v1.length, Arrays.asList(v1));
    assertArrayEquals("Didn't get expected index value", expected, indexValue);

    // now add a delete that covers that entire column family
    Delete d = new Delete(PK);
    d.deleteFamily(FAMILY);
    map.applyDelete(d);
    indexValue = map.toIndexValue();
    expected = KeyValueUtils.composeRowKey(PK, 0, Lists.newArrayList(new byte[0]));
    assertArrayEquals("Deleting family didn't specify null value as expected", expected, indexValue);

    // reset the map
    map = new ValueMap(group, PK);
    map.addToMap(kvs);

    // now try deleting one of the columns
    d = new Delete(PK);
    d.deleteColumns(FAMILY, QUAL);
    map.applyDelete(d);
    indexValue = map.toIndexValue();
    expected = KeyValueUtils.composeRowKey(PK, 0, Lists.newArrayList(new byte[0]));
    assertArrayEquals("Deleting family didn't specify null value as expected", expected, indexValue);

    // reset the map
    map = new ValueMap(group, PK);
    map.addToMap(kvs);

    // now try by deleting the single value
    d = new Delete(PK);
    d.deleteColumn(FAMILY, QUAL);
    map.applyDelete(d);
    indexValue = map.toIndexValue();
    expected = KeyValueUtils.composeRowKey(PK, 0, Lists.newArrayList(new byte[0]));
    assertArrayEquals("Deleting family didn't specify null value as expected", expected, indexValue);
  }

  @Test
  public void testOnlySpecifyFamilyWithMultipleStoredColumns() {
    ColumnGroup group = new ColumnGroup("testOnlySpecifyFamilyWithMultipleStoredColumns");
    CoveredColumn column = new CoveredColumn(FAMILY_STRING, null);
    group.add(column);

    ValueMap map = new ValueMap(group, PK);
    List<KeyValue> kvs = new ArrayList<KeyValue>();
    byte[] v1 = Bytes.toBytes("v1");
    KeyValue kv = new KeyValue(PK, FAMILY, QUAL, 1, v1);
    kvs.add(kv);
    byte[] v2 = Bytes.toBytes("v2");
    kv = new KeyValue(PK, Bytes.toBytes("family2"), QUAL, 1, v2);
    kvs.add(kv);
    // add another value for a different column in the indexed family
    byte[] v3 = Bytes.toBytes("v3");
    kv = new KeyValue(PK, FAMILY, Bytes.toBytes("qual2"), 1, v3);
    kvs.add(kv);
    map.addToMap(kvs);

    // simple case - no deletes, all columns
    byte[] indexValue = map.toIndexValue();
    byte[] expected = KeyValueUtils.composeRowKey(PK, v1.length + v3.length, Arrays.asList(v1, v3));
    assertArrayEquals("Didn't get expected index value", expected, indexValue);

    // now add a delete that covers that entire column family
    Delete d = new Delete(PK);
    d.deleteFamily(FAMILY);
    map.applyDelete(d);
    indexValue = map.toIndexValue();
    expected = KeyValueUtils.composeRowKey(PK, 0, Lists.newArrayList(new byte[0], new byte[0]));
    assertArrayEquals("Deleting family didn't specify null value as expected", expected, indexValue);

    // reset the map
    map = new ValueMap(group, PK);
    map.addToMap(kvs);

    // now try deleting one of the columns
    d = new Delete(PK);
    d.deleteColumns(FAMILY, QUAL);
    map.applyDelete(d);
    indexValue = map.toIndexValue();
    expected = KeyValueUtils.composeRowKey(PK, v3.length, Lists.newArrayList(new byte[0], v3));
    assertArrayEquals("Deleting family didn't specify null value as expected", expected, indexValue);

    // reset the map
    map = new ValueMap(group, PK);
    map.addToMap(kvs);

    // now try by deleting the single value
    d = new Delete(PK);
    d.deleteColumn(FAMILY, QUAL);
    map.applyDelete(d);
    indexValue = map.toIndexValue();
    expected = KeyValueUtils.composeRowKey(PK, v3.length, Lists.newArrayList(new byte[0], v3));
    assertArrayEquals("Deleting family didn't specify null value as expected", expected, indexValue);
  }
}
