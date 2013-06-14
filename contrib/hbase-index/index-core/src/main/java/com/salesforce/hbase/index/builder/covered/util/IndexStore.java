package com.salesforce.hbase.index.builder.covered.util;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.ExposedMemStore;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.Store;

/**
 * A collection of {@link Store}s for an index manager.
 */
public class IndexStore {

  private static final Configuration conf = HBaseConfiguration.create();
  static {
    // keep it all on the heap - hopefully this should be a bit faster and shouldn't need to grow
    // very large as we are just handling a single row.
    conf.setBoolean("hbase.hregion.memstore.mslab.enabled", false);
  }

  private ExposedMemStore store;
  private byte[] pk;

  public IndexStore(byte[] row) {
    this.store = new ExposedMemStore(conf, KeyValue.COMPARATOR);
    this.pk = row;
  }
  

  public void add(KeyValue kv) {
    store.add(kv);
  }

  public void add(List<KeyValue> edits) {
    for (KeyValue kv : edits) {
      add(kv);
    }
  }

  /**
   * @param family
   */
  public KeyValueScanner getFamilyScanner(byte[] family) {
    // only a single scanner that we ever care about
    KeyValueScanner scanner = store.getScanners().get(0);
    KeyValue seekLocation = KeyValue.createFirstOnRow(pk, family, null);
    // seek the scanner to that family
    try {
      scanner.seek(seekLocation);
    } catch (IOException e) {
      throw new RuntimeException("MemStore scanner somehow threw IOException!", e);
    }
    return scanner;
  }
}