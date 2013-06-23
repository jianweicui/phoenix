package com.salesforce.hbase.index.builder.covered;

import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
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

  private HashMap<ImmutableBytesWritable, ExposedMemStore> stores;

  public IndexStore(Result currentRow) {
    this.stores = new HashMap<ImmutableBytesWritable, ExposedMemStore>();
    for (KeyValue kv : currentRow.list()) {
      ImmutableBytesWritable bytes = new ImmutableBytesWritable(kv.getFamily());
      ExposedMemStore store = this.stores.get(bytes);
      addIfNotPresent(store, bytes);
    }
  }
  
  /**
   * @param store
   * @param bytes
   * @return
   */
  private ExposedMemStore addIfNotPresent(ExposedMemStore store, ImmutableBytesWritable family) {
    // haven't seen this family yet, create a new store
    if (store != null) {
      return store;
    }
    store = new ExposedMemStore(conf, KeyValue.COMPARATOR);
    stores.put(family, store);
    return store;
  }

  public void add(KeyValue kv) {
    //get the store to which this kv belongs
    ImmutableBytesWritable bytes = new ImmutableBytesWritable(kv.getFamily());
    ExposedMemStore store = this.stores.get(bytes);
    store = addIfNotPresent(store, bytes);
    store.add(kv);
  }

  /**
   * @param bytes
   */
  public KeyValueScanner getFamilyScanner(byte[] bytes) {
    ImmutableBytesWritable family = new ImmutableBytesWritable(bytes);
    ExposedMemStore store = this.stores.get(family);
    return store.getScanners().get(0);
  }

}
