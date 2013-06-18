package com.salesforce.hbase.index.builder.covered;

import static org.apache.commons.lang.ArrayUtils.addAll;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multiset;

/**
 *
 */
public class ValueMap {

  private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
  ArrayListMultimap<ImmutableBytesWritable, KeyValue> values = ArrayListMultimap.create();
  private ColumnGroup group;
  private byte[] pk;

  public ValueMap(ColumnGroup group, byte[] pk) {
    this.group = group;
    this.pk = pk;
  }

  /**
   * Add a collection of key-values to the internal map. the order the keys are added (both in the
   * iterable and in the order calling this method) specifies the underlying order in which the keys
   * are stored.
   * @param kvs
   */
  public void addToMap(Iterable<KeyValue> kvs) {
    // for each entry, figure out if it matches our columns, in which case load it into the value
    // multi-map
    for (KeyValue kv: kvs) {
      // add a matching family:column to the value map
      if (group.matches(kv.getFamily(), kv.getQualifier())){
        byte[] valuekey = addAll(kv.getFamily(), kv.getQualifier());
        values.put(new ImmutableBytesWritable(valuekey), kv);
      }
    }
  }
  
  /**
   * Apply a delete to the the existing map. Uses the underlying {@link Delete#getFamilyMap()} to
   * figure out which columns/versions to remove.
   * @param d to apply
   */
  public void applyDelete(Delete d) {
    for (Entry<byte[], List<KeyValue>> family : d.getFamilyMap().entrySet()) {
      //check to see if the column is in the values
      for (KeyValue kv : family.getValue()) {
        ImmutableBytesWritable key =
            new ImmutableBytesWritable(addAll(family.getKey(), kv.getQualifier()));

        // delete applies, so we need to figure out what to do with the data
        Type t = KeyValue.Type.codeToType(kv.getType());
        switch (t) {
        case DeleteColumn:
          // single family:column pair being deleted - removes all instances older than the given
          // timestamp
          List<KeyValue> kvs = values.get(key);
          List<KeyValue> toRemove = new ArrayList<KeyValue>(kvs.size());
          for (KeyValue stored : kvs) {
            if (stored.getTimestamp() < kv.getTimestamp()) {
              toRemove.add(stored);
            }
          }
          // remove the found keys
          kvs.removeAll(toRemove);

          // make sure we have a base element
          if (kvs.size() == 0) {
            kvs.add(null);
          }

          break;
        case DeleteFamily:
          // delete everything with this family - a more intensive operation as we need to seek
          // through all the entries to find things that have the same prefix. We could be more
          // efficient in how we store the values to make this easier, but it gets a little more
          // complicated (and larger) and this should be a rare operation anyways.
          List<ImmutableBytesWritable> keysToRemove = new ArrayList<ImmutableBytesWritable>();
          for (Entry<ImmutableBytesWritable, Collection<KeyValue>> entry : values.asMap()
              .entrySet()) {
            ImmutableBytesWritable storedKey = entry.getKey();
            // if the family prefixes the stored bytes, then we have a match and should remove all
            // these values
            if (Bytes.startsWith(storedKey.get(), kv.getFamily())) {
              keysToRemove.add(storedKey);
            }
          }

          // do the actual the removal
          for (ImmutableBytesWritable stored : keysToRemove) {
            values.removeAll(stored);
            values.get(stored).add(null);
          }
          break;
        case Delete:
          // delete a single key-value
          kvs = values.get(key);
          if (kvs == null || kvs.size() == 0) {
            break;
          }
          // if its the latest TS, we can just remove the first kv
          if (kv.getTimestamp() == HConstants.LATEST_TIMESTAMP) {
            kvs.remove(0);
          }
          // otherwise, we need to remove the specific key-value
          for (int i = 0; i < kvs.size(); i++) {
            KeyValue stored = kvs.get(i);
            if (stored.getTimestamp() == kv.getTimestamp()) {
              kvs.remove(i);
              break;
            }
          }

          // make sure we have a base case
          if (kvs.size() == 0) {
            kvs.add(null);
          }
          break;
        default:
          throw new IllegalArgumentException("Found non-delete type when applying delete! Got: "
              + t);
        }
      }
    }
  }

  /**
   * Get the most recent value for each column group, in the order of the columns stored in the
   * group and then build them into a single byte array to use as the prefix for an index update for
   * the column group.
   * @return
   */
  public byte[] toIndexValue() {
    int length = 0;
    List<byte[]> topValues = new ArrayList<byte[]>();
    for (CoveredColumn column : group) {
      List<byte[]> values = getValues(column);
      for (int i = 0; i < values.size(); i++) {
        byte[] value = values.get(i);
        if (value == null) {
          value = EMPTY_BYTE_ARRAY;
        }
        length += value.length;
        topValues.add(value);
      }
    }

    return KeyValueUtils.composeRowKey(pk, length, topValues);
  }

  /**
   * @param column
   * @return
   */
  private List<byte[]> getValues(CoveredColumn column) {
    List<byte[]> ret = new ArrayList<byte[]>();
    byte[] familyBytes = Bytes.toBytes(column.family);
    byte[] asBytes = addAll(familyBytes, column.qualifier);
    Multiset<ImmutableBytesWritable> keys = values.keys();
    //sort the keys so we get the correct ording in the returned values
    // this is a little bit heavy weight, but we shouldn't have a ton and we can come back and fix
    // this later if its too expensive.
    List<ImmutableBytesWritable> sorted = Lists.newArrayList(keys);
    Collections.sort(sorted);
    for (ImmutableBytesWritable stored : sorted) {
      // if they have the same family
      if (Bytes.startsWith(stored.get(), familyBytes)) {
        //if there is no qualifier or matches exactly to the pair
        if (column.qualifier == null || Bytes.equals(asBytes, stored.get())) {
          KeyValue kv = values.get(stored).get(0);
          ret.add(kv != null ? kv.getValue() : null);
        }
      }
    }
    return ret;
  }
}