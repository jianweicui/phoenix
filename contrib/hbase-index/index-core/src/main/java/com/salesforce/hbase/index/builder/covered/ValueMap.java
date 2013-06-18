package com.salesforce.hbase.index.builder.covered;

import static org.apache.commons.lang.ArrayUtils.addAll;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;

import javax.annotation.Nullable;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
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
   * @param kvs to add
   */
  public void addToMap(Iterable<KeyValue> kvs) {
    addToMap(kvs, null);
  }

  /**
   * Add a collection of key-values to the internal map. the order the keys are added (both in the
   * iterable and in the order calling this method) specifies the underlying order in which the keys
   * are stored.
   * @param kvs to add
   * @param filter filter to apply to the {@link KeyValue}s before they are added to the map. Any
   *          {@link KeyValue} that doesn't pass this filter won't even be considered in the map
   */
  public void addToMap(Iterable<KeyValue> kvs, Predicate<KeyValue> filter) {
    // apply the filter to the input keyvalues
    Iterable<KeyValue> iter = filter != null ? Iterables.filter(kvs, filter) : kvs;

    // for each entry, figure out if it matches our columns, in which case load it into the value
    // multi-map
    for (KeyValue kv : iter) {
      // add a matching family:column to the value map
      if (group.matches(kv.getFamily(), kv.getQualifier())) {
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
      // check to see if the column is in the values
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
    // sort the keys so we get the correct ordering in the returned values
    // this is a little bit heavy weight, but we shouldn't have a ton and we can come back and fix
    // this later if its too expensive.
    Multiset<ImmutableBytesWritable> keys = values.keys();
    List<ImmutableBytesWritable> sorted = Lists.newArrayList(keys);
    Collections.sort(sorted);
    for (CoveredColumn column : group) {
      List<byte[]> values = getValues(sorted, column);
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
  private List<byte[]> getValues(Iterable<ImmutableBytesWritable> sorted, CoveredColumn column) {
    final List<byte[]> ret = new ArrayList<byte[]>();
    Function<Pair<ImmutableBytesWritable, CoveredColumn>, Void> apply =
        new Function<Pair<ImmutableBytesWritable, CoveredColumn>, Void>() {
      @Override
      @Nullable
          public Void apply(@Nullable Pair<ImmutableBytesWritable, CoveredColumn> input) {
            KeyValue kv = ValueMap.this.values.get(input.getFirst()).get(0);
            ret.add(kv != null ? kv.getValue() : null);
            return null;
      }

    };
    applyChangesForMatchingColumns(apply, sorted, column);
    byte[] familyBytes = Bytes.toBytes(column.family);
    byte[] asBytes = addAll(familyBytes, column.qualifier);
    for (ImmutableBytesWritable stored : sorted) {
      // if they have the same family
      if (Bytes.startsWith(stored.get(), familyBytes)) {
        // if there is no qualifier or matches exactly to the pair
        if (column.qualifier == null || Bytes.equals(asBytes, stored.get())) {
          KeyValue kv = values.get(stored).get(0);
          ret.add(kv != null ? kv.getValue() : null);
        }
      }
    }
    return ret;
  }

  public void addColumnsToIndexUpdate(final Put indexInsert, final byte[] family,
      final long timestamp) {
    final int[] count = new int[] { 0 };
    Function<Pair<ImmutableBytesWritable, CoveredColumn>, Void> apply =
        new Function<Pair<ImmutableBytesWritable, CoveredColumn>, Void>() {
          @Override
          @Nullable
          public Void apply(@Nullable Pair<ImmutableBytesWritable, CoveredColumn> input) {
            indexInsert.add(family,
              ArrayUtils.addAll(Bytes.toBytes(count[0]++), input.getSecond().toIndexQualifier()),
              timestamp, null);
            return null;
          }
        };
    // sort the keys so we get the correct ordering in the returned values
    // this is a little bit heavy weight, but we shouldn't have a ton and we can come back and fix
    // this later if its too expensive.
    Multiset<ImmutableBytesWritable> keys = values.keys();
    List<ImmutableBytesWritable> sorted = Lists.newArrayList(keys);
    Collections.sort(sorted);
    for (CoveredColumn column : group) {
      applyChangesForMatchingColumns(apply, sorted, column);
    }
  }

  /**
   * Iterate through the passed keys looking for a match with the given column. If there is a match,
   * pass the surrounding context to the function to modify some internal state.
   * @param toApply function to apply on match
   * @param keys the keys to iterate through
   * @param column column to match the keys against. Uses the same matching as a
   *          {@link CoveredColumn} usually would.
   */
  private void applyChangesForMatchingColumns(Function<Pair<ImmutableBytesWritable, CoveredColumn>, Void> toApply,
      Iterable<ImmutableBytesWritable> keys, CoveredColumn column) {
    byte[] familyBytes = Bytes.toBytes(column.family);
    byte[] asBytes = addAll(familyBytes, column.qualifier);
    for (ImmutableBytesWritable stored : keys) {
      // if they have the same family
      if (Bytes.startsWith(stored.get(), familyBytes)) {
        // if there is no qualifier or matches exactly to the pair
        if (column.qualifier == null || Bytes.equals(asBytes, stored.get())) {
          toApply.apply(new Pair<ImmutableBytesWritable, CoveredColumn>(stored, column));
        }
      }
    }
  }
}