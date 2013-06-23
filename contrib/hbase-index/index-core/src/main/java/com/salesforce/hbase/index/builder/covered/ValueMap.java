package com.salesforce.hbase.index.builder.covered;

import static org.apache.commons.lang.ArrayUtils.addAll;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;

import javax.annotation.Nullable;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.MemStore;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multiset;
import com.google.common.collect.Ordering;
import com.google.common.collect.SortedSetMultimap;
import com.google.common.collect.TreeMultimap;

/**
 * Essentially a lightweight {@link MemStore} that just manages a single row.This makes it easier to
 * reason about applying changes to a single row (e.g. applying a delete, finding the most recent
 * value at a timetamp).
 */
public class ValueMap {
  // TODO look into switching this class out for a real MemStore - its far more likely to be correct
  // and potentially could be shared across index calls, though with some overhead.
  // using this keeps all the keys sorted in each pair, so we can then handle point in time queries
  // correctly
  SortedSetMultimap<ImmutableBytesWritable, KeyValue> values = TreeMultimap.create(
    Ordering.natural(), KeyValue.COMPARATOR);
  private ColumnGroup group;
  private byte[] pk;

  public ValueMap(ColumnGroup group, byte[] pk) {
    this.group = group;
    this.pk = pk;
  }

  public ValueMap clone() {
    ValueMap clone = new ValueMap(group, pk);
    for (Entry<ImmutableBytesWritable, Collection<KeyValue>> entry : values.asMap().entrySet()) {
      clone.values.putAll(entry.getKey(), entry.getValue());
    }
    return clone;
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
   * figure out which columns/versions to remove; if the {@link Delete} does not specify a family
   * (its just a row level delete) this method won't do anything.
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
          Collection<KeyValue> kvs = values.get(key);
          Collection<KeyValue> toRemove = new ArrayList<KeyValue>(kvs.size());
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
   * Get the values that match the specified column.
   * @param column to check against each value
   * @return all values matching the stored column. Returned stored in order to the
   *         lexicographically sorted order of the value's family:qualifier
   */
  public List<byte[]> getValues(Iterable<ImmutableBytesWritable> sorted, CoveredColumn column) {
    final List<byte[]> ret = new ArrayList<byte[]>();

    // for each match, just add it to the returned list of values
    // null kv == no match in the values, but it was there at some point (e.g. got removed by a
    // delete), so we add in a null in the values.
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

  /**
   * Iterate through the passed keys looking for a match with the given column. If there is a match,
   * calls the function with some of the state about the match.
   * <p>
   * We only iterate through the known keys, rather than through each expected column, so if there
   * isn't a stored key for the column, we aren't going to get a match.
   * @param toApply function to apply on match
   * @param keys the keys to iterate through
   * @param column column to match the keys against. Uses the same matching as a
   *          {@link CoveredColumn} usually would.
   */
  public static void applyChangesForMatchingColumns(
      Function<Pair<ImmutableBytesWritable, CoveredColumn>, Void> toApply,
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

  /**
   * @return
   */
  public Multiset<ImmutableBytesWritable> keys() {
    return this.values.keys();
  }

  /**
   * @return
   */
  public ColumnGroup getGroup() {
    return this.group;
  }

  public byte[] primaryKey() {
    return this.pk;
  }

  /**
   * @param allEdits {@link ValueMap} to evaluate
   * @return a lexicographically sorted {@link List} of the keys from the passed value map
   */
  public List<ImmutableBytesWritable> getSortedKeys() {
    Multiset<ImmutableBytesWritable> keys = this.keys();
    List<ImmutableBytesWritable> sorted = Lists.newArrayList(keys);
    Collections.sort(sorted);
    return sorted;
  }
}