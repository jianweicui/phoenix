package com.salesforce.hbase.stats.cleanup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;

import com.google.common.collect.Lists;

/**
 * Wrapper class around the necessary cleanup coprocessors.
 * <p>
 * We cleanup stats for a table on a couple different instances:
 * <ol>
 *  <li>On table delete
 *    <ul>
 *      <li>This requires adding a coprocessor on the HMaster and must occure before HMaster startup. Use
 *          {@link #setupConfiguration(Configuration)} to ensure this coprocessor is enabled.</li>
 *    </ul>
 *  </li>
 *  <li>On region split
 *    <ul>
 *      <li>The stats for the parent region of the split are removed from the stats</li>
 *      <li>This is via a region coprocessor, so it is merely added to the table descriptor via
 *      {@link #addToTable(HTableDescriptor)}</li>
 *    </ul>
 *  </li>
 */
public class CleanupStatistics {

  public static void verifyConfiguration(Configuration conf) {
    String[] classes = conf.getStrings(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY);
    List<String> contains = Lists.newArrayList(classes);
    String removeTableCleanupClassName = RemoveTableOnDelete.class.getName();
    if (!contains.contains(removeTableCleanupClassName)) {
      throw new IllegalArgumentException(
          removeTableCleanupClassName
              + " must be specified as a master observer to cleanup table statistics, but its missing from the configuration! We only found: "
              + classes);
    }
  }

  public static void setupConfiguration(Configuration conf) {
    String[] classes = conf.getStrings(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY);
    List<String> toAdd = classes == null ? new ArrayList<String>() : Lists.newArrayList(classes);
    String removeTableCleanupClassName = RemoveTableOnDelete.class.getName();
    if (!toAdd.contains(removeTableCleanupClassName)) {
      toAdd.add(removeTableCleanupClassName);
      conf.setStrings(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY, toAdd.toArray(new String[0]));
    }

    // make sure we didn't screw anything up
    verifyConfiguration(conf);
  }

  public static void addToTable(HTableDescriptor desc) throws IOException {
    String toAdd = RemoveRegionOnSplit.class.getName();
    for (String name : desc.getCoprocessors()) {
      // if its already been added, we are done
      if (name.equals(name)) {
        return;
      }
    }
    // hasn't been added yet, so we need to add it to the table
    desc.addCoprocessor(toAdd);
  }
}