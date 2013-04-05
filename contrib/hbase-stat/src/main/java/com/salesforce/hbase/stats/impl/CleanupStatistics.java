/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package com.salesforce.hbase.stats.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.coprocessor.BaseMasterObserver;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.Lists;
import com.salesforce.hbase.stats.StatWritingRegionObserver;
import com.salesforce.hbase.stats.StatisticsTable;
import com.salesforce.hbase.stats.util.SetupTableUtil;

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
    List<String> toAdd = classes == null? new ArrayList<String>(): Lists.newArrayList(classes);
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
    for(String name : desc.getCoprocessors()){
      //if its already been added, we are done
      if(name.equals(name)){
        return;
      }
    }
    //hasn't been added yet, so we need to add it to the table
    desc.addCoprocessor(toAdd);
  }

  /**
   * Cleanup the stats for the parent region on region split
   */
  public static class RemoveRegionOnSplit extends StatWritingRegionObserver {

    @Override
    public void postSplit(ObserverContext<RegionCoprocessorEnvironment> e, HRegion l, HRegion r)
        throws IOException {
      // get the parent
      HRegion parent = e.getEnvironment().getRegion();
      // and remove it from the stats
      stats.removeStatsForRegion(parent.getRegionInfo());
    }

  }

  public static class RemoveTableOnDelete extends BaseMasterObserver {

    @Override
    public void preDeleteTable(ObserverContext<MasterCoprocessorEnvironment> ctx, byte[] tableName)
        throws IOException {
      HTableDescriptor desc =ctx.getEnvironment().getMasterServices().getTableDescriptors().get(tableName);
      if(desc == null) {
        throw new IOException("Can't find table descriptor for table '" + Bytes.toString(tableName)
            + "' that is about to be deleted!");
      }
      // if we have turned on stats for this table
      if (SetupTableUtil.getStatsEnabled(desc)) {
        StatisticsTable stats = StatisticsTable.getStatisticsTableForCoprocessor(ctx.getEnvironment(), desc.getNameAsString());
        stats.removeStatsForTable(tableName);
        stats.close();
      }
    }
  }
}