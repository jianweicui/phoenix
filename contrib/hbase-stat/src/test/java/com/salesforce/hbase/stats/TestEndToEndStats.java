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
package com.salesforce.hbase.stats;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.salesforce.hbase.stats.impl.MinMaxKey;
import com.salesforce.hbase.stats.util.Constants;
import com.salesforce.hbase.stats.util.SetupTableUtil;

/**
 * 
 */
public class TestEndToEndStats {

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final byte[] FAM = Bytes.toBytes("FAMILY");

  @BeforeClass
  public static void setupCluster() throws Exception {
    SetupTableUtil.setupCluster(UTIL.getConfiguration());
    UTIL.startMiniCluster();
  }

  @AfterClass
  public static void teardownCluster() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testSimplePrimaryAndStatsTables() throws Exception {
    HTableDescriptor primary = new HTableDescriptor("primary");
    primary.addFamily(new HColumnDescriptor(FAM));
    
    //add the min/max key stats
    MinMaxKey.addToTable(primary);

    // setup the stats table
    HBaseAdmin admin = UTIL.getHBaseAdmin();
    SetupTableUtil.setupTable(admin, primary, true);
    // create the primary table
    admin.createTable(primary);

    String statTableName = Constants.STATS_TABLE_NAME;
    assertTrue("Stats table didn't get created!", admin.tableExists(statTableName));

    // load some data into our primary table
    HTable primaryTable = new HTable(UTIL.getConfiguration(), primary.getName());
    UTIL.loadTable(primaryTable, FAM);

    // now flush and compact our table
    HRegionServer server = UTIL.getRSForFirstRegionInTable(primary.getName());
    List<HRegion> regions = server.getOnlineRegions(primary.getName());
    assertTrue("Didn't find any regions for primary table!", regions.size() > 0);
    // flush and compact all the regions of the primary table
    for (HRegion region : regions) {
      region.flushcache();
      region.compactStores(true);
    }

    // and now scan the stats table
    HTable stats = new HTable(UTIL.getConfiguration(), statTableName);
    int count = getKeyValueCount(stats);

    // we should have 2 stats - a min and a max for the one column of the one region of the table
    assertEquals("Got an unexpected amount of stats!", 2, count);

    // then delete the table and make sure we don't have any more stats in our table
    admin.disableTable(primary.getName());
    admin.deleteTable(primary.getName());

    assertEquals("Stats table still has values after primary table delete", 0,
      getKeyValueCount(stats));
  }

  private int getKeyValueCount(HTable table) throws IOException {
    Scan scan = new Scan();
    scan.setMaxVersions(Integer.MAX_VALUE - 1);

    ResultScanner results = table.getScanner(scan);
    int count = 0;
    for (Result res : results) {
      count += res.list().size();
      System.out.println(count + ") " + res);
    }
    results.close();

    return count;

  }
}