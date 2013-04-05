/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.salesforce.hbase.stats;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.salesforce.hbase.stats.util.Constants;
import com.salesforce.hbase.stats.util.SetupTableUtil;


/**
 * Wrapper to access the statistics table for an HTable.
 * <p>
 * Each statistic is prefixed with the tablename and region from whence it came.
 */
public class StatisticsTable implements Closeable {

  private static final Map<String, StatisticsTable> tableMap = new HashMap<String, StatisticsTable>();

  /**
   * @param env Environment wherein the coprocessor is attempting to update the stats table.
   * @param primaryTableName name of the primary table on which we should collect stats
   * @return the {@link StatisticsTable} for the given primary table.
   * @throws IOException
   */
  public synchronized static StatisticsTable getStatisticsTableForCoprocessor(
      CoprocessorEnvironment env,
      String primaryTableName) throws IOException {
    StatisticsTable table = tableMap.get(primaryTableName);
    if (table == null) {
      table = new StatisticsTable(env.getTable(Constants.STATS_TABLE_NAME_BYTES));
      tableMap.put(primaryTableName, table);
    }
    return table;
  }

  private HTableInterface target;

  private StatisticsTable(HTableInterface stats) {
    this.target = stats;
  }

  private StatisticsTable(Configuration conf, HTableDescriptor primaryTable) throws IOException {
    this(new HTable(conf, Constants.STATS_TABLE_NAME_BYTES));
  }

  /**
   * Close the connection to the table
   */
  @Override
  public void close() throws IOException {
    target.close();
  }

  public void removeStatsForTable(byte[] tablename) throws IOException {
    removeRowsForPrefix(tablename);
  }

  public void removeStatsForRegion(HRegionInfo region) throws IOException {
    removeRowsForPrefix(region.getTableName(), region.getRegionName());
  }

  private void removeRowsForPrefix(byte[]... arrays) throws IOException {
    byte[] row = null;
    for (byte[] array : arrays) {
      row = ArrayUtils.addAll(row, array);
    }
    Scan scan = new Scan(row);
    scan.setFilter(new PrefixFilter(row));
    cleanupRows(scan);
  }

  /**
   * Delete all the rows that we find from the scanner
   * @param scan scan used on the statistics table to determine which keys need to be deleted
   * @throws IOException if we fail to communicate with the HTable
   */
  private void cleanupRows(Scan scan) throws IOException {
    // Because each region has, potentially, a bunch of different statistics, we need to go through
    // an delete each of them as we find them

    // TODO switch this to a CP that lets us just do a filtered delete

    // first we have to scan the table to find the rows to delete
    ResultScanner scanner = target.getScanner(scan);
    Delete d = null;
    // XXX possible memory issues here - we could be loading a LOT of stuff as we are doing a
    // copy for each result
    for (Result r : scanner) {
      // create a delete for each result
      d = new Delete(r.getRow());
      // let the table figure out when it wants to flush that stuff
      target.delete(d);
    }
  }

  /**
   * Update a list of statistics for the given region
   * @param region the region for which we are updating. This is used (together with the source
   *          table name) to build the row key.
   * @param stats Statistics for the region that we should update. The type of the
   *          {@link MetricValue} (T1), is used as a suffix on the row key; this groups different
   *          types of metrics together on a per-region basis. Then the
   *          {@link MetricValue#getInfo()}is used as the column qualifier. Finally,
   *          {@link MetricValue#getValue()} is used for the the value of the {@link Put}. This can
   *          be <tt>null</tt> or <tt>empty</tt>.
   * @throws IOException if we fail to do any of the puts. Any single failure will prevent any
   *           future attempts for the remaining list of stats to update
   */
  public void updateStats(HRegionInfo region, List<MetricValue> stats)
      throws IOException {
    // short circuit if we have nothing to write
    if (stats == null || stats.size() == 0) {
      return;
    }
    // first build the key
    for (MetricValue metric : stats) {
      byte[] rowkey = ArrayUtils.addAll(
        ArrayUtils.addAll(region.getTableName(), region.getRegionName()), metric.getType());
      Put p = new Put(rowkey);
      p.add(Constants.STATS_COLUMN_FAMILY, metric.getInfo(), metric.getValue());
      target.put(p);
    }
    // make sure it all reaches the target table when we are done
    target.flushCommits();
  }
}