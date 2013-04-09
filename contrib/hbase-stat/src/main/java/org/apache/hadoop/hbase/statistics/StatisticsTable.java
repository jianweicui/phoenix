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
package org.apache.hadoop.hbase.statistics;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.statistics.serialization.IndividualStatisticWriter;
import org.apache.hadoop.hbase.statistics.serialization.StatisticReader;
import org.apache.hadoop.hbase.util.Bytes;

import com.salesforce.hbase.stats.util.Constants;


/**
 * Wrapper to access the statistics table for an HTable.
 * <p>
 * Each {@link StatisticsTable} is bound to access the statistics for a single 'primary' table. This
 * helps decrease the chances of reading/writing the wrong statistic for the source table
 * <p>
 * Each statistic is prefixed with the tablename and region from whence it came.
 */
public class StatisticsTable implements Closeable {

  private static final Log LOG = LogFactory.getLog(StatisticsTable.class);
  // by default, we only return the latest version of a statistc
  private static final int DEFAULT_VERSIONS = 1;

  /** Map of the currently open statistics tables */
  private static final Map<String, StatisticsTable> tableMap = new HashMap<String, StatisticsTable>();

  /**
   * @param env Environment wherein the coprocessor is attempting to update the stats table.
   * @param primaryTableName name of the primary table on which we should collect stats
   * @return the {@link StatisticsTable} for the given primary table.
   * @throws IOException
   */
  public synchronized static StatisticsTable getStatisticsTableForCoprocessor(
      CoprocessorEnvironment env, String primaryTableName) throws IOException {
    StatisticsTable table = tableMap.get(primaryTableName);
    if (table == null) {
      table = new StatisticsTable(env.getTable(Constants.STATS_TABLE_NAME_BYTES));
      tableMap.put(primaryTableName, table);
    }
    return table;
  }

  private HTableInterface target;
  private byte[] sourceTableName;

  private StatisticsTable(HTableInterface target) {
    this.target = target;
    this.sourceTableName = target.getTableName();
  }

  public StatisticsTable(Configuration conf, HTableDescriptor source) throws IOException {
    this.target = new HTable(conf, Constants.STATS_TABLE_NAME);
    this.sourceTableName = source.getName();
  }

  /**
   * Close the connection to the table
   */
  @Override
  public void close() throws IOException {
    target.close();
  }

  public void removeStats() throws IOException {
    removeRowsForPrefix(sourceTableName);
  }

  public void removeStatsForRegion(HRegionInfo region) throws IOException {
    removeRowsForPrefix(sourceTableName, region.getRegionName());
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
   * @param serializer to convert the actual statistics to puts in the statistics table
   * @param data Statistics for the region that we should update. The type of the
   *          {@link StatisticValue} (T1), is used as a suffix on the row key; this groups different
   *          types of metrics together on a per-region basis. Then the
   *          {@link StatisticValue#getInfo()}is used as the column qualifier. Finally,
   *          {@link StatisticValue#getValue()} is used for the the value of the {@link Put}. This
   *          can be <tt>null</tt> or <tt>empty</tt>.
   * @throws IOException if we fail to do any of the puts. Any single failure will prevent any
   *           future attempts for the remaining list of stats to update
   */
  public void updateStats(IndividualStatisticWriter serializer, List<StatisticValue> data)
      throws IOException {
    // short circuit if we have nothing to write
    if (data == null || data.size() == 0) {
      return;
    }

    // serialize each of the metrics with the associated serializer
    for (StatisticValue metric : data) {
      target.put(serializer.serialize(metric));
    }
    // make sure it all reaches the target table when we are done
    target.flushCommits();
  }

  public <S extends StatisticValue> List<ColumnFamilyStatistic<S>> read(StatisticReader<S> reader)
      throws IOException {
    return read(reader, DEFAULT_VERSIONS);
  }

  public <S extends StatisticValue> List<ColumnFamilyStatistic<S>> read(StatisticReader<S> reader,
      int versions) throws IOException {
    byte[] scanPrefix = reader.getRowKey(sourceTableName);
    LOG.info("Reading for prefix: " + Bytes.toString(scanPrefix));
    return getResults(target, scanPrefix, reader, versions);
  }

  public <S extends StatisticValue> List<ColumnFamilyStatistic<S>> read(StatisticReader<S> reader,
      byte[] region) throws IOException {
    return read(reader, region, DEFAULT_VERSIONS);
  }

  public <S extends StatisticValue> List<ColumnFamilyStatistic<S>> read(StatisticReader<S> reader,
      byte[] region, int versions) throws IOException {
    byte[] scanPrefix = reader.getRowKey(sourceTableName, region);
    LOG.info("Reading for prefix: " + Bytes.toString(scanPrefix));
    return getResults(target, scanPrefix, reader, versions);
  }

  public <S extends StatisticValue> ColumnFamilyStatistic<S> read(byte[] region, byte[] column,
      StatisticReader<S> reader) throws IOException {
    return read(region, column, reader, DEFAULT_VERSIONS);
  }

  public <S extends StatisticValue> ColumnFamilyStatistic<S> read(byte[] region, byte[] column,
      StatisticReader<S> reader, int versions) throws IOException {
    byte[] row = reader.getRowKey(sourceTableName, region, column);
    Get g = new Get(row);
    g.setMaxVersions(versions);
    Result r = target.get(g);
    return reader.deserialize(r);
  }

  /**
   * Read the latest version of the statistic from the pr
   * @param t
   * @param scan
   * @return
   * @throws IOException
   */
  private <S extends StatisticValue> List<ColumnFamilyStatistic<S>> getResults(HTableInterface t,
      byte[] prefix,
 StatisticReader<S> reader, int versions) throws IOException {
    Scan scan = new Scan(prefix);
    scan.addFamily(Constants.STATS_DATA_COLUMN_FAMILY);
    scan.setFilter(new PrefixFilter(prefix));
    // we only return the latest version of the statistic
    scan.setMaxVersions(versions);
    ResultScanner scanner = t.getScanner(scan);
    List<ColumnFamilyStatistic<S>> stats = new ArrayList<ColumnFamilyStatistic<S>>();
    for (Result r : scanner) {
      LOG.info("Got result:" + r);
      stats.add(reader.deserialize(r));
    }
    return stats;
  }

  /**
   * @return the underlying {@link HTableInterface} to which this table is writing
   */
  public HTableInterface getTable() {
    return target;
  }
}