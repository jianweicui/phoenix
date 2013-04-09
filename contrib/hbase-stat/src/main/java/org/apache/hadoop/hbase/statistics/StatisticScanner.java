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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.statistics.serialization.IndividualStatisticWriter;
import org.apache.hadoop.io.MultipleIOException;

import com.google.common.collect.Lists;

/**
 * Delegating scanner that just updates a bunch of statistics based on the configured trackers
 */
public class StatisticScanner implements InternalScanner {

  private static final Log LOG = LogFactory.getLog(StatisticScanner.class);

  private Collection<StatisticTracker> trackers = new ArrayList<StatisticTracker>();

  private StatisticsTable stats;
  private InternalScanner delegate;
  private HRegionInfo region;
  private final byte[] family;

  public StatisticScanner(HRegion parent, InternalScanner delegate, byte[] family)
      throws IOException {
    this.stats = new StatisticsTable(parent.getConf(), parent.getTableDesc());
    this.delegate = delegate;
    this.region = parent.getRegionInfo();
    this.family = family;
  }

  public void addStatisticTracker(StatisticTracker tracker) {
    this.trackers.add(tracker);
  }

  public void setTrackers(Collection<StatisticTracker> trackers) {
    this.trackers = trackers;
  }

  public boolean next(List<KeyValue> result) throws IOException {
    boolean ret = delegate.next(result);
    updateStat(result);
    return ret;
  }

  public boolean next(List<KeyValue> result, String metric) throws IOException {
    boolean ret = delegate.next(result, metric);
    updateStat(result);
    return ret;
  }

  public boolean next(List<KeyValue> result, int limit) throws IOException {
    boolean ret = delegate.next(result, limit);
    updateStat(result);
    return ret;
  }

  public boolean next(List<KeyValue> result, int limit, String metric) throws IOException {
    boolean ret = delegate.next(result, limit, metric);
    updateStat(result);
    return ret;
  }

  /**
   * Update the current statistics based on the lastest batch of key-values from the underlying
   * scanner
   * @param results next batch of {@link KeyValue}s
   */
  protected void updateStat(final List<KeyValue> results) {
    // pass each key-value into each tracker, so we can update the statistic for it
    for (KeyValue kv : results) {
      for (StatisticTracker tracker : trackers) {
        tracker.updateStatistic(kv);
      }
    }
  }

  /**
   * Write out the gathered statistics to the statistics table {@inheritDoc}
   */
  @Override
  public void close() throws IOException {
    IOException toThrow = null;
    try {
      // get all the data from each of the trackers
      List<StatisticValue> data = new ArrayList<StatisticValue>(trackers.size());

      // this is where we would tie in a custom SerDe for each statistic, if we wanted to do custom
      // key specifications, etc

      for (StatisticTracker tracker : trackers) {
        List<StatisticValue> values = tracker.getCurrentStats();
        if (values != null) {
          // tie the serializer for each tracker to the data to serialize
          data.addAll(values);
        }
        // and tell the tracker that it should reset its counter
        tracker.clear();
      }

      // update the statistics table - OK to be best effort here - we can always run a compaction
      // again and fix the stats
      stats.updateStats(
        new IndividualStatisticWriter(region.getTableName(), region.getRegionName(),
            family), data);
    } catch (IOException e) {
      // it would be nice to be able to surface this error to the user in a better way
      LOG.error("Failed to update statistics table!", e);
      toThrow = e;
    }
    // close the delegate scanner
    try {
      delegate.close();
    } catch (IOException e) {
      if (toThrow == null) {
        throw e;
      }
      throw MultipleIOException.createIOException(Lists.newArrayList(toThrow, e));
    }
  }
}
