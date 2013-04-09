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
package com.salesforce.hbase.stats.impl;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.statistics.HistogramStatisticValue;
import org.apache.hadoop.hbase.statistics.StatisticTracker;
import org.apache.hadoop.hbase.statistics.StatisticValue;
import org.apache.hadoop.hbase.statistics.serialization.IndividualStatisticReader;
import org.apache.hadoop.hbase.statistics.serialization.StatisticReader;
import org.apache.hadoop.hbase.util.Bytes;

import com.salesforce.hbase.stats.BaseStatistic;

/**
 * {@link StatisticTracker} that keeps track of an equal depth histogram.
 * <p>
 * This is different from a traditional histogram in that we just keep track of the key at every 'n'
 * bytes; another name for this is region "guide posts".
 */
public class EqualDepthHistogramStatisticTracker extends BaseStatistic {

  public static final String BYTE_DEPTH_CONF_KEY = "com.salesforce.guidepost.width";

  private final static byte[] NAME = Bytes.toBytes("equal_depth_histogram");

  private static final long DEFAULT_BYTE_DEPTH = 100;

  private long guidepostDepth;
  private long byteCount = 0;
  private int keyCount;
  private HistogramStatisticValue histogram;

  public static void addToTable(HTableDescriptor desc, long depth) throws IOException {
    Map<String, String> props = Collections.singletonMap(BYTE_DEPTH_CONF_KEY, Long.toString(depth));
    desc.addCoprocessor(EqualDepthHistogramStatisticTracker.class.getName(), null,
      Coprocessor.PRIORITY_USER, props);
  }

  /**
   * Get a reader for the statistic
   * @param primary table for which you want to read the stats
   * @return a {@link StatisticReader} to get the raw Histogram stats.
   */
  public static StatisticReader<HistogramStatisticValue> getStatistcReader(HTableDescriptor primary) {
    return new StatisticReader<HistogramStatisticValue>(
        new IndividualStatisticReader.HistogramStatisticReader(), NAME);
  }

  @Override
  public void start(CoprocessorEnvironment e) throws IOException {
   super.start(e);
   //get the byte depth for this histogram
    guidepostDepth = e.getConfiguration().getLong(BYTE_DEPTH_CONF_KEY, DEFAULT_BYTE_DEPTH);
    this.histogram = newHistogram();
  }

  private HistogramStatisticValue newHistogram() {
    return new HistogramStatisticValue(NAME,
        Bytes.toBytes("equal_width_histogram_" + guidepostDepth + "bytes"));
  }

  @Override
  public List<StatisticValue> getCurrentStats() {
    return Collections.singletonList((StatisticValue) histogram);
  }

  @Override
  public void clear() {
    this.histogram = newHistogram();
    this.byteCount = 0;
    this.keyCount = 0;
  }

  @Override
  public void updateStatistic(KeyValue kv) {
    byteCount += kv.getBuffer().length;
    keyCount++;
    // if we are at the next guide-post, add it to the histogram
    if (byteCount >= guidepostDepth) {
      // update the histogram
      this.histogram.addColumn(keyCount, kv.getBuffer());

      //reset the count for the next key
      byteCount = 0;
      keyCount = 0;
    }
  }
}
