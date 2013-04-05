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
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.util.Bytes;

import com.salesforce.hbase.stats.MetricValue;
import com.salesforce.hbase.stats.StatScanner;
import com.salesforce.hbase.stats.StatisticCounter;
import com.salesforce.hbase.stats.StatisticsTable;

/**
 * Coprocessor that just keeps track of the min/max key on a per-column family basis.
 * <p>
 * This can then also be used to find the per-table min/max key for the table.
 */
public class MinMaxKey extends StatisticCounter {

  private static final byte[] MAX_SUFFIX = Bytes.toBytes("max_region_key");
  private static final byte[] MIN_SUFFIX = Bytes.toBytes("min_region_key");

  public static void addToTable(HTableDescriptor desc) throws IOException {
    desc.addCoprocessor(MinMaxKey.class.getName());
  }

  @Override
  protected InternalScanner getInternalScanner(ObserverContext<RegionCoprocessorEnvironment> c,
      Store store, InternalScanner internalScan) {
    return new MinMaxTrackingScanner(stats, store.getFamily(), c.getEnvironment().getRegion()
        .getRegionInfo(), internalScan);
  }

  private static class MinMaxTrackingScanner extends StatScanner {

    private byte[] column;
    private byte[] min;
    private byte[] max;

    public MinMaxTrackingScanner(StatisticsTable stats, HColumnDescriptor column,
        HRegionInfo region, InternalScanner delegate) {
      super(stats, region, delegate);
      this.column = column.getName();
    }

    protected synchronized void updateStat(final List<KeyValue> result) {
      for (KeyValue kv : result) {

        // first time through, so both are null
        if (min == null) {
          min = copyRow(kv);
          max = copyRow(kv);
          continue;
        }
        if (Bytes.compareTo(kv.getBuffer(), kv.getRowOffset(), kv.getRowLength(), min, 0,
          min.length) < 0) {
          min = copyRow(kv);
        }
        if (Bytes.compareTo(kv.getBuffer(), kv.getRowOffset(), kv.getRowLength(), max, 0,
          max.length) > 0) {
          max = copyRow(kv);
        }
      }
    }

    private byte[] copyRow(KeyValue kv) {
      return Arrays.copyOfRange(kv.getBuffer(), kv.getRowOffset(),
        kv.getRowOffset() + kv.getRowLength());
    }

    @Override
    protected List<MetricValue> getCurrentStats() {
      List<MetricValue> data = new ArrayList<MetricValue>(2);
      data.add(new MetricValue(column, MIN_SUFFIX, min));
      data.add(new MetricValue(column, MAX_SUFFIX, max));
      return data;
    }
  }

}
