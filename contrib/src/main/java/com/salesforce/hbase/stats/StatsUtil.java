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

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreScanner;

/**
 * 
 */
public class StatsUtil {

  private StatsUtil() {
    // private ctor for util classes
  }

  /**
   * see {@link RegionObserver#preCompactScannerOpen(ObserverContext, Store, List, ScanType, long,
   *     InternalScanner)}
   * @return a standard {@link InternalScanner} if the requested compaction is a major comapction.
   *         If it is a {@link ScanType#MINOR_COMPACT}, <tt>null</tt> is returned.
   * @throws IOException
   */
  @SuppressWarnings("javadoc")
  public static InternalScanner getMajorCompactionScanner(
      ObserverContext<RegionCoprocessorEnvironment> c,
      Store store, List<? extends KeyValueScanner> scanners, ScanType scanType, long earliestPutTs,
      InternalScanner s) throws IOException {
    InternalScanner internalScan = null;
    if (scanType.equals(ScanType.MAJOR_COMPACT)) {
      // create a majorcompaction scanner, just like we do in the Compactor
      Scan scan = new Scan();
      scan.setMaxVersions(store.getFamily().getMaxVersions());
      long smallestReadPoint = store.getHRegion().getSmallestReadPoint();
      internalScan = new StoreScanner(store, store.getScanInfo(), scan, scanners, scanType,
          smallestReadPoint, earliestPutTs);
    }
    return internalScan;
  }
}
