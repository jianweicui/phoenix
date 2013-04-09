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
package com.salesforce.hbase.stats.cleanup;

import java.io.IOException;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.coprocessor.BaseMasterObserver;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.util.Bytes;

import com.salesforce.hbase.stats.StatisticsTable;
import com.salesforce.hbase.stats.util.SetupTableUtil;

public class RemoveTableOnDelete extends BaseMasterObserver {

  @Override
  public void preDeleteTable(ObserverContext<MasterCoprocessorEnvironment> ctx, byte[] tableName)
      throws IOException {
    HTableDescriptor desc = ctx.getEnvironment().getMasterServices().getTableDescriptors()
        .get(tableName);
    if (desc == null) {
      throw new IOException("Can't find table descriptor for table '" + Bytes.toString(tableName)
          + "' that is about to be deleted!");
    }
    // if we have turned on stats for this table
    if (SetupTableUtil.getStatsEnabled(desc)) {
      StatisticsTable stats = StatisticsTable.getStatisticsTableForCoprocessor(
        ctx.getEnvironment(), desc.getNameAsString());
      stats.removeStats();
      stats.close();
    }
  }
}