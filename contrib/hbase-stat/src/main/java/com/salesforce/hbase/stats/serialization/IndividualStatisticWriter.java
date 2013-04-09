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
package com.salesforce.hbase.stats.serialization;

import org.apache.hadoop.hbase.client.Put;

import com.salesforce.hbase.stats.StatisticValue;
import com.salesforce.hbase.stats.util.Constants;

/**
 * Simple serializer that always puts generates the same formatted key for an individual
 * statistic. This writer is used to write a single {@link StatisticValue} to the statistics
 * table. They should be read back via an {@link IndividualStatisticReader}.
 */
public class IndividualStatisticWriter {
  private final byte[] source;
  private byte[] region;
  private byte[] column;

  public IndividualStatisticWriter(byte[] sourcetable, byte[] region, byte[] column) {
    this.source = sourcetable;
    this.region = region;
    this.column = column;
  }

  public Put serialize(StatisticValue value) {
    byte[] prefix = StatisticSerDe.getRowKey(source, region, column, value.getType());
    Put put = new Put(prefix);
    put.add(Constants.STATS_DATA_COLUMN_FAMILY, value.getInfo(), value.getValue());
    return put;
  }

}