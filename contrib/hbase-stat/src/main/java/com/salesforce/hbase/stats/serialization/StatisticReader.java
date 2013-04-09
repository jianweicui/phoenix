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

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;

import com.salesforce.hbase.stats.ColumnFamilyStatistic;
import com.salesforce.hbase.stats.StatisticValue;
import com.salesforce.hbase.stats.StatisticsTable;

/**
 * Read a statistic from a {@link StatisticsTable}. This is an abstraction around the underlying
 * serialization format of a given statistic, allow us to change the format without exposing any
 * under the hood mechanics to the user.
 * <p>
 * Users should <b>not</b> use this class to read from the statistics table. Instead, you should
 * pass a reader a {@link StatisticsTable} and use its {@link StatisticsTable#read} methods to
 * properly deserialize the statistics
 * @param <S> Type of statistic that should be read
 */
public class StatisticReader<S extends StatisticValue> {

  private IndividualStatisticReader<S> serde;
  private byte[] name;

  public StatisticReader(IndividualStatisticReader<S> serde,
      byte[] statisticName) {
    this.serde = serde;
    this.name = statisticName;
  }

  public byte[] getRowKey(byte[] source) {
    return StatisticSerDe.getRowPrefix(source, name);
  }

  public byte[] getRowKey(byte[] source, byte[] regionname) {
    return StatisticSerDe.getRowPrefix(source, regionname, name);
  }

  public byte[] getRowKey(byte[] source, byte[] regionname,
 byte[] columnfamily) {
    return StatisticSerDe.getRowKey(source, regionname, columnfamily, name);
  }
  
  public ColumnFamilyStatistic<S> deserialize(Result r) throws IOException {
    return serde.deserialize(r);
  }


}