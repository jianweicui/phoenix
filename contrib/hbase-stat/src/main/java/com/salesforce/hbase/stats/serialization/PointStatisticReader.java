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

import java.util.Arrays;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.salesforce.hbase.stats.ColumnFamilyStatistic;
import com.salesforce.hbase.stats.StatisticValue;

/**
 * Read simple {@link StatisticValue}s from raw {@link Result}s. Expects serialization with the
 * {@link IndividualStatisticWriter}.
 */
public class PointStatisticReader implements IndividualStatisticReader<StatisticValue> {

  public ColumnFamilyStatistic<StatisticValue> deserialize(Result r) {
    // break out the key based on its parts
    // 1. start with getting the lengths of the key parts
    byte[] row = r.getRow();
    int sizes[] = new int[StatisticSerDe.NUM_KEY_PARTS];
    int start = row.length - Bytes.SIZEOF_INT;
    for (int i = StatisticSerDe.NUM_KEY_PARTS - 1; i >= 0; i--) {
      sizes[i] = Bytes.toInt(row, start, Bytes.SIZEOF_INT);
      start -= Bytes.SIZEOF_INT;
    }

    // 1b. break out each part of the key so we can rebuild the statistic
    start = sizes[0]; // this is the end of the table name, so we can just skip it immediately
    int end = start + sizes[1];
    // for right now, we just copy the array over - its a bit inefficient, but we can always go to
    // ByteBuffers later.
    byte[] statname = Arrays.copyOfRange(row, start,end);
    start += sizes[1];
    end= start+ sizes[2];
    byte[] region = Arrays.copyOfRange(row, start, end);
    start += sizes[2];
    end= start+ sizes[3];
    byte[] family = Arrays.copyOfRange(row, start, end);
    ColumnFamilyStatistic<StatisticValue> stat =
        new ColumnFamilyStatistic<StatisticValue>(region, family);
    for (KeyValue kv : r.list()) {
      byte[] info = Arrays.copyOfRange(kv.getBuffer(), kv.getQualifierOffset(),
        kv.getQualifierOffset() + kv.getQualifierLength());
      byte[] value = Arrays.copyOfRange(kv.getBuffer(), kv.getValueOffset(), kv.getValueOffset()
          + kv.getValueLength());
      stat.add(new StatisticValue(statname, info, value));
    }
    return stat;
  }
}