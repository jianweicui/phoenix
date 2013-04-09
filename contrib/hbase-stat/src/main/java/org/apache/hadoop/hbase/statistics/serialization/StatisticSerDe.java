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
package org.apache.hadoop.hbase.statistics.serialization;


import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;


/**
 * Simple utility class for managing multiple key parts of the statistic
 */
public class StatisticSerDe {

  // XXX - this where we would use orderly to get the sorting consistent

  /** Number of parts in our complex key */
  protected static final int NUM_KEY_PARTS = 4;

  /**
   * Get the prefix based on the region, column and name of the statistic
   * @param table name of the source table
   * @param statName name of the statistic
   * @return the row key that should be used for this statistic
   */
  public static byte[] getRowPrefix(byte[] table, byte[] statName) {
    byte[] prefix = table;
    prefix = Bytes.add(prefix, statName);
    return prefix;
  }

  /**
   * Get the prefix based on the region, column and name of the statistic
   * @param table name of the source table
   * @param regionname name of the region where the statistic was gathered
   * @param statName name of the statistic
   * @return the row key that should be used for this statistic
   */
  public static byte[] getRowPrefix(byte[] table, byte[] regionname, byte[] statName) {
    byte[] prefix = table;
    prefix = Bytes.add(prefix, statName);
    prefix = Bytes.add(prefix, regionname);
    return prefix;
  }

  /**
   * Get the prefix based on the region, column and name of the statistic
   * @param table name of the source table
   * @param region name of the region where the statistic was gathered
   * @param column column for which the statistic was gathered
   * @param statName name of the statistic
   * @return the row key that should be used for this statistic
   */
  public static byte[] getRowKey(byte[] table, byte[] region, byte[] column, byte[] statName) {
    // always starts with the source table
    byte[] prefix = new byte[0];
    // then append each part of the key and
    byte[][] parts = new byte[][] { table, statName, region, column };
    int[] sizes = new int[NUM_KEY_PARTS];
    // XXX - this where we would use orderly to get the sorting consistent
    for (int i = 0; i < NUM_KEY_PARTS; i++) {
      prefix = Bytes.add(prefix, parts[i]);
      sizes[i] = parts[i].length;
    }
    // then we add on the sizes to the end of the key
    for (int size : sizes) {
      prefix = Bytes.add(prefix, Bytes.toBytes(size));
    }

    return prefix;
  }
}