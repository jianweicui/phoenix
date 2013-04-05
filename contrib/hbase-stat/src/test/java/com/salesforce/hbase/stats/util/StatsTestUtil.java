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
package com.salesforce.hbase.stats.util;

import org.apache.hadoop.hbase.HTableDescriptor;

/**
 * Helper utility for testing
 */
public class StatsTestUtil {

  /**
   * @return a valid {@link HTableDescriptor} for the primary table on which we want to collect
   *         statistics
   */
  public static HTableDescriptor getValidPrimaryTableDescriptor() {
    HTableDescriptor table = new HTableDescriptor("primary_table_for_test");
    return table;
  }

}
