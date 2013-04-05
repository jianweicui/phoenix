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

/**
 * Simple holder class for a metric on a ta
 */
public class MetricValue {

  private byte[] type;
  private byte[] info;
  private byte[] value;

  public MetricValue(byte[] type, byte[] info, byte[] value) {
    this.type = type;
    this.info = info;
    this.value = value;
  }

  public byte[] getType() {
    return type;
  }

  public byte[] getInfo() {
    return info;
  }

  public byte[] getValue() {
    return value;
  }

}
