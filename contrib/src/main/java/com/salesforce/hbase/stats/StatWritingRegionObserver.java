package com.salesforce.hbase.stats;

import java.io.IOException;

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;



public class StatWritingRegionObserver extends BaseRegionObserver {
  protected StatisticsTable stats;

  @Override
  public void start(CoprocessorEnvironment e) throws IOException {
    HTableDescriptor desc = ((RegionCoprocessorEnvironment) e).getRegion().getTableDesc();
    stats = StatisticsTable.getStatisticsTableForCoprocessor(e, desc.getNameAsString());
  }

  
  @Override
  public void stop(CoprocessorEnvironment e) throws IOException {
    stats.close();
  }
}
