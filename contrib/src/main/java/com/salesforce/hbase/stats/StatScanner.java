package com.salesforce.hbase.stats;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.io.MultipleIOException;

import com.google.common.collect.Lists;

public abstract class StatScanner implements InternalScanner {
  private static final Log LOG = LogFactory.getLog(StatScanner.class);
  private InternalScanner delegate;
  private StatisticsTable stats;
  private HRegionInfo region;

  public StatScanner(StatisticsTable stats, HRegionInfo region, InternalScanner delegate) {
    this.stats = stats;
    this.delegate = delegate;
    this.region = region;
  }

  public boolean next(List<KeyValue> result) throws IOException {
    boolean ret = delegate.next(result);
    updateStat(result);
    return ret;
  }

  public boolean next(List<KeyValue> result, String metric) throws IOException {
    boolean ret = delegate.next(result, metric);
    updateStat(result);
    return ret;
  }

  public boolean next(List<KeyValue> result, int limit) throws IOException {
    boolean ret = delegate.next(result, limit);
    updateStat(result);
    return ret;
  }

  public boolean next(List<KeyValue> result, int limit, String metric) throws IOException {
    boolean ret = delegate.next(result, limit, metric);
    updateStat(result);
    return ret;
  }

  /**
   * Update the current statistics based on the lastest batch of key-values from the underlying
   * scanner
   * @param results next batch of {@link KeyValue}s
   */
  protected abstract void updateStat(final List<KeyValue> results);

  /**
   * @return the metrics that should be written for the stats table. This is called <b>exactly
   *         once</b> for each compaction.
   */
  protected abstract List<MetricValue> getCurrentStats();

  public void close() throws IOException {
    IOException toThrow = null;
    try {
      // update the statistics table
      List<MetricValue> data = getCurrentStats();
      stats.updateStats(region, data);
    } catch (IOException e) {
      LOG.error("Failed to update statistics table!", e);
      toThrow = e;
    }
    // close the delegate scanner
    try {
      delegate.close();
    } catch (IOException e) {
      if (toThrow == null) {
        throw e;
      }
      throw MultipleIOException.createIOException(Lists.newArrayList(toThrow, e));
    }
  }
}
