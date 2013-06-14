package com.salesforce.hbase.index.builder;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;

/**
 * ZooKeeper backed configuration for determining basic configuration for a cluster
 */
public class ZooKeeperIndexConfiguration implements Abortable {

  private static Log LOG = LogFactory.getLog(ZooKeeperIndexConfiguration.class);
  private static final String DEFAULT_PREFIX = "/salesforce/hbase/indexing";

  private ZooKeeperWatcher watcher;
  private boolean aborted;

  public ZooKeeperIndexConfiguration(Configuration conf) throws ZooKeeperConnectionException,
      IOException {
    this.watcher = new ZooKeeperWatcher(conf, "ZooKeeperIndexConfiguration", this);
  }

  public ZooKeeperIndexConfiguration(ZooKeeperWatcher watcher) {
    this.watcher = watcher;
  }

  @Override
  public void abort(String why, Throwable e) {
    if (this.aborted) {
      return;
    }
    LOG.info("Aborting because: " + why, e);
    // try to close the watcher - the watcher may have aborted us, but don't worry about it too much
    this.watcher.close();
  }

  @Override
  public boolean isAborted() {
    return this.aborted;
  }

}
