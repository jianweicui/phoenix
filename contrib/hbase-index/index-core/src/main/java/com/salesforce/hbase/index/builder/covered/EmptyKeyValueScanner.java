package com.salesforce.hbase.index.builder.covered;

import java.io.IOException;
import java.util.SortedSet;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;

/**
 * {@link KeyValueScanner} that is backed by no keys.
 */
public class EmptyKeyValueScanner implements KeyValueScanner {

  @Override
  public KeyValue peek() {
    // TODO Auto-generated method stub
  }

  @Override
  public KeyValue next() throws IOException {
    // TODO Auto-generated method stub
  }

  @Override
  public boolean seek(KeyValue key) throws IOException {
    // TODO Auto-generated method stub
  }

  @Override
  public boolean reseek(KeyValue key) throws IOException {
    // TODO Auto-generated method stub
  }

  @Override
  public long getSequenceID() {
    // TODO Auto-generated method stub
  }

  @Override
  public void close() {
    // TODO Auto-generated method stub
  }

  @Override
  public boolean shouldUseScanner(Scan scan, SortedSet<byte[]> columns, long oldestUnexpiredTS) {
    // TODO Auto-generated method stub
  }

  @Override
  public boolean requestSeek(KeyValue kv, boolean forward, boolean useBloom) throws IOException {
    // TODO Auto-generated method stub
  }

  @Override
  public boolean realSeekDone() {
    // TODO Auto-generated method stub
  }

  @Override
  public void enforceSeek() throws IOException {
    // TODO Auto-generated method stub
  }

  @Override
  public boolean isFileScanner() {
    // TODO Auto-generated method stub
  }

}
