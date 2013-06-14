package com.salesforce.hbase.index.builder;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;

/**
 * An index builder to help maintain fully consistent indexes
 */
public abstract class ConsistentIndexBuilder extends BaseIndexBuilder {

  protected HTableInterface localTable;

  @Override
  public void setup(RegionCoprocessorEnvironment env) throws IOException {
    // TODO this should just be a server-local table. We can do much better here, but for now this
    // works as a POC
    localTable = env.getTable(env.getRegion().getTableDesc().getName());
  }

  @Override
  public Map<Mutation, String> getIndexUpdate(Put put) throws IOException {
    // get the current state of the row in our table
    Result r = localTable.get(new Get(put.getRow()));
    return getIndexUpdate(r, put);
  }

  /**
   * Get the update that should be placed into the index when given the state of the current row
   * @param currentRowState current state of the row
   */
  protected abstract Map<Mutation, String> getIndexUpdate(Result currentRowState, Put put);

  @Override
  public Map<Mutation, String> getIndexUpdate(Delete delete) throws IOException {
    // get the current state of the row in our table
    Result r = localTable.get(new Get(delete.getRow()));
    return getIndexUpdate(r, delete);
  }

  /**
   * Get the update that should be placed into the index when given the state of the current row
   * @param currentRowState current state of the row
   */
  protected abstract Map<Mutation, String> getIndexUpdate(Result currentRowState, Delete delete);

}
