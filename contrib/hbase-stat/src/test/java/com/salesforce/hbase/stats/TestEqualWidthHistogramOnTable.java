package com.salesforce.hbase.stats;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.List;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.statistics.ColumnFamilyStatistic;
import org.apache.hadoop.hbase.statistics.HistogramStatisticValue;
import org.apache.hadoop.hbase.statistics.StatisticsTable;
import org.apache.hadoop.hbase.statistics.serialization.StatisticReader;
import org.apache.hadoop.hbase.util.Bytes;

import com.salesforce.hbase.protobuf.generated.StatisticProtos.HistogramColumn;
import com.salesforce.hbase.stats.impl.EqualDepthHistogramStatisticTracker;
import com.salesforce.hbase.stats.util.Constants;
import com.salesforce.hbase.stats.util.StatsTestUtil;

/**
 * A full, real table test of the the {@link EqualDepthHistogramStatisticTracker}. This is the
 * complement to {@link TestEqualWidthHistogramStat}.
 */
public class TestEqualWidthHistogramOnTable extends TestTrackerImpl {

  // number of keys in each column
  private final int columnWidth = 676;
  // depth is the width (count of keys) times the number of bytes of each key, which in this case is
  // fixed to 3 bytes, so we know the depth in all cases
  private final int columnDepth = columnWidth * 3;

  @Override
  protected void preparePrimaryTableDescriptor(HTableDescriptor primary) throws Exception {
    EqualDepthHistogramStatisticTracker.addToTable(primary, columnDepth);
  }

  @Override
  protected void verifyStatistics(HTableDescriptor primary) throws Exception {
    // scan the stats table for a raw count
    HTable statTable = new HTable(UTIL.getConfiguration(), Constants.STATS_TABLE_NAME);
    int count = StatsTestUtil.getKeyValueCount(statTable);

    // we should have just 1 stat - our histogram
    assertEquals("Got an unexpected amount of stats!", 1, count);
    StatisticsTable table = new StatisticsTable(UTIL.getConfiguration(), primary);

    // now get a custom reader to interpret the results
    StatisticReader<HistogramStatisticValue> reader = EqualDepthHistogramStatisticTracker.getStatistcReader(primary);
    List<ColumnFamilyStatistic<HistogramStatisticValue>> stats = table.read(reader);

    // should only have a single column family
    assertEquals("More than one column family has statistics!", 1, stats.size());
    List<HistogramStatisticValue> values = stats.get(0).getValues();
    assertEquals("More than one histogram in the column family/region", 1, values.size());
    HistogramStatisticValue value = values.get(0);
    List<HistogramColumn> columns = value.getHistogram().getColumnsList();
    assertEquals("Got the wrong number of columns in the equal-depth histogram", 26, columns.size());

    // make sure we have the expected guide posts
    byte counter = 'a';
    for (HistogramColumn column : columns) {
      assertEquals("Column width shouldn't vary for a equal-depth histogram", columnWidth,
        column.getWidth());
      byte[] guidepost = new byte[] { counter, 'z', 'z' };
      byte[] actual = column.getValue().toByteArray();
      assertArrayEquals(
        "Guidepost should be:" + Bytes.toString(guidepost) + " , but was: "
            + Bytes.toString(actual), guidepost, actual);
      counter++;
    }

    // cleanup
    statTable.close();
    table.close();
  }

}
