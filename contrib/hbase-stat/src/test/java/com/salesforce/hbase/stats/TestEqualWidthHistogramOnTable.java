package com.salesforce.hbase.stats;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.List;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.ByteString;
import com.salesforce.hbase.protobuf.generated.StatisticProtos.Histogram;
import com.salesforce.hbase.stats.impl.EqualByteDepthHistogramStatisticTracker;
import com.salesforce.hbase.stats.util.Constants;
import com.salesforce.hbase.stats.util.StatsTestUtil;

/**
 * A full, real table test of the the {@link EqualByteDepthHistogramStatisticTracker}. This is the
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
    EqualByteDepthHistogramStatisticTracker.addToTable(primary, columnDepth);
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
    StatisticReader<HistogramStatisticValue> reader =
        EqualByteDepthHistogramStatisticTracker.getStatistcReader(table);
    List<ColumnFamilyStatistic<HistogramStatisticValue>> stats = reader.read();

    // should only have a single column family
    assertEquals("More than one column family has statistics!", 1, stats.size());
    List<HistogramStatisticValue> values = stats.get(0).getValues();
    assertEquals("More than one histogram in the column family/region", 1, values.size());
    Histogram histogram = values.get(0).getHistogram();
    assertEquals("Got an incorrect number of guideposts!", 26, histogram.getValueList().size());

    // make sure we got the correct guideposts
    byte counter = 'a';
    for (ByteString column : histogram.getValueList()) {
      byte[] guidepost = new byte[] { counter, 'z', 'z' };
      byte[] actual = column.toByteArray();
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
