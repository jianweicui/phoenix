package com.salesforce.hbase.stats;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.salesforce.hbase.protobuf.generated.StatisticProtos.HistogramColumn;
import com.salesforce.hbase.protobuf.generated.StatisticProtos.HistogramColumns;

/**
 * {@link StatisticValue} whose value is actually a histogram of data.
 * <p>
 * Two different types of histograms are supported - fixed width and fixed depth.
 * <ol>
 *  <li>Fixed Width - the width of the each column is <b>fixed</b>, but there is a variable number
 *  of elements in each 'bucket' in the histogram. This is the 'usual' histogram.
 *    <ul>
 *      <li>You should only use {@link #addColumn(byte[])} as the width of each column is 
 *      known</li>
 *    </ul>
 *  </li>
 *  <li>Fixed Depth - the width of the each column is <b>variable</b>, but there are a known number 
 *  of elements in each 'bucket' in the histogram. For instance, this can be used to determine
 *  every n'th key
 *    <ul>
 *      <li>You should only use {@link #addColumn(int, byte[])} as the depth of each column is
 *       variable</li>
 *    </ul>
 *  </li>
 * </ol>
 */
public class HistogramStatisticValue extends StatisticValue {

  private HistogramColumns.Builder builder = HistogramColumns.newBuilder();

  /**
   * Build a statistic value - should only be used by the
   * {@link com.salesforce.hbase.stats.serialization.HistogramStatisticReader}
   * .
   * @param value statistic instance to reference - no data is copied
   * @throws InvalidProtocolBufferException if the data in the {@link StatisticValue} is not a
   *           histogram
   */
  public HistogramStatisticValue(StatisticValue value) throws InvalidProtocolBufferException {
    super(value.name, value.info, value.value);
    // reset the builder based on the data
    builder = HistogramColumns.parseFrom(value.value).toBuilder();
  }

  /**
   * Build a fixed-depth histogram. All histogram columns should be added via
   * {@link #addColumn(int, byte[])} to ensure we know the width of each column.
   * @param name name of the statistic
   * @param info general info about the statistic
   */
  public HistogramStatisticValue(byte[] name, byte[] info) {
    super(name, info, null);
  }

  /**
   * Build a fixed-width histogram. All histogram columns should be added via
   * {@link #addColumn(byte[])} as we know the width of each column
   * @param name name of the statistic
   * @param info general info about the statistic
   * @param width width of all the columns
   */
  public HistogramStatisticValue(byte[] name, byte[] info, int width) {
    super(name, info, null);
    this.builder.setColumnWidth(width);
  }

  /**
   * Add a column to this histogram - should only be used with fixed width histograms, where the
   * width is specified in the constructor
   * @param value value of the next column - added to the end of the histogram
   */
  public void addColumn(byte[] value) {
    builder.addColumns(HistogramColumn.newBuilder().setValue(ByteString.copyFrom(value)).build());
  }

  /**
   * Add a column with a width to this histogram - should only be used with fixed depth histograms,
   * where the width is <b>not</b> specified in the constructor
   * @param width width of the column in the histogram
   * @param value value of the next column - added to the end of the histogram
   */
  public void addColumn(int width, byte[] value) {
    builder.addColumns(HistogramColumn.newBuilder().setValue(ByteString.copyFrom(value))
        .setWidth(width).build());
  }

  /**
   * Get the raw bytes for the histogram. Can be rebuilt using {@link #getHistogram(byte[])}
   */
  public byte[] getValue() {
    return builder.build().toByteArray();
  }

  public HistogramColumns getHistogram() {
    return this.builder.build();
  }

  public static HistogramColumns getHistogram(byte[] raw) throws InvalidProtocolBufferException {
    return HistogramColumns.parseFrom(raw);
  }
}