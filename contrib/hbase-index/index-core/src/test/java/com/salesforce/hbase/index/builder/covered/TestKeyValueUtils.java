package com.salesforce.hbase.index.builder.covered;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;

import org.junit.Test;

/**
 *
 */
public class TestKeyValueUtils {

  @Test
  public void testIndexKeyBuilding() {
    fail("not yet implemented");
  }

  @Test
  public void testAllNulls() {
    byte[] pk = new byte[] { 'a', 'b', 'z' };
    // check positive cases first
    byte[] result = CoveredColumnIndexCodec.composeRowKey(pk, 0, Arrays.asList(new byte[0]));
    assertTrue("Didn't correctly read single element as being null in row key",
      CoveredColumnIndexCodec.checkRowKeyForAllNulls(result));
    result = CoveredColumnIndexCodec.composeRowKey(pk, 0, Arrays.asList(new byte[0], new byte[0]));
    assertTrue("Didn't correctly read two elements as being null in row key",
      CoveredColumnIndexCodec.checkRowKeyForAllNulls(result));

    // check cases where it isn't null
    result = CoveredColumnIndexCodec.composeRowKey(pk, 2, Arrays.asList(new byte[] { 1, 2 }));
    assertFalse("Found a null key, when it wasn't!", CoveredColumnIndexCodec.checkRowKeyForAllNulls(result));
    result = CoveredColumnIndexCodec.composeRowKey(pk, 2, Arrays.asList(new byte[] { 1, 2 }, new byte[0]));
    assertFalse("Found a null key, when it wasn't!", CoveredColumnIndexCodec.checkRowKeyForAllNulls(result));
  }

}
