package com.salesforce.hbase.index.builder.covered;

import java.util.Arrays;

import javax.annotation.Nullable;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Predicate;

/**
 * A single Column (either a Column Family or a full Family:Qualifier pair) in a {@link ColumnGroup}
 * . If no column qualifier is specified, matches all known qualifiers of the family.
 */
public class CoveredColumn {

  public static final String SEPARATOR = ":";
  String family;
  byte[] qualifier;

  public CoveredColumn(String family, byte[] qualifier) {
    this.family = family;
    this.qualifier = qualifier;
  }

  public static CoveredColumn parse(String spec) {
    int sep = spec.indexOf(SEPARATOR);
    if (sep < 0) {
      throw new IllegalArgumentException(spec + " is not a valid specifier!");
    }
    String family = spec.substring(0, sep);
    String qual = spec.substring(sep + 1);
    byte[] column = qual.length() == 0 ? null : Bytes.toBytes(qual);
    return new CoveredColumn(family, column);
  }

  public String serialize() {
    return CoveredColumn.serialize(family, qualifier);
  }

  /**
   * @param first
   * @param second
   * @return
   */
  public static String serialize(String first, byte[] second) {
    String nextValue = first + CoveredColumn.SEPARATOR;
    if (second != null) {
      nextValue += Bytes.toString(second);
    }
    return nextValue;
  }

  /**
   * @param family2
   * @return
   */
  public boolean matchesFamily(String family2) {
    return this.family.equals(family2);
  }


  public Predicate<KeyValue> getColumnQualifierPredicate() {
    return new Predicate<KeyValue>() {
      @Override
      public boolean apply(@Nullable KeyValue input) {
        if (input == null) {
          return false;
        }
        return CoveredColumn.this.matchesQualifier(input.getQualifier());
      }
    };
  }

  public Predicate<KeyValue> getColumnFamilyPredicate() {
    return new Predicate<KeyValue>() {
      @Override
      public boolean apply(@Nullable KeyValue input) {
        if (input == null) {
          return false;
        }
        // filter down kvs based on the family
        byte[] qual = input.getFamily();
        return Arrays.equals(qual, Bytes.toBytes(family));
      }
    };
  }

  /**
   * @return <tt>true</tt> if this should include all column qualifiers, <tt>false</tt> otherwise
   */
  public boolean allColumns() {
    return this.qualifier == null;
  }

  /**
   * @param qualifier2
   */
  public boolean matchesQualifier(byte[] qual) {
    // empty qualifier matches all
    return qualifier == null || Arrays.equals(qual, qualifier);
  }

  @Override
  public boolean equals(Object o) {
    CoveredColumn other = (CoveredColumn) o;
    if (this.family.equals(other.family)) {
      return Bytes.equals(qualifier, other.qualifier);
    }
    return false;
  }

  @Override
  public int hashCode() {
    int hash = this.family.hashCode();
    if (this.qualifier != null) {
      hash += Bytes.hashCode(qualifier);
    }

    return hash;
  }

  @Override
  public String toString() {
    String qualString = qualifier == null ? "null" : Bytes.toString(qualifier);
    return "CoveredColumn:[" + family + ":" + qualString + "]";
  }
}