package com.scalar.db.io;

public enum DataType {
  BOOLEAN,
  INT,
  BIGINT,
  FLOAT,
  DOUBLE,
  TEXT,
  BLOB;

  /**
   *
   * @return the equivalent {@link com.datastax.driver.core.DataType}
   */
  public com.datastax.driver.core.DataType toCassandraDataType() {
    switch (this) {
      case BOOLEAN:
        return com.datastax.driver.core.DataType.cboolean();
      case INT:
        return com.datastax.driver.core.DataType.cint();
      case BIGINT:
        return com.datastax.driver.core.DataType.bigint();
      case FLOAT:
        return com.datastax.driver.core.DataType.cfloat();
      case DOUBLE:
        return com.datastax.driver.core.DataType.cdouble();
      case TEXT:
        return com.datastax.driver.core.DataType.text();
      case BLOB:
        return com.datastax.driver.core.DataType.blob();
      default:
        throw new UnsupportedOperationException(String.format("%s is not yet implemented", this));
    }
  }
}
