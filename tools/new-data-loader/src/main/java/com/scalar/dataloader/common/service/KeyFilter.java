package com.scalar.dataloader.common.service;

import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;

public class KeyFilter {

  private final String column;
  private final String value;

  public KeyFilter(String key, String value) {
    this.column = key;
    this.value = value;
  }

  public String getColumn() {
    return this.column;
  }

  public String getValue() {
    return this.value;
  }

  public Key toScalarDBKey(DataType dataType) {
    Key.Builder builder = Key.newBuilder();
    switch (dataType) {
      case BOOLEAN:
        return builder.addBoolean(this.column, Boolean.parseBoolean(this.value)).build();
      case INT:
        return builder.addInt(this.column, Integer.parseInt(this.value)).build();
      case BIGINT:
        return builder.addBigInt(this.column, Long.parseLong(this.value)).build();
      case FLOAT:
        return builder.addFloat(this.column, Float.parseFloat(this.value)).build();
      case DOUBLE:
        return builder.addDouble(this.column, Double.parseDouble(this.value)).build();
      case TEXT:
        return builder.addText(this.column, this.value).build();
      case BLOB:
        return builder.addBlob(this.column, this.value.getBytes()).build();
    }
    return null;
  }
}
