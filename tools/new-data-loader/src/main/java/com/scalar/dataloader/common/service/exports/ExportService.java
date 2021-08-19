package com.scalar.dataloader.common.service.exports;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.scalar.dataloader.common.dao.generic.GenericDao;
import com.scalar.dataloader.common.service.BaseService;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.Result;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Value;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

public abstract class ExportService extends BaseService {

  protected final GenericDao dao;

  public ExportService(GenericDao dao, DistributedStorageAdmin storageAdmin) {
    super(storageAdmin);
    this.dao = dao;
  }

  public abstract List<Result> export(Export export) throws Exception;

  public JsonArray exportToJSON(Export export) throws Exception {
    // Get data
    List<Result> results = this.export(export);

    // Get table metadata
    Map<String, DataType> columnDataTypes = getColumnDataTypes(export.getNamespace(), export.getTableName());

    // Serialize
    JsonArray rows = new JsonArray();
    for (Result result : results) {
      JsonObject row = new JsonObject();
      Map<String, Value<?>> values = result.getValues();
      for (String columnName : values.keySet()) {
        Value<?> columnValue = values.get(columnName);
        switch (columnDataTypes.get(columnName)) {
          case BOOLEAN:
            row.addProperty(columnName, columnValue.getAsBoolean());
            break;
          case INT:
            row.addProperty(columnName, columnValue.getAsInt());
            break;
          case BIGINT:
            row.addProperty(columnName, columnValue.getAsLong());
            break;
          case FLOAT:
            row.addProperty(columnName, columnValue.getAsFloat());
            break;
          case DOUBLE:
            row.addProperty(columnName, columnValue.getAsDouble());
            break;
          case TEXT:
            String textValue = columnValue.getAsString().isPresent() ? columnValue.getAsString().get() : "";
            row.addProperty(columnName, textValue);
            break;
          case BLOB:
            row.addProperty(columnName, columnValue.getAsBytes().toString());
            break;
        }
      }
      rows.add(row);
    }
    return rows;
  }

  protected void validateClusterKeySorts(LinkedHashSet<String> clusteringKeyNames, List<ScanOrdering> sorts) throws Exception {
    for (ScanOrdering sort : sorts) {
      // O(n) but list is always going to be very small so it's ok
      if (!clusteringKeyNames.contains(sort.getClusteringKey())) {
        throw new Exception(String.format("Sorting on clustering key '%s' is not possible, key not found", sort.getClusteringKey()));
      }
    }
  }

  protected void validateProjections(LinkedHashSet<String> columnNames, List<String> columns) throws Exception {
    if (columns == null || columns.isEmpty()) {
      return;
    }
    for (String column : columns) {
      // O(n) but list is always going to be very small so it's ok
      if (!columnNames.contains(column)) {
        throw  new Exception(String.format("The column '%s' was not found", column));
      }
    }
  }

  protected void validateExport(Export export, TableMetadata tableMetadata) throws Exception {
    // check if keys exists
    this.validateScalarDBKey(tableMetadata.getPartitionKeyNames(), export.getScanPartitionKey());

    // validate projections
    this.validateProjections(tableMetadata.getColumnNames(), export.getProjections());

    // validate sorts
    this.validateClusterKeySorts(tableMetadata.getClusteringKeyNames(), export.getSorts());
  }
}
