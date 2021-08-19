package com.scalar.dataloader.common.service.exports;

import com.scalar.dataloader.common.dao.generic.GenericDao;
import com.scalar.dataloader.common.dao.generic.ScanBoundary;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.Result;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;

import java.util.*;

public class ExportServiceForStorage extends ExportService {

  private final DistributedStorage storage;

  public ExportServiceForStorage(
      GenericDao dao, DistributedStorage storage, DistributedStorageAdmin storageAdmin) {
    super(dao, storageAdmin);
    this.storage = storage;
  }

  @Override
  public List<Result> export(Export export) throws Exception {
    // Get table Metadata
    TableMetadata tableMetadata =
        this.getTableMetadata(export.getNamespace(), export.getTableName());

    // validate
    this.validateExport(export, tableMetadata);

    // Convert partition key filter to actual Key
    DataType columnDataType =
        tableMetadata.getColumnDataType(export.getScanPartitionKey().getColumn());
    Key partitionKey = export.getScanPartitionKey().toScalarDBKey(columnDataType);

    // convert scan start clustering key to scalardb.Key
    Optional<Key> scanStartKey = Optional.empty();
    if (export.getScanStartKeyFilter() != null) {
      columnDataType =
          tableMetadata.getColumnDataType(export.getScanStartKeyFilter().getColumn());
      scanStartKey =
          Optional.of(export.getScanStartKeyFilter().toScalarDBKey(columnDataType));
    }

    // convert scan end clustering key to scalardb.Key
    Optional<Key> scanEndKey = Optional.empty();
    if (export.getScanEndKeyFilter() != null) {
      columnDataType =
          tableMetadata.getColumnDataType(export.getScanEndKeyFilter().getColumn());
      scanEndKey =
          Optional.of(export.getScanEndKeyFilter().toScalarDBKey(columnDataType));
    }

    return this.scanExportData(export, partitionKey, scanStartKey, scanEndKey);
  }

  private List<Result> scanExportData(
          Export export, Key partitionKey, Optional<Key> scanStartKey, Optional<Key> scanEndKey)
          throws Exception {

    // Create scan boundary
    ScanBoundary scanBoundary = new ScanBoundary(scanStartKey, scanEndKey, export.getIsStartScanInclusive(), export.getIsEndScanInclusive());

    // scan and return
    return this.dao.scan(
            export.getNamespace(),
            export.getTableName(),
            partitionKey,
            scanBoundary,
            export.getSorts(),
            export.getProjections(),
            this.storage);
  }
}
