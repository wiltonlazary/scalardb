package com.scalar.dataloader.common.service.exports;

import com.scalar.dataloader.common.dao.generic.GenericDao;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;

import java.util.Optional;

public class ExportServiceForStorage extends ExportService {

  private final DistributedStorage storage;

  public ExportServiceForStorage(
      GenericDao dao, DistributedStorage storage, DistributedStorageAdmin storageAdmin) {
    super(dao, storageAdmin);
    this.storage = storage;
  }

  @Override
  public void export(Export export) throws Exception {
    // Get table Metadata
    TableMetadata tableMetadata =
        this.getTableMetadata(export.getNamespace(), export.getTableName());

    // check if keys exists
    this.validateScalarDBKey(tableMetadata.getPartitionKeyNames(), export.getScanPartitionKey());

    // Convert partition key filter to actual Key
    DataType columnDataType =
        tableMetadata.getColumnDataType(export.getScanPartitionKey().getColumn());
    Key partitionKey = export.getScanPartitionKey().toScalarDBKey(columnDataType);

    // validate sorts
    this.validateSorts(tableMetadata, export.getSorts());

    // convert sort keys to
    if (!export.getSorts().isEmpty()) {

    }

    // scan and get the data
    this.dao.scan(
        export.getNamespace(),
        export.getTableName(),
        partitionKey,
        Optional.empty(),
        Optional.empty(),
        export.getSorts(),
        this.storage);

    // export to the actual file
  }
}
