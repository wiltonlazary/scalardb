package com.scalar.db.storage;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import java.util.HashMap;
import java.util.Iterator;
import org.junit.Test;

public abstract class AdminIntegrationTestBase {

  protected static final String NAMESPACE = "integration_testing";
  protected static final String GET_TABLE = "get_table";
  protected static final String TRUNCATE_TABLE = "truncate_table_test";
  protected static final String DROP_TABLE = "drop_table_test";
  protected static final String COL_NAME1 = "c1";
  protected static final String COL_NAME2 = "c2";
  protected static final String COL_NAME3 = "c3";
  protected static final String COL_NAME4 = "c4";
  protected static final String COL_NAME5 = "c5";
  protected static final String COL_NAME6 = "c6";
  protected static final String COL_NAME7 = "c7";
  protected static final String COL_NAME8 = "c8";
  protected static final String COL_NAME9 = "c9";
  protected static final String COL_NAME10 = "c10";
  protected static final String COL_NAME11 = "c11";

  private DistributedStorageAdmin admin;

  public void setUp(DistributedStorageAdmin admin) throws Exception {
    this.admin = admin;
  }

  @Test
  public void getTableMetadata_CorrectTableGiven_MetadataShouldNotBeNull()
      throws ExecutionException {
    TableMetadata tableMetadata = admin.getTableMetadata(NAMESPACE, GET_TABLE);
    assertThat(tableMetadata).isNotNull();
  }

  @Test
  public void getTableMetadata_CorrectTableGiven_ShouldReturnCorrectPartitionKeyNames()
      throws ExecutionException {
    TableMetadata tableMetadata = admin.getTableMetadata(NAMESPACE, GET_TABLE);

    assertThat(tableMetadata.getPartitionKeyNames().size()).isEqualTo(2);
    Iterator<String> iterator = tableMetadata.getPartitionKeyNames().iterator();
    assertThat(iterator.next()).isEqualTo(COL_NAME2);
    assertThat(iterator.next()).isEqualTo(COL_NAME1);
  }

  @Test
  public void getTableMetadata_CorrectTableGiven_ShouldReturnCorrectClusteringKeyNames()
      throws ExecutionException {
    TableMetadata tableMetadata = admin.getTableMetadata(NAMESPACE, GET_TABLE);

    assertThat(tableMetadata.getClusteringKeyNames().size()).isEqualTo(2);
    Iterator<String> iterator = tableMetadata.getClusteringKeyNames().iterator();
    assertThat(iterator.next()).isEqualTo(COL_NAME4);
    assertThat(iterator.next()).isEqualTo(COL_NAME3);
  }

  @Test
  public void getTableMetadata_CorrectTableGiven_ShouldReturnCorrectColumnNames()
      throws ExecutionException {
    TableMetadata tableMetadata = admin.getTableMetadata(NAMESPACE, GET_TABLE);

    assertThat(tableMetadata.getColumnNames().size()).isEqualTo(11);
    assertThat(tableMetadata.getColumnNames().contains(COL_NAME1)).isTrue();
    assertThat(tableMetadata.getColumnNames().contains(COL_NAME2)).isTrue();
    assertThat(tableMetadata.getColumnNames().contains(COL_NAME3)).isTrue();
    assertThat(tableMetadata.getColumnNames().contains(COL_NAME4)).isTrue();
    assertThat(tableMetadata.getColumnNames().contains(COL_NAME5)).isTrue();
    assertThat(tableMetadata.getColumnNames().contains(COL_NAME6)).isTrue();
    assertThat(tableMetadata.getColumnNames().contains(COL_NAME7)).isTrue();
    assertThat(tableMetadata.getColumnNames().contains(COL_NAME8)).isTrue();
    assertThat(tableMetadata.getColumnNames().contains(COL_NAME9)).isTrue();
    assertThat(tableMetadata.getColumnNames().contains(COL_NAME10)).isTrue();
    assertThat(tableMetadata.getColumnNames().contains(COL_NAME11)).isTrue();
  }

  @Test
  public void getTableMetadata_CorrectTableGiven_ShouldReturnCorrectColumnDataTypes()
      throws ExecutionException {
    TableMetadata tableMetadata = admin.getTableMetadata(NAMESPACE, GET_TABLE);

    assertThat(tableMetadata.getColumnDataType(COL_NAME1)).isEqualTo(DataType.INT);
    assertThat(tableMetadata.getColumnDataType(COL_NAME2)).isEqualTo(DataType.TEXT);
    assertThat(tableMetadata.getColumnDataType(COL_NAME3)).isEqualTo(DataType.TEXT);
    assertThat(tableMetadata.getColumnDataType(COL_NAME4)).isEqualTo(DataType.INT);
    assertThat(tableMetadata.getColumnDataType(COL_NAME5)).isEqualTo(DataType.INT);
    assertThat(tableMetadata.getColumnDataType(COL_NAME6)).isEqualTo(DataType.TEXT);
    assertThat(tableMetadata.getColumnDataType(COL_NAME7)).isEqualTo(DataType.BIGINT);
    assertThat(tableMetadata.getColumnDataType(COL_NAME8)).isEqualTo(DataType.FLOAT);
    assertThat(tableMetadata.getColumnDataType(COL_NAME9)).isEqualTo(DataType.DOUBLE);
    assertThat(tableMetadata.getColumnDataType(COL_NAME10)).isEqualTo(DataType.BOOLEAN);
    assertThat(tableMetadata.getColumnDataType(COL_NAME11)).isEqualTo(DataType.BLOB);
  }

  @Test
  public void getTableMetadata_CorrectTableGiven_ShouldReturnCorrectClusteringOrders()
      throws ExecutionException {
    TableMetadata tableMetadata = admin.getTableMetadata(NAMESPACE, GET_TABLE);

    assertThat(tableMetadata.getClusteringOrder(COL_NAME1)).isNull();
    assertThat(tableMetadata.getClusteringOrder(COL_NAME2)).isNull();
    assertThat(tableMetadata.getClusteringOrder(COL_NAME3)).isEqualTo(Scan.Ordering.Order.DESC);
    assertThat(tableMetadata.getClusteringOrder(COL_NAME4)).isEqualTo(Scan.Ordering.Order.ASC);
    assertThat(tableMetadata.getClusteringOrder(COL_NAME5)).isNull();
    assertThat(tableMetadata.getClusteringOrder(COL_NAME6)).isNull();
    assertThat(tableMetadata.getClusteringOrder(COL_NAME7)).isNull();
    assertThat(tableMetadata.getClusteringOrder(COL_NAME8)).isNull();
    assertThat(tableMetadata.getClusteringOrder(COL_NAME9)).isNull();
    assertThat(tableMetadata.getClusteringOrder(COL_NAME10)).isNull();
    assertThat(tableMetadata.getClusteringOrder(COL_NAME11)).isNull();
  }

  @Test
  public void getTableMetadata_CorrectTableGiven_ShouldReturnCorrectSecondaryIndexNames()
      throws ExecutionException {
    TableMetadata tableMetadata = admin.getTableMetadata(NAMESPACE, GET_TABLE);

    assertThat(tableMetadata.getSecondaryIndexNames().size()).isEqualTo(2);
    assertThat(tableMetadata.getSecondaryIndexNames().contains(COL_NAME5)).isTrue();
    assertThat(tableMetadata.getSecondaryIndexNames().contains(COL_NAME6)).isTrue();
  }

  @Test
  public void getTableMetadata_WrongTableGiven_ShouldReturnNull() throws ExecutionException {
    TableMetadata tableMetadata = admin.getTableMetadata("wrong_ns", "wrong_table");
    assertThat(tableMetadata).isNull();
  }

  @Test
  public void createTable_CorrectTableGiven_ShouldBeCreated() throws ExecutionException {
    // Arrange
    String table = "create_table_test";
    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addPartitionKey("c1")
            .addPartitionKey("c7")
            .addClusteringKey("c4")
            .addClusteringKey("c6", Order.DESC)
            .addColumn("c1", DataType.INT)
            .addColumn("c2", DataType.TEXT)
            .addColumn("c3", DataType.BLOB)
            .addColumn("c4", DataType.DOUBLE)
            .addColumn("c5", DataType.BIGINT)
            .addColumn("c6", DataType.BOOLEAN)
            .addColumn("c7", DataType.TEXT)
            .addSecondaryIndex("c2")
            .addSecondaryIndex("c4")
            .build();

    // Act
    admin.createTable(NAMESPACE, table, tableMetadata, new HashMap<>());

    // Assert
    TableMetadata metadata = admin.getTableMetadata(NAMESPACE, table);
    assertThat(metadata.getPartitionKeyNames()).containsExactly("c1", "c7");
    assertThat(metadata.getClusteringKeyNames()).containsExactly("c4", "c6");
    assertThat(metadata.getClusteringOrder("c4")).isEqualTo(Order.ASC);
    assertThat(metadata.getClusteringOrder("c6")).isEqualTo(Order.DESC);
    assertThat(metadata.getColumnNames())
        .containsExactlyInAnyOrder("c1", "c2", "c3", "c4", "c5", "c6", "c7");
    assertThat(metadata.getColumnDataType("c1")).isEqualTo(DataType.INT);
    assertThat(metadata.getColumnDataType("c2")).isEqualTo(DataType.TEXT);
    assertThat(metadata.getColumnDataType("c3")).isEqualTo(DataType.BLOB);
    assertThat(metadata.getColumnDataType("c4")).isEqualTo(DataType.DOUBLE);
    assertThat(metadata.getColumnDataType("c5")).isEqualTo(DataType.BIGINT);
    assertThat(metadata.getColumnDataType("c6")).isEqualTo(DataType.BOOLEAN);
    assertThat(metadata.getColumnDataType("c7")).isEqualTo(DataType.TEXT);
    assertThat(metadata.getSecondaryIndexNames()).containsExactlyInAnyOrder("c2", "c4");
  }

  @Test
  public void truncateTable_correctTableGiven_ShouldTruncateTable() throws ExecutionException {
    // Act
    admin.truncateTable(NAMESPACE, TRUNCATE_TABLE);

    // Assert
    assertThat(isTruncateTestTableTruncated()).isTrue();
  }

  @Test
  public void dropTable_correctTableGiven_ShouldDropTable() throws ExecutionException {
    // Act
    admin.dropTable(NAMESPACE, DROP_TABLE);
    // Assert
    TableMetadata metadata = admin.getTableMetadata(NAMESPACE, DROP_TABLE);
    assertThat(metadata).isNull();
  }

  protected abstract boolean isTruncateTestTableTruncated();
}
