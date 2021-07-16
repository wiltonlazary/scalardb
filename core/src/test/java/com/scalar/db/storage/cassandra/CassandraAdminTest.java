package com.scalar.db.storage.cassandra;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.schemabuilder.KeyspaceOptions;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class CassandraAdminTest {

  private static final String SAMPLE_PREFIX = "sample_prefix_";
  @Mock CassandraTableMetadataManager metadataManager;
  @Mock ClusterManager clusterManager;
  @Mock DatabaseConfig config;
  @Mock Session cassandraSession;
  CassandraAdmin cassandraAdmin;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    when(config.getNamespacePrefix()).thenReturn(Optional.of(SAMPLE_PREFIX));
    when(clusterManager.getSession()).thenReturn(cassandraSession);
    cassandraAdmin = new CassandraAdmin(config, clusterManager);
  }

  @Test
  public void
      getTableMetadata_ConstructedWithoutNamespacePrefix_ShouldBeCalledWithoutNamespacePrefix()
          throws ExecutionException {
    // Arrange
    Optional<String> namespacePrefix = Optional.empty();
    String namespace = "ns";
    String table = "table";

    CassandraAdmin admin = new CassandraAdmin(metadataManager, namespacePrefix);

    // Act
    admin.getTableMetadata(namespace, table);

    // Assert
    verify(metadataManager).getTableMetadata(namespace, table);
  }

  @Test
  public void getTableMetadata_ConstructedWithNamespacePrefix_ShouldBeCalledWithNamespacePrefix()
      throws ExecutionException {
    // Arrange
    Optional<String> namespacePrefix = Optional.of("prefix");
    String namespace = "ns";
    String table = "table";

    CassandraAdmin admin = new CassandraAdmin(metadataManager, namespacePrefix);

    // Act
    admin.getTableMetadata(namespace, table);

    // Assert
    verify(metadataManager).getTableMetadata(namespacePrefix.get() + "_" + namespace, table);
  }

  @Test
  public void createNamespace_UnknowNetworkStrategyOption_ShouldThrowIllegalArgumentException() {
    // Arrange
    String namespace = "sample_ns";
    Map<String, String> options = new HashMap<>();
    options.put(CassandraAdmin.NETWORK_STRATEGY, "xxx_strategy");
    // Act
    // Assert
    Assertions.assertThatThrownBy(() -> cassandraAdmin.createNamespace(namespace, options))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void createNamespace_UseSimpleStrategy_ShouldExecuteCreateKeyspaceStatement()
      throws ExecutionException {
    // Arrange
    String namespace = "sample_ns";
    Map<String, String> options = new HashMap<>();
    options.put(
        CassandraAdmin.NETWORK_STRATEGY, CassandraNetworkStrategy.SIMPLE_STRATEGY.toString());
    options.put(CassandraAdmin.REPLICATION_FACTOR, "3");
    // Act
    cassandraAdmin.createNamespace(namespace, options);

    // Assert
    Map<String, Object> replicationOptions = new LinkedHashMap<>();
    replicationOptions.put("class", CassandraNetworkStrategy.SIMPLE_STRATEGY.toString());
    replicationOptions.put("replication_factor", "3");
    KeyspaceOptions query =
        SchemaBuilder.createKeyspace(SAMPLE_PREFIX + namespace)
            .ifNotExists()
            .with()
            .replication(replicationOptions);
    verify(cassandraSession).execute(query.getQueryString());
  }

  @Test
  public void createNamespace_UseNetworkTopologyStrategy_ShouldExecuteCreateKeyspaceStatement()
      throws ExecutionException {
    // Arrange
    String namespace = "sample_ns";
    Map<String, String> options = new HashMap<>();
    options.put(
        CassandraAdmin.NETWORK_STRATEGY,
        CassandraNetworkStrategy.NETWORK_TOPOLOGY_STRATEGY.toString());
    options.put(CassandraAdmin.REPLICATION_FACTOR, "5");
    // Act
    cassandraAdmin.createNamespace(namespace, options);

    // Assert
    Map<String, Object> replicationOptions = new LinkedHashMap<>();
    replicationOptions.put("class", CassandraNetworkStrategy.NETWORK_TOPOLOGY_STRATEGY.toString());
    replicationOptions.put("dc1_name", "5");
    KeyspaceOptions query =
        SchemaBuilder.createKeyspace(SAMPLE_PREFIX + namespace)
            .ifNotExists()
            .with()
            .replication(replicationOptions);
    verify(cassandraSession).execute(query.getQueryString());
  }

  @Test
  public void
      createNamespace_WithoutStrategyNorReplicationFactor_ShouldExecuteCreateKeyspaceStatement()
          throws ExecutionException {
    // Arrange
    String namespace = "sample_ns";
    Map<String, String> options = new HashMap<>();
    // Act
    cassandraAdmin.createNamespace(namespace, options);

    // Assert
    Map<String, Object> replicationOptions = new LinkedHashMap<>();
    replicationOptions.put("class", CassandraNetworkStrategy.SIMPLE_STRATEGY.toString());
    replicationOptions.put("replication_factor", "1");
    KeyspaceOptions query =
        SchemaBuilder.createKeyspace(SAMPLE_PREFIX + namespace)
            .ifNotExists()
            .with()
            .replication(replicationOptions);

    verify(cassandraSession).execute(query.getQueryString());
  }

  @Test
  public void createTableInternal_ShouldExecuteCreateTableStatement() throws ExecutionException {
    String namespace = "sample_ns";
    String table = "sample_table";
    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addPartitionKey("c1")
            .addClusteringKey("c4")
            .addColumn("c1", DataType.INT)
            .addColumn("c2", DataType.TEXT)
            .addColumn("c3", DataType.BLOB)
            .addColumn("c4", DataType.INT)
            .addColumn("c5", DataType.BOOLEAN)
            .addSecondaryIndex("c2")
            .addSecondaryIndex("c4")
            .build();
    // Assert
    //    cassandraAdmin.createTableInternal(namespace, table, tableMetadata);

    //
    //    verify(cassandraSession).execute("bar");
  }
}
