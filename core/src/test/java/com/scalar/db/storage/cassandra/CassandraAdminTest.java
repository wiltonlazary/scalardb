package com.scalar.db.storage.cassandra;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.Session;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class CassandraAdminTest {

  @Mock CassandraTableMetadataManager metadataManager;
  @Mock ClusterManager clusterManager;
  @Mock DatabaseConfig config;
  @Mock Session cassandraSession;
  CassandraAdmin cassandraAdmin;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    when(config.getNamespacePrefix()).thenReturn(Optional.of("sample_prefix_"));
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
  public void createNamespace_UseSimpleStrategy_ShouldExecuteCreateStatement()
      throws ExecutionException {
    // Arrange
    String namespace = "sample_ns";
    Map<String, String> options = new HashMap<>();
    options.put(CassandraAdmin.NETWORK_STRATEGY, CassandraAdmin.SIMPLE_STRATEGY);
    options.put(CassandraAdmin.REPLICATION_FACTOR, "3");
    // Act
    cassandraAdmin.createNamespace(namespace, options);

    // Assert
    verify(cassandraSession)
        .execute(
            "CREATE KEYSPACE IF NOT EXISTS sample_prefix_sample_ns WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 3 };");
  }

  @Test
  public void createNamespace_UseNetworkTopologyStrategy_ShouldExecuteCreateStatement()
      throws ExecutionException {
    // Arrange
    String namespace = "sample_ns";
    Map<String, String> options = new HashMap<>();
    options.put(CassandraAdmin.NETWORK_STRATEGY, CassandraAdmin.NETWORK_TOPOGOLY_STRATEGY);
    options.put(CassandraAdmin.REPLICATION_FACTOR, "5");
    // Act
    cassandraAdmin.createNamespace(namespace, options);

    // Assert
    verify(cassandraSession)
        .execute(
            "CREATE KEYSPACE IF NOT EXISTS sample_prefix_sample_ns WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'dc1_name' : 5 };");
  }

  @Test
  public void createNamespace_WithoutStrategyNorReplicationFactor_ShouldExecuteCreateStatement()
      throws ExecutionException {
    // Arrange
    String namespace = "sample_ns";
    Map<String, String> options = new HashMap<>();
    // Act
    cassandraAdmin.createNamespace(namespace, options);

    // Assert
    verify(cassandraSession)
        .execute(
            "CREATE KEYSPACE IF NOT EXISTS sample_prefix_sample_ns WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
  }


}
