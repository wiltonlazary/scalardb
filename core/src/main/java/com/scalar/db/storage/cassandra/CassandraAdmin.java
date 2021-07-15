package com.scalar.db.storage.cassandra;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.StorageRuntimeException;
import java.util.Map;
import java.util.Optional;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class CassandraAdmin implements DistributedStorageAdmin {

  public static final String NETWORK_STRATEGY = "network-strategy";
  public static final String SIMPLE_STRATEGY = "SimpleStrategy";
  public static final String NETWORK_TOPOGOLY_STRATEGY = "NetworkTopologyStrategy";
  public static final String REPLICATION_FACTOR = "replication_factor";
  private final ClusterManager clusterManager;
  private final CassandraTableMetadataManager metadataManager;
  private final Optional<String> namespacePrefix;

  @Inject
  public CassandraAdmin(DatabaseConfig config, ClusterManager clusterManager) {
    this.clusterManager = clusterManager;
    this.clusterManager.getSession();
    metadataManager = new CassandraTableMetadataManager(clusterManager);
    namespacePrefix = config.getNamespacePrefix();
  }

  @VisibleForTesting
  CassandraAdmin(CassandraTableMetadataManager metadataManager, Optional<String> namespacePrefix) {
    clusterManager = null;
    this.metadataManager = metadataManager;
    this.namespacePrefix = namespacePrefix.map(n -> n + "_");
  }

  @Override
  public void createTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {
    // TODO Should the metatadaManager be populated as well
    createNamespace(namespace, options);
    createTableInternal(namespace, table, metadata);
    createSecondaryIndex(namespace, table, metadata);
    throw new UnsupportedOperationException("implement later");
  }

  @Override
  public void dropTable(String namespace, String table) throws ExecutionException {
    throw new UnsupportedOperationException("implement later");
  }

  @Override
  public void truncateTable(String namespace, String table) throws ExecutionException {
    throw new UnsupportedOperationException("implement later");
  }

  @Override
  public TableMetadata getTableMetadata(String namespace, String table) throws ExecutionException {
    try {
      return metadataManager.getTableMetadata(fullNamespace(namespace), table);
    } catch (StorageRuntimeException e) {
      throw new ExecutionException("getting a table metadata failed", e);
    }
  }

  private String fullNamespace(String namespace) {
    return namespacePrefix.map(s -> s + namespace).orElse(namespace);
  }

  @VisibleForTesting
  void createNamespace(String namespace, Map<String, String> options) throws ExecutionException {
    StringBuilder sb = new StringBuilder();
    sb.append(
        String.format(
            "CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = ", fullNamespace(namespace)));
    String replicationFactor = options.getOrDefault(REPLICATION_FACTOR, "1");
    String networkStrategy = options.getOrDefault(NETWORK_STRATEGY, SIMPLE_STRATEGY);
    if (networkStrategy.equals(SIMPLE_STRATEGY)) {
      sb.append(
          String.format(
              "{'class' : 'SimpleStrategy', 'replication_factor' : %s };", replicationFactor));
    } else if (networkStrategy.equals(NETWORK_TOPOGOLY_STRATEGY)) {
      sb.append(
          String.format(
              "{'class' : 'NetworkTopologyStrategy', 'dc1_name' : %s };", replicationFactor));
    } else {
      throw new IllegalArgumentException("The network strategy is unknown");
    }
    try {
      clusterManager.getSession().execute(sb.toString());
    } catch (RuntimeException e) {
      throw new ExecutionException(String.format("creating the %s namespace failed", namespace), e);
    }
  }

  @VisibleForTesting
  void createTableInternal(String namespace, String table, TableMetadata metadata)
      throws ExecutionException {
    StringBuilder sb = new StringBuilder();
    sb.append(
        String.format("CREATE TABLE [IF NOT EXISTS] %s.%s (", fullNamespace(namespace), table));
    boolean isCompoundKey =
        metadata.getPartitionKeyNames().size() + metadata.getClusteringKeyNames().size() > 1;
    for (String columnName : metadata.getColumnNames()) {
      sb.append(String.format("%s %s", columnName, metadata.getColumnDataType(columnName)));
      // If the key is not a compound key
      if (!isCompoundKey) {
        sb.append(" PRIMARY KEY");
      }
    }
    sb.append(",");
    // Add compound key statement
    if (isCompoundKey) {
      // TODO test composite primary key without partition key
      // TODO test single primary key
      // TODO test
      String primaryKey;
      if (metadata.getPartitionKeyNames().size() > 1) {
        primaryKey = String.format("(%s)", String.join(", ", metadata.getPartitionKeyNames()));
      } else {
        primaryKey = metadata.getPartitionKeyNames().stream().findFirst().get();
      }
      sb.append("PRIMARY KEY").append(primaryKey);
      String clusteringKeys = String.join(", ", metadata.getClusteringKeyNames());
      if (!clusteringKeys.isEmpty()) {
        sb.append(String.format(", %s)", clusteringKeys));
      }
    }
    sb.append(")");
    StringBuilder clusteringOrderStatement = new StringBuilder();
    for (String clusteringKeyName : metadata.getClusteringKeyNames()) {
      Order ckOrder = metadata.getClusteringOrder(clusteringKeyName);
      if (ckOrder != null) {
        clusteringOrderStatement.append(String.format("%s %s", clusteringKeyName, ckOrder));
      }
    }
    if (clusteringOrderStatement.length() > 1) {
      sb.append(String.format("WITH CLUSTERING ORDER BY (%s)", clusteringOrderStatement));
    }
    sb.append(";");
    try {
      clusterManager.getSession().execute(sb.toString());
    } catch (RuntimeException e) {
      throw new ExecutionException(
          String.format("creating the table %s.%s failed", namespace, table), e);
    }
  }

  @VisibleForTesting
  void createSecondaryIndex(String namespace, String table, TableMetadata metadata)
      throws ExecutionException {
    for (String index : metadata.getSecondaryIndexNames()) {
      String indexName =
          String.format("%s_%s_%s", table, DistributedStorageAdmin.INDEX_PREFIX, table);
      // CREATE INDEX sample_table_index_c2 ON sample_db.sample_table (c2);
      String indexCreationStatement =
          String.format("CREATE INDEX %s ON %s.%s (%s);", indexName, namespace, table, index);
      try {
        clusterManager.getSession().execute(indexCreationStatement);
      } catch (RuntimeException e) {
        throw new ExecutionException(
            String.format(
                "creating the secondary index for %s.%s.%s failed", namespace, table, index),
            e);
      }
    }
  }

  @Override
  public void close() {
    clusterManager.close();
  }
}
