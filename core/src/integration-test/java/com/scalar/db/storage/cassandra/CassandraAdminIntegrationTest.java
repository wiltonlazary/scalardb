package com.scalar.db.storage.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.AdminIntegrationTestBase;
import java.util.Properties;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;

public class CassandraAdminIntegrationTest extends AdminIntegrationTestBase {
  private static final String INDEX1 = "test_index1";
  private static final String INDEX2 = "test_index2";
  private static final String CONTACT_POINT = "localhost";
  private static final String USERNAME = "cassandra";
  private static final String PASSWORD = "cassandra";
  private static final String CREATE_KEYSPACE_STMT =
      "CREATE KEYSPACE "
          + NAMESPACE
          + " WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }";
  private static final String CREATE_TABLE_STMT =
      "CREATE TABLE "
          + NAMESPACE
          + "."
          + GET_TABLE
          + " (c1 int, c2 text, c3 text, c4 int, c5 int, c6 text, c7 bigint, c8 float, c9 double,"
          + " c10 boolean, c11 blob, PRIMARY KEY((c2, c1), c4, c3))"
          + " WITH CLUSTERING ORDER BY (c4 ASC, c3 DESC)";
  private static final String CREATE_INDEX_STMT1 =
      "CREATE INDEX " + INDEX1 + " ON " + NAMESPACE + "." + GET_TABLE + " (c5)";
  private static final String CREATE_INDEX_STMT2 =
      "CREATE INDEX " + INDEX2 + " ON " + NAMESPACE + "." + GET_TABLE + " (c6)";
  private static final String DROP_KEYSPACE_STMT = "DROP KEYSPACE " + NAMESPACE;
  private static final String CREATE_TRUNCATE_TABLE_STMT =
      "CREATE TABLE " + NAMESPACE + "." + TRUNCATE_TABLE + " (c1 int PRIMARY KEY)";
  private static final String INSERT_DATA_TRUNCATE_TABLE_STMT =
      "INSERT INTO " + NAMESPACE + "." + TRUNCATE_TABLE + " (c1) VALUES (1)";
  private static final String SELECT_ALL_TRUNCATE_TABLE_STMT =
      "SELECT * from " + NAMESPACE + "." + TRUNCATE_TABLE;
  private static final String CREATE_DROP_TABLE_STMT =
      "CREATE TABLE " + NAMESPACE + "." + DROP_TABLE + " (c1 int PRIMARY KEY)";
  private static DistributedStorageAdmin admin;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    ProcessBuilder builder;
    Process process;
    int ret;

    builder =
        new ProcessBuilder("cqlsh", "-u", USERNAME, "-p", PASSWORD, "-e", CREATE_KEYSPACE_STMT);
    process = builder.start();
    ret = process.waitFor();
    if (ret != 0) {
      Assert.fail("CREATE KEYSPACE failed.");
    }

    builder = new ProcessBuilder("cqlsh", "-u", USERNAME, "-p", PASSWORD, "-e", CREATE_TABLE_STMT);
    process = builder.start();
    ret = process.waitFor();
    if (ret != 0) {
      Assert.fail("CREATE GET TABLE failed.");
    }

    builder = new ProcessBuilder("cqlsh", "-u", USERNAME, "-p", PASSWORD, "-e", CREATE_INDEX_STMT1);
    process = builder.start();
    ret = process.waitFor();
    if (ret != 0) {
      Assert.fail("CREATE INDEX failed.");
    }

    builder = new ProcessBuilder("cqlsh", "-u", USERNAME, "-p", PASSWORD, "-e", CREATE_INDEX_STMT2);
    process = builder.start();
    ret = process.waitFor();
    if (ret != 0) {
      Assert.fail("CREATE INDEX failed.");
    }

    builder =
        new ProcessBuilder(
            "cqlsh", "-u", USERNAME, "-p", PASSWORD, "-e", CREATE_TRUNCATE_TABLE_STMT);
    process = builder.start();
    ret = process.waitFor();
    if (ret != 0) {
      Assert.fail("CREATE TRUNCATE TABLE failed.");
    }

    builder =
        new ProcessBuilder(
            "cqlsh", "-u", USERNAME, "-p", PASSWORD, "-e", INSERT_DATA_TRUNCATE_TABLE_STMT);
    process = builder.start();
    ret = process.waitFor();
    if (ret != 0) {
      Assert.fail("INSERT TRUNCATE TABLE failed.");
    }

    builder =
        new ProcessBuilder(
            "cqlsh", "-u", USERNAME, "-p", PASSWORD, "-e", CREATE_DROP_TABLE_STMT);
    process = builder.start();
    ret = process.waitFor();
    if (ret != 0) {
      Assert.fail("CREATE TRUNCATE TABLE failed.");
    }

    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, CONTACT_POINT);
    props.setProperty(DatabaseConfig.USERNAME, USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, PASSWORD);
    DatabaseConfig config = new DatabaseConfig(props);
    ClusterManager clusterManager = new ClusterManager(config);
    admin = new CassandraAdmin(new DatabaseConfig(props), clusterManager);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    ProcessBuilder builder;
    Process process;
    int ret;

    builder = new ProcessBuilder("cqlsh", "-u", USERNAME, "-p", PASSWORD, "-e", DROP_KEYSPACE_STMT);
    process = builder.start();
    ret = process.waitFor();
    if (ret != 0) {
      Assert.fail("DROP KEYSPACE failed.");
    }

    admin.close();
  }

  @Before
  public void setUp() throws Exception {
    setUp(admin);
  }

  protected boolean isTruncateTestTableTruncated() {
    Cluster cluster =
        Cluster.builder()
            .withCredentials(USERNAME, PASSWORD)
            .addContactPoint("localhost")
            .withoutJMXReporting()
            .build();
    Session session = cluster.connect();
    ResultSet resultSet = session.execute(SELECT_ALL_TRUNCATE_TABLE_STMT);
    boolean isTruncated = resultSet.all().size() == 0;
    cluster.close();

    return isTruncated;
  }
}
