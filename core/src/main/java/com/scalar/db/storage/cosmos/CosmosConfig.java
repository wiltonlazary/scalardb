package com.scalar.db.storage.cosmos;

import com.google.common.base.Strings;
import com.scalar.db.config.DatabaseConfig;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import java.util.Properties;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

@Immutable
@SuppressFBWarnings("JCIP_FIELD_ISNT_FINAL_IN_IMMUTABLE_CLASS")
public class CosmosConfig extends DatabaseConfig {

  public static final String PREFIX = DatabaseConfig.PREFIX + "cosmos.";
  public static final String TABLE_METADATA_DATABASE = PREFIX + "table_metadata.database";

  @Nullable private String tableMetadataDatabase;

  public CosmosConfig(File propertiesFile) throws IOException {
    super(propertiesFile);
  }

  public CosmosConfig(InputStream stream) throws IOException {
    super(stream);
  }

  public CosmosConfig(Properties properties) {
    super(properties);
  }

  @Override
  protected void load() {
    String storage = getProperties().getProperty(DatabaseConfig.STORAGE);
    if (storage == null || !storage.equals("cosmos")) {
      throw new IllegalArgumentException(DatabaseConfig.STORAGE + " should be 'cosmos'");
    }

    super.load();

    if (!Strings.isNullOrEmpty(getProperties().getProperty(TABLE_METADATA_DATABASE))) {
      tableMetadataDatabase = getProperties().getProperty(TABLE_METADATA_DATABASE);
    }
  }

  public Optional<String> getTableMetadataDatabase() {
    return Optional.ofNullable(tableMetadataDatabase);
  }
}
