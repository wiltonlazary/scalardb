package command;

import com.scalar.db.config.DatabaseConfig;
import core.SchemaOperator;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.logging.Logger;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import utils.SchemaParser;

@Command(name = "--config", description = "Using config file for Scalar DB")
public class ConfigFileBasedCommand implements Callable<Integer> {

  @Parameters(index = "0", description = "Path to config file of Scalar DB")
  String configPath;

  @Option(
      names = {"-f", "--schema-file"},
      description = "Path to schema json file",
      required = true)
  String schemaFile;

  @Option(
      names = {"-D", "--delete"},
      description = "Delete tables")
  Boolean deleteTables;

  @Override
  public Integer call() throws Exception {

    Logger.getGlobal().info("Config path: " + configPath);
    Logger.getGlobal().info("Schema path: " + schemaFile);

    DatabaseConfig dbConfig = new DatabaseConfig(new FileInputStream(configPath));
    SchemaOperator operator = new SchemaOperator(dbConfig);
    SchemaParser schemaMap = new SchemaParser(schemaFile, new HashMap<String, String>());

    if (deleteTables) {
      operator.deleteTables(schemaMap.getTables());
    } else {
      operator.createTables(schemaMap.hasTransactionTable(), schemaMap.getTables());
    }
    return 0;
  }
}
