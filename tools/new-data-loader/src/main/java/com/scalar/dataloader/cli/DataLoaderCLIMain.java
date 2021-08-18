package com.scalar.dataloader.cli;

import com.scalar.dataloader.cli.commands.ExportCommand;
import com.scalar.dataloader.cli.commands.GuiceFactory;
import com.scalar.dataloader.cli.commands.ImportCommand;
import com.scalar.dataloader.common.dao.LocallyConfiguredCassandraFactory;
import com.scalar.dataloader.common.dao.ScalarDbManager;
import com.scalar.dataloader.common.dao.generic.GenericDao;
import com.scalar.dataloader.common.service.exports.ExportService;
import com.scalar.dataloader.common.service.exports.ExportServiceForStorage;
import com.scalar.dataloader.common.service.exports.ExportServiceForTransaction;
import com.scalar.dataloader.common.service.imports.ImportService;
import com.scalar.dataloader.common.service.imports.ImportServiceForStorage;
import com.scalar.dataloader.common.service.imports.ImportServiceForTransaction;
import picocli.CommandLine;

import java.io.IOException;

import static com.scalar.dataloader.common.Constants.SCALARDB_PROPERTIES_FILE_NAME;
import static com.scalar.dataloader.common.Constants.SCALAR_DB_TRANSACTION_MODE;

@CommandLine.Command(
    description = "Scalar DB art demo CLI",
    mixinStandardHelpOptions = true,
    version = "1.0",
    subcommands = {ImportCommand.class, ExportCommand.class})
public class DataLoaderCLIMain {

  // The actual variable here is unused but the annotation is necessary for PICOCLI
  @CommandLine.Option(
      names = {"-m", "--mode"},
      description = "storage or transaction",
      defaultValue = "storage",
      scope = CommandLine.ScopeType.INHERIT)
  static String dbMode;

  // The actual variable here is unused but the annotation is necessary for PICOCLI
  @CommandLine.Option(
      names = {"-c", "--config"},
      paramLabel = "CONFIG",
      description = "path to the scalardb.properties file",
      scope = CommandLine.ScopeType.INHERIT)
  static String scalarPropertiesFilePath;

  public static void main(String[] args) throws IOException {
    DataLoaderCLIMain cli = new DataLoaderCLIMain();

    // Get `mode` and `scalarPropertiesFilePath` manually
    // The above annotation makes sure that PicoCLI adds the support for these arguments
    // However, at the time we need them, PICOCLI is not ready yet, so we need to get them manually
    String mode = "storage";
    String scalarPropertiesFilePath = SCALARDB_PROPERTIES_FILE_NAME;

    for (int i = 0; i < args.length; ++i) {
      if ("--mode".equals(args[i]) || "-m".equals(args[i])) {
        mode = args[i + 1];
      }
      if ("--config".equals(args[i]) || "-c".equals(args[i])) {
        scalarPropertiesFilePath = args[i + 1];
      }
    }

    ExportService exportService;
    ImportService importService;
    GenericDao dao = new GenericDao();
    ScalarDbManager scalarDbManager =
        new ScalarDbManager(new LocallyConfiguredCassandraFactory(scalarPropertiesFilePath));

    // Determine to start the services in Storage or Transaction mode
    if (mode.equals(SCALAR_DB_TRANSACTION_MODE)) {
      exportService =
          new ExportServiceForTransaction(dao, scalarDbManager.getDistributedTransactionManager());
      importService =
          new ImportServiceForTransaction(dao, scalarDbManager.getDistributedTransactionManager());
    } else {
      exportService = new ExportServiceForStorage(dao, scalarDbManager.getDistributedStorage());
      importService = new ImportServiceForStorage(dao, scalarDbManager.getDistributedStorage());
    }

    int exitCode =
        new CommandLine(cli, new GuiceFactory(exportService, importService)).execute(args);
    System.exit(exitCode);
  }
}
