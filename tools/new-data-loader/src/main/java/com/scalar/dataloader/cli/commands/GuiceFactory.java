package com.scalar.dataloader.cli.commands;

import com.google.inject.AbstractModule;
import com.google.inject.ConfigurationException;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.scalar.dataloader.common.service.exports.ExportService;
import com.scalar.dataloader.common.service.imports.ImportService;
import picocli.CommandLine;
import picocli.CommandLine.IFactory;

public class GuiceFactory implements IFactory {

    private Injector injector;

    public GuiceFactory(ExportService exportService, ImportService importService) {
        injector = Guice.createInjector(new DemoModule(exportService, importService));
    }

    @Override
    public <K> K create(Class<K> aClass) throws Exception {
        try {
            return injector.getInstance(aClass);
        } catch (ConfigurationException ex) { // no implementation found in Guice configuration
            return CommandLine.defaultFactory().create(aClass); // fallback if missing
        }
    }

    static class DemoModule extends AbstractModule {

        private final ExportService exportService;
        private final ImportService importService;

        public DemoModule(ExportService exportService, ImportService importService) {
            this.exportService = exportService;
            this.importService = importService;
        }

        @Override
        protected void configure() {
            bind(ExportService.class).toInstance(exportService);
            bind(ImportService.class).toInstance(importService);
        }
    }
}
