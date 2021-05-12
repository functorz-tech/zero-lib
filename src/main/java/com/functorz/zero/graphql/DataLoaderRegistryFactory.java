package com.functorz.zero.graphql;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.dataloader.BatchLoader;
import org.dataloader.DataLoader;
import org.dataloader.DataLoaderOptions;
import org.dataloader.DataLoaderRegistry;

public class DataLoaderRegistryFactory {
  private Map<String, BatchLoader<?, ?>> batchLoaders = new ConcurrentHashMap<>();

  public DataLoaderRegistry buildCachelessRegistry() {
    return buildRegistry(DataLoaderOptions.newOptions().setCachingEnabled(false));
  }

  public void addDataLoader(String name, BatchLoader loader) {
    batchLoaders.put(name, loader);
  }

  public DataLoaderRegistry buildRegistry() {
    return buildRegistry(DataLoaderOptions.newOptions().setCachingEnabled(true));
  }

  public DataLoaderRegistry buildRegistry(DataLoaderOptions options) {
    DataLoaderRegistry registry = new DataLoaderRegistry();
    if (batchLoaders != null) {
      batchLoaders.entrySet().stream().forEach(entry -> {
        registry.register(entry.getKey(), new DataLoader<>(entry.getValue(), options));
      });
    }
    return registry;
  }
}

