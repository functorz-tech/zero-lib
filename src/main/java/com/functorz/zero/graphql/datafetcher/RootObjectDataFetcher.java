package com.functorz.zero.graphql.datafetcher;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;

public class RootObjectDataFetcher implements DataFetcher<CompletableFuture<ObjectNode>> {

  private ObjectMapper objectMapper;

  public RootObjectDataFetcher(ObjectMapper objectMapper) {
    this.objectMapper = objectMapper;
  }

  @Override
  public CompletableFuture<ObjectNode> get(DataFetchingEnvironment environment) {
    return CompletableFuture.supplyAsync(() -> {
      ObjectNode objectNode = objectMapper.createObjectNode();
      return objectNode;
    });
  }
}
