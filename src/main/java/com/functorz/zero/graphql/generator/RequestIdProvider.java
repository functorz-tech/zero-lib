package com.functorz.zero.graphql.generator;

import graphql.execution.ExecutionId;
import graphql.execution.ExecutionIdProvider;

public class RequestIdProvider implements ExecutionIdProvider {

  @Override
  public ExecutionId provide(String query, String operationName, Object context) {
    return ExecutionId.generate();
  }
  
}
