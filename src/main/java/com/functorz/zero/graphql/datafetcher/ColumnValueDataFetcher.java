package com.functorz.zero.graphql.datafetcher;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.functorz.zero.datamodel.ColumnType;
import graphql.execution.ExecutionStepInfo;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.GraphQLFieldDefinition;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class ColumnValueDataFetcher<T> implements DataFetcher<T> {

  private ColumnType columnType;

  @Override
  public T get(DataFetchingEnvironment environment) throws Exception {
    ExecutionStepInfo stepInfo = environment.getExecutionStepInfo();
    ObjectNode node = (ObjectNode) environment.getSource();
    GraphQLFieldDefinition fieldDefinition = stepInfo.getFieldDefinition();
    String fieldName = fieldDefinition.getName();
    JsonNode resultNode = node.get(fieldName);
    return (T) parseResult(resultNode);
  }

  private Object parseResult(JsonNode result) {
    if (result == null || result instanceof NullNode) {
      return null;
    }
    switch (columnType) {
      case BIGINT:
      case BIGSERIAL:
        return result.asLong();
      case INTEGER:
        return result.asInt();
      case FLOAT8:
        return result.asDouble();
      case DECIMAL:
        return result;
      case TEXT:
        return result.asText();
      case BOOLEAN:
        return result.asBoolean();
      case TIMESTAMPTZ:
      case TIMETZ:
        return OffsetDateTime.parse(result.asText());
      case DATE:
        return LocalDate.parse(result.asText());
      case JSONB:
        return result;
      default:
        throw new IllegalStateException();
    }
  }
}
