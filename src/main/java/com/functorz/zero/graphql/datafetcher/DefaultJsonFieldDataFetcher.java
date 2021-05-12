package com.functorz.zero.graphql.datafetcher;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.functorz.zero.graphql.GraphQLConstants;
import graphql.TrivialDataFetcher;
import graphql.execution.ExecutionStepInfo;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLScalarType;
import graphql.schema.GraphQLType;
import io.leangen.graphql.util.GraphQLUtils;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class DefaultJsonFieldDataFetcher<T> implements DataFetcher<T>, TrivialDataFetcher<T> {
  private boolean isList;

  public static <T> DataFetcher<T> fetching(boolean isList) {
    return new DefaultJsonFieldDataFetcher<>(isList);
  }

  public static <T> DataFetcher<T> fetchingReturning(boolean isList) {
    return new DefaultJsonFieldDataFetcher<>(isList);
  }

  private static Object parseResult(JsonNode resultNode, GraphQLScalarType type) {
    if (resultNode == null || resultNode instanceof NullNode) {
      return null;
    }
    switch (type.getName()) {
      case GraphQLConstants.SCALAR_TYPE_LONG_NAME:
        return resultNode.asLong();
      case GraphQLConstants.SCALAR_TYPE_INTEGER_NAME:
        return resultNode.asInt();
      case GraphQLConstants.SCALAR_TYPE_FLOAT_NAME:
        return resultNode.asDouble();
      case GraphQLConstants.SCALAR_TYPE_BOOLEAN_NAME:
        return resultNode.asBoolean();
      case GraphQLConstants.SCALAR_TYPE_STRING_NAME:
        return resultNode.asText();
      case GraphQLConstants.SCALAR_TYPE_BIGDECIMAL_NAME:
        return new BigDecimal(resultNode.asText());
      case GraphQLConstants.SCALAR_TYPE_JSON_NAME:
        return resultNode;
      case GraphQLConstants.SCALAR_TYPE_OFFSETDATETIME_NAME:
        return OffsetDateTime.parse(resultNode.asText());
      case GraphQLConstants.SCALAR_TYPE_LOCALDATE_NAME:
        return LocalDate.parse(resultNode.asText());
      default:
        throw new IllegalStateException(String.format("unknown scalar type: %s", type.getName()));
    }
  }

  @Override
  public T get(DataFetchingEnvironment env) throws Exception {
    if (isList) {
      return getList(env);
    }
    ExecutionStepInfo stepInfo = env.getExecutionStepInfo();
    GraphQLFieldDefinition fieldDefinition = stepInfo.getFieldDefinition();
    GraphQLType wrappedType = GraphQLUtils.unwrap(fieldDefinition.getType());
    if (!(wrappedType instanceof GraphQLScalarType)) {
      throw new IllegalStateException("json field data fetcher only supports scalar type");
    }

    ObjectNode node = (ObjectNode) env.getSource();
    if (node == null) {
      return null;
    }

    String fieldName = fieldDefinition.getName();
    JsonNode resultNode = node.get(fieldName);
    return (T) parseResult(resultNode, (GraphQLScalarType) wrappedType);
  }

  private T getList(DataFetchingEnvironment env) {
    ObjectNode node = (ObjectNode) env.getSource();
    ArrayNode returning = (ArrayNode) node.get(GraphQLConstants.RETURNING);
    List<ObjectNode> result = new ArrayList<>();
    returning.forEach(jsonNode -> result.add((ObjectNode) jsonNode));
    return (T) result;
  }
}
