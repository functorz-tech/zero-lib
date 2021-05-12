package com.functorz.zero.graphql;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.functorz.zero.datamodel.ColumnType;
import com.functorz.zero.datamodel.DataModel;
import com.functorz.zero.datamodel.RelationMetadata;
import com.functorz.zero.datamodel.TableMetadata;
import com.functorz.zero.graphql.datafetcher.RootObjectDataFetcher;
import com.functorz.zero.graphql.dataloader.DataLoaderKeyWrapper;
import com.functorz.zero.graphql.generator.AggregateType;
import com.functorz.zero.graphql.generator.TableConfig;
import com.functorz.zero.graphql.subscription.GraphQLSubscriptionEventObserver;
import com.functorz.zero.graphql.subscription.InMemorySub;
import com.functorz.zero.graphql.subscription.PostgresEventObserver;
import com.functorz.zero.graphql.subscription.PostgresMutationEvent;
import com.functorz.zero.utils.DataModelUtils;
import com.functorz.zero.utils.GraphQLUtils;
import com.functorz.zero.utils.ResultSetUtils;
import com.functorz.zero.utils.Utils;
import com.google.common.collect.Sets;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.SelectedField;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.dataloader.DataLoader;
import org.jooq.Field;
import org.jooq.impl.DSL;
import org.reactivestreams.Publisher;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.StatementCallback;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.CollectionUtils;

public class DataFetcherGenerator {
  private JdbcTemplate jdbcTemplate;
  private ObjectMapper objectMapper;
  private TransactionTemplate transactionTemplate;
  private Map<String, TableConfig> tableConfigMap;
  private PostgresEventObserver postgresEventObserver;

  public DataFetcherGenerator(DataModel dataModel, ObjectMapper objectMapper, JdbcTemplate jdbcTemplate,
                              PlatformTransactionManager transactionManager, GraphQLSubscriptionEventObserver eventObserver) {
    this.tableConfigMap = DataModelUtils.generateTableConfigMap(dataModel);
    this.jdbcTemplate = jdbcTemplate;
    this.objectMapper = objectMapper;
    this.transactionTemplate = new TransactionTemplate(transactionManager);
    this.postgresEventObserver = new InMemorySub(eventObserver);
  }

  public DataFetcher<CompletableFuture<List<ObjectNode>>> generateQueryListDataFetcher(TableConfig tableConfig) {
    return env -> {
      return CompletableFuture.supplyAsync(() -> query(tableConfig, env));
    };
  }

  public DataFetcher<CompletableFuture<ObjectNode>> generateQueryByPkDataFetcher(TableConfig tableConfig) {
    return env -> {
      return CompletableFuture.supplyAsync(() -> queryByPk(tableConfig, env));
    };
  }

  public DataFetcher<Publisher<Object>> generateSubscriptionQueryDataFetcher(TableConfig tableConfig, boolean isList) {
    return env -> {
      Set<String> tableSet = parseRelatedTableSetFromSelectionSet(env, tableConfig);
      String subscriptionKey = String.format("sub_%s", env.getExecutionId().toString());
      return postgresEventObserver.on(subscriptionKey, tableSet)
        .distinct(event -> {
          if (event instanceof PostgresMutationEvent) {
            return ((PostgresMutationEvent) event).getTableName();
          } else {
            return event;
          }
        }).sample(Duration.ofSeconds(1))
        .map(event -> isList ? query(tableConfig, env) : queryByPk(tableConfig, env));
    };
  }

  public DataFetcher<Publisher<Object>> generateSubscriptionAggregateDataFetcher(TableConfig tableConfig) {
    return env -> {
      Set<String> tableSet = Set.of(tableConfig.getTableMetadata().getName());
      String subscriptionKey = String.format("sub_%s", env.getExecutionId().toString());
      return postgresEventObserver.on(subscriptionKey, tableSet)
        .distinct(event -> {
          if (event instanceof PostgresMutationEvent) {
            return ((PostgresMutationEvent) event).getTableName();
          } else {
            return event;
          }
        }).sample(Duration.ofSeconds(1))
        .map(event -> new RootObjectDataFetcher(objectMapper));
    };
  }

  public DataFetcher<CompletableFuture<?>> generateAggregateFieldDataFetcher(
      TableMetadata tableMetadata, ColumnType columnType, String columnName, AggregateType type) {
    return env -> CompletableFuture.supplyAsync(() -> {
      ResultSetExtractor<List<ObjectNode>> rsExtractor = ResultSetUtils::mapResultSetToListJson;
      String aggregateSql = DataFetcherSqlGenerator.generateAggregateQuerySql(tableMetadata, columnType, columnName, env, type);
      List<ObjectNode> result = jdbcTemplate.query(aggregateSql, rsExtractor);
      if (CollectionUtils.isEmpty(result)) {
        throw new IllegalStateException("aggregate query cannot be empty");
      }
      if (result.size() > 1) {
        throw new IllegalStateException(String.format("unexpected state, sql: %s, result: %s",
          aggregateSql, Utils.writeObjectToStringUnchecked(result)));
      }
      return mappingResult(columnType, type, result.get(0));
    });
  }

  public DataFetcher<CompletableFuture<?>> generateRelationFieldDataFetcher(TableMetadata tableMetadata,
                                                                            RelationMetadata relationMetadata) {
    return env -> {
      String dataLoaderName = relationMetadata.getId().toString();
      DataLoader<Object, ?> dataLoader = env.getDataLoader(dataLoaderName);
      if (dataLoader == null) {
        throw new IllegalStateException(String.format("get dataLoader failed, name:%s", dataLoaderName));
      }
      List<SelectedField> selectedFields = env.getSelectionSet().getFields();
      Map<String, Object> arguments = env.getArguments();
      ObjectNode parentNode = (ObjectNode) env.getSource();
      String queryColumnName = DataModelUtils.getQueryColumnName(tableMetadata, relationMetadata);
      Object key = Utils.parseScalarNodeValue(parentNode.get(queryColumnName));
      if (key == null) {
        throw new IllegalStateException("key should never be null");
      }
      return dataLoader.load(new DataLoaderKeyWrapper(key, selectedFields, arguments));
    };
  }

  public DataFetcher<?> generateUpdateMutationFieldDataFetcher(TableConfig tableConfig) {
    return env -> {
      ResultSetExtractor<ArrayNode> rsExtractor = ResultSetUtils::mapResultSetToArrayNode;
      Map<String, Object> arguments = env.getArguments();
      if (!arguments.containsKey(GraphQLConstants._SET)
          && !arguments.containsKey(GraphQLConstants._INC)) {
        return null;
      }
      String updateByPkSql = DataFetcherSqlGenerator.generateUpdateMutationSql(tableConfig.getTableMetadata(), env);
      StatementCallback<ArrayNode> statementCallback = ResultSetUtils
          .generateStatementCallback(updateByPkSql, rsExtractor);
      ArrayNode updateReturning = jdbcTemplate.execute(statementCallback);
      return mapReturningResultToMutationResponse(updateReturning);
    };
  }

  public DataFetcher<?> generateUpdateByPkMutationFieldDataFetcher(TableConfig tableConfig) {
    return env -> {
      ResultSetExtractor<ObjectNode> rsExtractor = ResultSetUtils::mapResultSetToObjectNode;
      Map<String, Object> arguments = env.getArguments();
      if (!arguments.containsKey(GraphQLConstants._SET)
          && !arguments.containsKey(GraphQLConstants._INC)) {
        throw new IllegalArgumentException("at least any one of _set, _inc is expected");
      }
      String updateByPkSql = DataFetcherSqlGenerator.generateUpdateByPkMutationSql(tableConfig.getTableMetadata(), env);
      StatementCallback<ObjectNode> statementCallback = ResultSetUtils
        .generateStatementCallback(updateByPkSql, rsExtractor);
      return jdbcTemplate.execute(statementCallback);
    };
  }

  public DataFetcher<?> generateInsertMutationFieldDataFetcher(TableConfig tableConfig) {
    return env -> {
      return transactionTemplate.execute(transactionStatus -> {
        Set<String> columnSet = DataModelUtils.generateColumnSet(tableConfig.getTableMetadata());
        Set<String> selectionSet = GraphQLUtils.getMutationReturningSelectionSet(env);
        Set<String> mutationSelectionSet = GraphQLUtils.getMutationSelectionSet(env);

        Set<String> selectColumns = selectionSet.stream()
          .filter(columnSet::contains).collect(Collectors.toSet());
        selectColumns.add(GraphQLConstants.ID);
        Set<Field<Object>> selectFields = mutationSelectionSet.contains(GraphQLConstants.RETURNING)
          ? Utils.mapColumnSetToFieldSet(selectColumns)
          : Set.of(DSL.field("1"));
        Map<String, Object> arguments = env.getArguments();
        List<Object> objects = (List<Object>) arguments.get(GraphQLConstants.OBJECTS);
        Map<String, Object> onConflict = (Map<String, Object>) arguments.get(GraphQLConstants.ON_CONFLICT);
        ResultSetExtractor<ObjectNode> rsExtractor = ResultSetUtils::mapResultSetToObjectNode;
        return objects.stream().map(object -> {
          Map<String, Object> objectInput = (Map<String, Object>) object;
          String insertSql = DataFetcherSqlGenerator.generateInsertOneMutationSql(tableConfig.getTableMetadata().getName(),
            columnSet, selectFields, objectInput, onConflict);
          StatementCallback<ObjectNode> callback = ResultSetUtils
            .generateStatementCallback(insertSql, rsExtractor);
          ObjectNode objectResult = jdbcTemplate.execute(callback);
          Map<RelationTarget, Object> relationObjectInsertInput = findRelationObjectInsertInput(tableConfig, objectInput);
          Long objectId = objectResult.get(GraphQLConstants.ID).asLong();
          executeRelationObjectInsert(relationObjectInsertInput, objectId);
          return objectResult;
        }).collect(Collectors.toList());
      });
    };
  }

  public DataFetcher<ObjectNode> generateInsertOneMutationFieldDataFetcher(TableConfig tableConfig) {
    return env -> {
      return transactionTemplate.execute(transactionStatus -> {
        Set<String> columnSet = DataModelUtils.generateColumnSet(tableConfig.getTableMetadata());
        Set<String> selectColumns = GraphQLUtils.getMutationSelectionSet(env);
        selectColumns.add(GraphQLConstants.ID);
        Map<String, Object> arguments = env.getArguments();
        Map<String, Object> objectInput = (Map<String, Object>) arguments.get(GraphQLConstants.OBJECT);
        Map<String, Object> onConflict = (Map<String, Object>) arguments.get(GraphQLConstants.ON_CONFLICT);
        Set<Field<Object>> selectFields = Utils.mapColumnSetToFieldSet(selectColumns);
        String insertSql = DataFetcherSqlGenerator.generateInsertOneMutationSql(
          tableConfig.getTableMetadata().getName(), columnSet, selectFields, objectInput, onConflict);
        ResultSetExtractor<ObjectNode> rsExtractor = ResultSetUtils::mapResultSetToObjectNode;
        StatementCallback<ObjectNode> callback = ResultSetUtils.generateStatementCallback(insertSql, rsExtractor);
        ObjectNode object = jdbcTemplate.execute(callback);
        if (object == null) {
          throw new IllegalStateException("insert object failed");
        }
        Map<RelationTarget, Object> relationObjectInsertInput = findRelationObjectInsertInput(tableConfig, objectInput);
        Long objectId = object.get(GraphQLConstants.ID).asLong();
        executeRelationObjectInsert(relationObjectInsertInput, objectId);
        return object;
      });
    };
  }

  public DataFetcher<?> generateDeleteMutationFieldDataFetcher(TableMetadata tableMetadata) {
    return env -> {
      return transactionTemplate.execute(transactionStatus -> {
        String deleteSql = DataFetcherSqlGenerator.generateDeleteMutationSql(tableMetadata, env);
        ResultSetExtractor<ArrayNode> rsExtractor = ResultSetUtils::mapResultSetToArrayNode;
        ArrayNode deleteReturning = jdbcTemplate.query(deleteSql, rsExtractor);
        return mapReturningResultToMutationResponse(deleteReturning);
      });
    };
  }

  public DataFetcher<?> generateDeleteByPkMutationFieldDataFetcher(TableMetadata tableMetadata) {
    return env -> {
      return transactionTemplate.execute(transactionStatus -> {
        String deleteByPkSql = DataFetcherSqlGenerator.generateDeleteByPkMutationSql(tableMetadata, env);
        ResultSetExtractor<List<ObjectNode>> rsExtractor = ResultSetUtils::mapResultSetToListJson;
        List<ObjectNode> result = jdbcTemplate.query(deleteByPkSql, rsExtractor);
        if (CollectionUtils.isEmpty(result)) {
          return null;
        }
        return result.get(0);
      });
    };
  }

  private ObjectNode queryByPk(TableConfig tableConfig, DataFetchingEnvironment env) {
    ResultSetExtractor<List<ObjectNode>> rsExtractor = ResultSetUtils::mapResultSetToListJson;
    String querySql = DataFetcherSqlGenerator.generateQueryByPkSql(tableConfig, env);
    List<ObjectNode> result = jdbcTemplate.query(querySql, rsExtractor);
    if (CollectionUtils.isEmpty(result)) {
      return null;
    }
    if (result.size() > 1) {
      throw new IllegalStateException("unexpected state");
    }
    return result.get(0);
  }

  private List<ObjectNode> query(TableConfig tableConfig, DataFetchingEnvironment env) {
    ResultSetExtractor<List<ObjectNode>> rsExtractor = ResultSetUtils::mapResultSetToListJson;
    String querySql = DataFetcherSqlGenerator.generateQueryListSql(tableConfig, env);
    return jdbcTemplate.query(querySql, rsExtractor);
  }

  private Set<String> parseRelatedTableSetFromSelectionSet(DataFetchingEnvironment env, TableConfig tableConfig) {
    if (CollectionUtils.isEmpty(tableConfig.getRelationAsSourceTable())
      && CollectionUtils.isEmpty(tableConfig.getRelationAsTargetTable())) {
      return Set.of(tableConfig.getTableMetadata().getName());
    }
    Set<String> relatedTableSet = generateRelatedTableSet(tableConfig);
    Set<String> selectionSet = GraphQLUtils.getMutationSelectionSet(env);
    selectionSet.add(tableConfig.getTableMetadata().getName());
    return Sets.intersection(relatedTableSet, selectionSet).immutableCopy();
  }

  private Set<String> generateRelatedTableSet(TableConfig tableConfig) {
    Set<String> relatedTableSet = new HashSet<>();
    relatedTableSet.add(tableConfig.getTableMetadata().getName());
    if (!CollectionUtils.isEmpty(tableConfig.getRelationAsSourceTable())) {
      tableConfig.getRelationAsSourceTable().stream()
        .forEach(relationMetadata -> {
          relatedTableSet.add(relationMetadata.getTargetTable());
        });
    }
    if (!CollectionUtils.isEmpty(tableConfig.getRelationAsTargetTable())) {
      tableConfig.getRelationAsTargetTable().stream()
        .forEach(relationMetadata -> {
          relatedTableSet.add(relationMetadata.getSourceTable());
        });
    }
    return relatedTableSet;
  }

  private void executeInsertOneRelationObjectMutation(RelationTarget target,
                                                      Map<String, Object> objectInput, Map<String, Object> onConflict, Long objectId) {
    TableConfig tableConfig = tableConfigMap.get(target.getTableName());
    Set<String> columnSet = DataModelUtils.generateColumnSet(tableConfig.getTableMetadata());
    objectInput.put(target.getTargetColumn(), objectId);
    String insertRelationSql = DataFetcherSqlGenerator.generateInsertOneMutationSql(
      target.getTableName(), columnSet, Set.of(DSL.field(GraphQLConstants.ID)), objectInput, onConflict);
    ResultSetExtractor<ObjectNode> rsExtractor = ResultSetUtils::mapResultSetToObjectNode;
    StatementCallback<ObjectNode> callback = ResultSetUtils.generateStatementCallback(insertRelationSql, rsExtractor);
    ObjectNode relationObject = jdbcTemplate.execute(callback);
    long relationObjectId = relationObject.get(GraphQLConstants.ID).asLong();
    Map<RelationTarget, Object> relationObjectInsertInput = findRelationObjectInsertInput(tableConfig, objectInput);
    executeRelationObjectInsert(relationObjectInsertInput, relationObjectId);
  }

  private void executeRelationObjectInsert(Map<RelationTarget, Object> relationObjectInsertInput, Long objectId) {
    if (!CollectionUtils.isEmpty(relationObjectInsertInput)) {
      relationObjectInsertInput.entrySet().forEach(entry -> {
        Map<String, Object> relationArguments = Utils.uncheckedCast(entry.getValue());
        Object relationInputData = relationArguments.get(GraphQLConstants.DATA);
        Map<String, Object> relationInputOnConflict = Utils
          .uncheckedCast(relationArguments.get(GraphQLConstants.ON_CONFLICT));
        if (relationInputData instanceof List) {
          List<Object> relationInputDataList = Utils.uncheckedCast(relationInputData);
          relationInputDataList.stream()
            .forEach(input -> executeInsertOneRelationObjectMutation(entry.getKey(),
              (Map<String, Object>)input, relationInputOnConflict, objectId));
        } else {
          executeInsertOneRelationObjectMutation(entry.getKey(),
            (Map<String, Object>)relationInputData, relationInputOnConflict, objectId);
        }
      });
    }
  }

  private ObjectNode mapReturningResultToMutationResponse(ArrayNode batchMutationReturning) {
    ObjectNode objectNode = Utils.newObjectNode();
    if (batchMutationReturning == null) {
      return null;
    }
    objectNode.put(GraphQLConstants.AFFECTED_ROWS, batchMutationReturning.size());
    objectNode.set(GraphQLConstants.RETURNING, batchMutationReturning);
    return objectNode;
  }

  private Object mappingResult(ColumnType columnType, AggregateType aggregateType, ObjectNode node) {
    JsonNode result = node.get(aggregateType.getGraphQLFieldName());
    Class<?> javaType = aggregateType.aggregateResultJavaType(columnType);
    return objectMapper.convertValue(result, javaType);
  }

  private Map<RelationTarget, Object> findRelationObjectInsertInput(TableConfig tableConfig, Map<String, Object> objectInput) {
    if (CollectionUtils.isEmpty(tableConfig.getRelationAsSourceTable())) {
      return null;
    }
    return tableConfig.getRelationAsSourceTable().stream()
      .filter(relationMetadata -> objectInput.containsKey(relationMetadata.getNameInSource()))
      .map(relationMetadata -> {
        RelationTarget target = new RelationTarget(relationMetadata.getTargetTable(), relationMetadata.getTargetColumn());
        return Map.entry(target, objectInput.get(relationMetadata.getNameInSource()));
      }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  @AllArgsConstructor
  @Getter
  public static class InsertInput {
    private JdbcTemplate jdbcTemplate;
    private TableConfig tableConfig;
    private Object insertObject;
    private Map<String, Object> onConflict;
  }

  @AllArgsConstructor
  @Getter
  @EqualsAndHashCode
  public static class RelationTarget {
    private String tableName;
    private String targetColumn;
  }
}
