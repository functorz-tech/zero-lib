package com.functorz.zero.graphql.generator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.functorz.zero.datamodel.ColumnMetadata;
import com.functorz.zero.datamodel.ColumnType;
import com.functorz.zero.datamodel.TableMetadata;
import com.functorz.zero.graphql.DataFetcherGenerator;
import com.functorz.zero.graphql.GraphQLConstants;
import com.functorz.zero.graphql.datafetcher.DefaultJsonFieldDataFetcher;
import com.functorz.zero.graphql.datafetcher.RootObjectDataFetcher;
import com.functorz.zero.utils.GraphQLUtils;
import graphql.Scalars;
import graphql.language.FieldDefinition;
import graphql.language.InputValueDefinition;
import graphql.language.NonNullType;
import graphql.language.Type;
import graphql.language.TypeName;
import graphql.schema.DataFetcher;
import graphql.schema.FieldCoordinates;
import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLCodeRegistry;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLNonNull;
import graphql.schema.GraphQLOutputType;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphQLSchema.Builder;
import java.util.List;
import java.util.stream.Collectors;

public class GraphQLSchemaGenerator {

  private final String queryRoot;
  private final String mutationRoot;
  private final String subscriptionRoot;

  private OperationMapper operationMapper = new OperationMapper();
  private DataFetcherGenerator dataFetcherGenerator;
  private GraphQLTypeManager typeManager;
  private ObjectMapper objectMapper;
  private GraphQLCodeRegistry.Builder codeRegistryBuilder;

  public GraphQLSchemaGenerator(DataFetcherGenerator dataFetcherGenerator, ObjectMapper objectMapper) {
    this("Query", "Mutation", "Subscription");
    this.dataFetcherGenerator = dataFetcherGenerator;
    this.objectMapper = objectMapper;
    this.typeManager = new GraphQLTypeManager(dataFetcherGenerator, objectMapper, codeRegistryBuilder);
  }

  private GraphQLSchemaGenerator(String queryRoot, String mutationRoot, String subscriptionRoot) {
    this.queryRoot = queryRoot;
    this.mutationRoot = mutationRoot;
    this.subscriptionRoot = subscriptionRoot;
    this.codeRegistryBuilder = GraphQLCodeRegistry.newCodeRegistry();
    codeRegistryBuilder.defaultDataFetcher(env -> DefaultJsonFieldDataFetcher.fetching(false));
  }

  public GraphQLSchema generate() {
    Builder schemaBuilder = GraphQLSchema.newSchema();
    schemaBuilder.query(operationMapper.getRootQuery(queryRoot));
    schemaBuilder.mutation(operationMapper.getRootMutation(mutationRoot));
    schemaBuilder.subscription(operationMapper.getRootSubscription(subscriptionRoot));
    schemaBuilder.additionalTypes(typeManager.getAdditionalTypes());
    schemaBuilder.codeRegistry(codeRegistryBuilder.build());
    return schemaBuilder.build();
  }

  public void registerTableMetadata(TableConfig tableConfig) {
    typeManager.registerTable(tableConfig);
    // register query api
    registerObjectRootListQueryOperation(tableConfig, false);
    registerTableRootQueryByIdOperation(tableConfig, false);
    registerTableAggregateQueryOperation(tableConfig, false);

    // register mutation api
    registerTableDeleteMutationOperation(tableConfig);
    registerTableDeleteByPkMutationOperation(tableConfig);

    registerTableInsertMutationOperation(tableConfig);
    registerTableInsertOneMutationOperation(tableConfig);

    registerTableUpdateMutationOperation(tableConfig);
    registerTableUpdateByPkMutationOperation(tableConfig);

    // register subscription api
    registerTableRootQueryByIdOperation(tableConfig, true);
    registerObjectRootListQueryOperation(tableConfig, true);
    registerTableAggregateQueryOperation(tableConfig, true);
  }

  private void registerTableUpdateMutationOperation(TableConfig tableConfig) {
    TableMetadata tableMetadata = tableConfig.getTableMetadata();
    List<GraphQLArgument> updateArguments = typeManager.generateTableUpdateMutationArguments(tableMetadata);
    List<InputValueDefinition> inputValueDefinitions = updateArguments.stream()
      .map(GraphQLArgument::getDefinition).collect(Collectors.toList());
    String tableMutationResponseTypeName = String.format(
      GraphQLConstants.MUTATION_RESPONSE_TYPE_NAME_FORMAT, tableMetadata.getName());
    String updateMutationName = String
      .format(GraphQLConstants.UPDATE_MUTATION_NAME_FORMAT, tableMetadata.getName());
    FieldDefinition fieldDefinition = FieldDefinition.newFieldDefinition()
      .name(updateMutationName)
      .type(new TypeName(tableMutationResponseTypeName))
      .inputValueDefinitions(inputValueDefinitions)
      .build();
    FieldCoordinates updateCoordinates = FieldCoordinates.coordinates(mutationRoot, updateMutationName);
    DataFetcher<?> updateDataFetcher = dataFetcherGenerator.generateUpdateMutationFieldDataFetcher(tableConfig);
    codeRegistryBuilder.dataFetcher(updateCoordinates, updateDataFetcher);
    GraphQLFieldDefinition updateMutationFieldDefinition = GraphQLFieldDefinition.newFieldDefinition()
      .name(updateMutationName)
      .arguments(updateArguments)
      .type((GraphQLOutputType) typeManager.getObjectTypeByName(tableMutationResponseTypeName))
      .definition(fieldDefinition)
      .build();
    operationMapper.registerRootMutationOperation(updateMutationFieldDefinition);
  }

  private void registerTableUpdateByPkMutationOperation(TableConfig tableConfig) {
    TableMetadata tableMetadata = tableConfig.getTableMetadata();
    List<GraphQLArgument> updateByPkArguments = typeManager.generateTableUpdateByPkMutationArguments(tableMetadata);
    List<InputValueDefinition> inputValueDefinitions = updateByPkArguments.stream()
      .map(GraphQLArgument::getDefinition).collect(Collectors.toList());
    String updateByPkMutationName = String
      .format(GraphQLConstants.UPDATE_BY_PK_MUTATION_NAME_FORMAT, tableMetadata.getName());
    FieldDefinition fieldDefinition = FieldDefinition.newFieldDefinition()
      .name(updateByPkMutationName)
      .type(new TypeName(tableMetadata.getName()))
      .inputValueDefinitions(inputValueDefinitions)
      .build();
    FieldCoordinates updateByPkCoordinates = FieldCoordinates.coordinates(mutationRoot, updateByPkMutationName);
    DataFetcher<?> updateDataFetcher = dataFetcherGenerator.generateUpdateByPkMutationFieldDataFetcher(tableConfig);
    codeRegistryBuilder.dataFetcher(updateByPkCoordinates, updateDataFetcher);
    GraphQLFieldDefinition updateByPkMutationFieldDefinition = GraphQLFieldDefinition.newFieldDefinition()
      .name(updateByPkMutationName)
      .arguments(updateByPkArguments)
      .type((GraphQLOutputType) typeManager.getObjectTypeByName(tableMetadata.getName()))
      .definition(fieldDefinition)
      .build();
    operationMapper.registerRootMutationOperation(updateByPkMutationFieldDefinition);
  }

  private void registerTableInsertMutationOperation(TableConfig tableConfig) {
    TableMetadata tableMetadata = tableConfig.getTableMetadata();
    List<GraphQLArgument> insertArguments = typeManager.generateTableInsertMutationArguments(tableMetadata);
    List<InputValueDefinition> inputValueDefinitions = insertArguments.stream()
      .map(GraphQLArgument::getDefinition).collect(Collectors.toList());
    String tableMutationResponseTypeName = String.format(
        GraphQLConstants.MUTATION_RESPONSE_TYPE_NAME_FORMAT, tableMetadata.getName());
    String insertMutationName = String.format(GraphQLConstants.INSERT_MUTATION_NAME_FORMAT, tableMetadata.getName());
    FieldDefinition fieldDefinition = FieldDefinition.newFieldDefinition()
      .name(insertMutationName)
      .type(new TypeName(tableMutationResponseTypeName))
      .inputValueDefinitions(inputValueDefinitions)
      .build();
    FieldCoordinates insertCoordinates = FieldCoordinates.coordinates(mutationRoot, insertMutationName);
    DataFetcher<?> insertDataFetcher = dataFetcherGenerator.generateInsertMutationFieldDataFetcher(tableConfig);
    codeRegistryBuilder.dataFetcher(insertCoordinates, insertDataFetcher);
    GraphQLFieldDefinition insertMutationFieldDefinition = GraphQLFieldDefinition.newFieldDefinition()
      .name(insertMutationName)
      .arguments(insertArguments)
      .type((GraphQLOutputType) typeManager.getObjectTypeByName(tableMutationResponseTypeName))
      .definition(fieldDefinition)
      .build();
    operationMapper.registerRootMutationOperation(insertMutationFieldDefinition);
  }

  private void registerTableInsertOneMutationOperation(TableConfig tableConfig) {
    TableMetadata tableMetadata = tableConfig.getTableMetadata();
    List<GraphQLArgument> insertOneArgument = typeManager.generateTableInsertOneMutationArguments(tableMetadata);
    List<InputValueDefinition> inputValueDefinitions = insertOneArgument.stream()
      .map(GraphQLArgument::getDefinition).collect(Collectors.toList());
    String insertOneMutationName = String.format(GraphQLConstants.INSERT_ONE_MUTATION_NAME_FORMAT, tableMetadata.getName());
    FieldDefinition fieldDefinition = FieldDefinition.newFieldDefinition()
      .name(insertOneMutationName)
      .type(new TypeName(tableMetadata.getName()))
      .inputValueDefinitions(inputValueDefinitions)
      .build();
    FieldCoordinates insertCoordinates = FieldCoordinates.coordinates(mutationRoot, insertOneMutationName);
    DataFetcher<?> insertDataFetcher = dataFetcherGenerator.generateInsertOneMutationFieldDataFetcher(tableConfig);
    codeRegistryBuilder.dataFetcher(insertCoordinates, insertDataFetcher);
    GraphQLFieldDefinition insertMutationFieldDefinition = GraphQLFieldDefinition.newFieldDefinition()
      .name(insertOneMutationName)
      .arguments(insertOneArgument)
      .type((GraphQLOutputType) typeManager.getObjectTypeByName(tableMetadata.getName()))
      .definition(fieldDefinition)
      .build();
    operationMapper.registerRootMutationOperation(insertMutationFieldDefinition);
  }

  private void registerTableDeleteMutationOperation(TableConfig tableConfig) {
    TableMetadata tableMetadata = tableConfig.getTableMetadata();
    GraphQLArgument argument = typeManager.generateTableDeleteMutationArgument(tableMetadata);
    String deleteMutationName = String.format(GraphQLConstants.DELETE_MUTATION_NAME_FORMAT, tableMetadata.getName());
    String tableMutationResponseTypeName = String.format(
      GraphQLConstants.MUTATION_RESPONSE_TYPE_NAME_FORMAT, tableMetadata.getName());
    FieldDefinition fieldDefinition = FieldDefinition.newFieldDefinition()
      .name(deleteMutationName)
      .type(new TypeName(tableMutationResponseTypeName))
      .inputValueDefinition(argument.getDefinition())
      .build();
    FieldCoordinates deleteCoordinates = FieldCoordinates.coordinates(mutationRoot, deleteMutationName);
    DataFetcher<?> dataFetcher = dataFetcherGenerator.generateDeleteMutationFieldDataFetcher(tableMetadata);
    codeRegistryBuilder.dataFetcher(deleteCoordinates, dataFetcher);
    GraphQLFieldDefinition deleteMutationFieldDefinition = GraphQLFieldDefinition.newFieldDefinition()
      .name(deleteMutationName)
      .argument(argument)
      .type((GraphQLOutputType) typeManager.getObjectTypeByName(tableMutationResponseTypeName))
      .definition(fieldDefinition)
      .build();
    operationMapper.registerRootMutationOperation(deleteMutationFieldDefinition);
  }

  private void registerTableDeleteByPkMutationOperation(TableConfig tableConfig) {
    TableMetadata tableMetadata = tableConfig.getTableMetadata();
    GraphQLArgument argument = typeManager.generateTableDeleteByPkMutationArgument(tableMetadata);

    String deleteByPkMutationName = String.format(GraphQLConstants.DELETE_MUTATION_BY_PK_NAME_FORMAT, tableMetadata.getName());
    FieldDefinition fieldDefinition = FieldDefinition.newFieldDefinition()
      .name(deleteByPkMutationName)
      .type(new TypeName(Scalars.GraphQLLong.getName()))
      .inputValueDefinition(argument.getDefinition())
      .build();
    FieldCoordinates deleteByPkCoordinates = FieldCoordinates.coordinates(mutationRoot, deleteByPkMutationName);
    DataFetcher<?> dataFetcher = dataFetcherGenerator.generateDeleteByPkMutationFieldDataFetcher(tableMetadata);
    codeRegistryBuilder.dataFetcher(deleteByPkCoordinates, dataFetcher);
    GraphQLFieldDefinition deleteMutationFieldDefinition = GraphQLFieldDefinition.newFieldDefinition()
      .name(deleteByPkMutationName)
      .argument(argument)
      .type((GraphQLOutputType) typeManager.getObjectTypeByName(tableMetadata.getName()))
      .definition(fieldDefinition)
      .build();
    operationMapper.registerRootMutationOperation(deleteMutationFieldDefinition);
  }

  private void registerTableAggregateQueryOperation(TableConfig tableConfig, boolean isSubscription) {
    TableMetadata tableMetadata = tableConfig.getTableMetadata();
    List<InputValueDefinition> inputValueDefinitions = generateTableListOrAggregateQueryInputDefinition(tableMetadata);
    String aggregateTypeName = String.format(GraphQLConstants.AGGREGATE_QUERY_TYPE_NAME_FORMAT, tableMetadata.getName());
    FieldDefinition fieldDefinition = FieldDefinition.newFieldDefinition()
        .name(aggregateTypeName)
        .inputValueDefinitions(inputValueDefinitions)
        .type(new TypeName(aggregateTypeName))
        .build();
    List<GraphQLArgument> arguments = typeManager.generateTableQueryArguments(tableMetadata.getName());
    String rootField = isSubscription ? subscriptionRoot : queryRoot;
    FieldCoordinates aggregateCoordinates = FieldCoordinates.coordinates(rootField, aggregateTypeName);
    DataFetcher dataFetcher = isSubscription
      ? dataFetcherGenerator.generateSubscriptionAggregateDataFetcher(tableConfig)
      : new RootObjectDataFetcher(objectMapper);
    codeRegistryBuilder.dataFetcher(aggregateCoordinates, dataFetcher);
    GraphQLFieldDefinition rootAggregateQueryDefinition = GraphQLFieldDefinition.newFieldDefinition()
      .name(aggregateTypeName)
      .description(tableMetadata.getDescription())
      .definition(fieldDefinition)
      .arguments(arguments)
      .type(GraphQLNonNull.nonNull(typeManager.getObjectTypeByName(aggregateTypeName)))
      .build();
    if (isSubscription) {
      operationMapper.registerRootSubscriptionOperation(rootAggregateQueryDefinition);
    } else {
      operationMapper.registerRootQueryOperation(rootAggregateQueryDefinition);
    }
  }

  private void registerTableRootQueryByIdOperation(TableConfig tableConfig, boolean isSubscription) {
    TableMetadata tableMetadata = tableConfig.getTableMetadata();
    ColumnMetadata primaryKeyColumn = tableMetadata.getPrimaryKeyColumn();
    InputValueDefinition inputValueDefinition = InputValueDefinition.newInputValueDefinition()
      .name(primaryKeyColumn.getName())
      .type(mapColumnTypeToGraphQLType(primaryKeyColumn))
      .build();
    FieldDefinition fieldDefinition = FieldDefinition.newFieldDefinition()
      .name(tableMetadata.getName())
      .inputValueDefinition(inputValueDefinition)
      .type(new TypeName(tableMetadata.getName()))
      .build();
    GraphQLArgument primaryKeyArgument = GraphQLArgument.newArgument()
      .name(primaryKeyColumn.getName())
      .definition(inputValueDefinition)
      .type(GraphQLNonNull.nonNull(GraphQLUtils.mapColumnToGraphQLScalarType(primaryKeyColumn.getType())))
      .build();
    String queryByPkFieldName = String.format(GraphQLConstants.QUERY_BY_PK_NAME_FORMAT, tableMetadata.getName());
    String rootField = isSubscription ? subscriptionRoot : queryRoot;
    FieldCoordinates queryByPkCoordinates = FieldCoordinates.coordinates(rootField, queryByPkFieldName);
    DataFetcher dataFetcher = isSubscription
      ? dataFetcherGenerator.generateSubscriptionQueryDataFetcher(tableConfig, false)
      : dataFetcherGenerator.generateQueryByPkDataFetcher(tableConfig);
    codeRegistryBuilder.dataFetcher(queryByPkCoordinates, dataFetcher);
    GraphQLFieldDefinition rootTableQueryByPkDefinition = GraphQLFieldDefinition.newFieldDefinition()
      .name(queryByPkFieldName)
      .description(tableMetadata.getDescription())
      .definition(fieldDefinition)
      .argument(primaryKeyArgument)
      .type((GraphQLOutputType) typeManager.getObjectTypeByName(tableMetadata.getName()))
      .build();
    if (isSubscription) {
      operationMapper.registerRootSubscriptionOperation(rootTableQueryByPkDefinition);
    } else {
      operationMapper.registerRootQueryOperation(rootTableQueryByPkDefinition);
    }
  }

  private void registerObjectRootListQueryOperation(TableConfig tableConfig, boolean isSubscription) {
    TableMetadata tableMetadata = tableConfig.getTableMetadata();
    List<InputValueDefinition> inputValueDefinitions =
      generateTableListOrAggregateQueryInputDefinition(tableMetadata);
    FieldDefinition fieldDefinition = FieldDefinition.newFieldDefinition()
      .name(tableMetadata.getName())
      .inputValueDefinitions(inputValueDefinitions)
      .type(new TypeName(tableMetadata.getName()))
      .build();
    List<GraphQLArgument> arguments = typeManager.generateTableQueryArguments(tableMetadata.getName());
    String rootField = isSubscription ? subscriptionRoot : queryRoot;
    FieldCoordinates listRootQueryCoordinates = FieldCoordinates.coordinates(rootField, tableMetadata.getName());
    DataFetcher dataFetcher = isSubscription
      ? dataFetcherGenerator.generateSubscriptionQueryDataFetcher(tableConfig, true)
      : dataFetcherGenerator.generateQueryListDataFetcher(tableConfig);
    codeRegistryBuilder.dataFetcher(listRootQueryCoordinates, dataFetcher);
    GraphQLFieldDefinition rootQueryListDefinition = GraphQLFieldDefinition.newFieldDefinition()
      .name(tableMetadata.getName())
      .description(tableMetadata.getDescription())
      .definition(fieldDefinition)
      .arguments(arguments)
      .type(GraphQLNonNull.nonNull(GraphQLList.list(
          GraphQLNonNull.nonNull(typeManager.getObjectTypeByName(tableMetadata.getName())))))
      .build();
    if (isSubscription) {
      operationMapper.registerRootSubscriptionOperation(rootQueryListDefinition);
    } else {
      operationMapper.registerRootQueryOperation(rootQueryListDefinition);
    }
  }

  private List<InputValueDefinition> generateTableListOrAggregateQueryInputDefinition(TableMetadata tableMetadata) {
    String whereInputTypeName = String.format(GraphQLConstants.WHERE_TYPE_FORMAT, tableMetadata.getName());
    String orderByInputTypeName = String.format(GraphQLConstants.ORDER_BY_TYPE_NAME_FORMAT, tableMetadata.getName());
    String distinctOnInputTypeName = String.format(GraphQLConstants.QUERY_DISTINCT_TYPE_NAME_FORMAT, tableMetadata.getName());
    InputValueDefinition whereInputDefinition = generateInputValueDefinition(
      whereInputTypeName, new TypeName(whereInputTypeName));
    InputValueDefinition orderByInputDefinition = generateInputValueDefinition(
      orderByInputTypeName, new TypeName(orderByInputTypeName));
    InputValueDefinition distinctOnInputDefinition = generateInputValueDefinition(
      distinctOnInputTypeName, new TypeName(distinctOnInputTypeName));
    TypeName intType = GraphQLUtils.mapColumnTypeToTypeName(ColumnType.INTEGER);
    InputValueDefinition limitInputDefinition = generateInputValueDefinition(
      GraphQLConstants.LIMIT, intType);
    InputValueDefinition offsetInputDefinition = generateInputValueDefinition(
      GraphQLConstants.OFFSET, intType);
    return List.of(whereInputDefinition, orderByInputDefinition,
      distinctOnInputDefinition, limitInputDefinition, offsetInputDefinition);
  }

  private InputValueDefinition generateInputValueDefinition(String inputName, Type inputType) {
    return InputValueDefinition.newInputValueDefinition()
      .name(inputName)
      .type(inputType)
      .build();
  }

  private Type<? extends Type> mapColumnTypeToGraphQLType(ColumnMetadata columnMetadata) {
    ColumnType columnType = columnMetadata.getType();
    boolean required = columnMetadata.isRequired() || columnMetadata.isPrimaryKey();
    TypeName typeName = GraphQLUtils.mapColumnTypeToTypeName(columnType);
    return required ? new NonNullType(typeName) : typeName;
  }
}
