package com.functorz.zero.graphql.generator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.functorz.zero.datamodel.ColumnMetadata;
import com.functorz.zero.datamodel.ColumnType;
import com.functorz.zero.datamodel.RelationMetadata;
import com.functorz.zero.datamodel.RelationType;
import com.functorz.zero.datamodel.TableMetadata;
import com.functorz.zero.datamodel.constraint.PrimaryKeyConstraint;
import com.functorz.zero.datamodel.constraint.UniqueConstraint;
import com.functorz.zero.graphql.DataFetcherGenerator;
import com.functorz.zero.graphql.GraphQLConstants;
import com.functorz.zero.graphql.datafetcher.DefaultJsonFieldDataFetcher;
import com.functorz.zero.graphql.datafetcher.RootObjectDataFetcher;
import com.functorz.zero.utils.GraphQLUtils;
import com.functorz.zero.utils.Utils;
import graphql.Scalars;
import graphql.language.EnumTypeDefinition;
import graphql.language.EnumValueDefinition;
import graphql.language.FieldDefinition;
import graphql.language.InputObjectTypeDefinition;
import graphql.language.InputObjectTypeDefinition.Builder;
import graphql.language.InputValueDefinition;
import graphql.language.ListType;
import graphql.language.NonNullType;
import graphql.language.ObjectTypeDefinition;
import graphql.language.Type;
import graphql.language.TypeName;
import graphql.schema.DataFetcher;
import graphql.schema.FieldCoordinates;
import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLCodeRegistry;
import graphql.schema.GraphQLEnumType;
import graphql.schema.GraphQLEnumValueDefinition;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLInputObjectField;
import graphql.schema.GraphQLInputObjectType;
import graphql.schema.GraphQLInputType;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLNonNull;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLOutputType;
import graphql.schema.GraphQLScalarType;
import graphql.schema.GraphQLType;
import graphql.schema.GraphQLTypeReference;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.springframework.util.CollectionUtils;

public class GraphQLTypeManager {

  private final EnumMap<ColumnType, GraphQLInputObjectType> inputObjectTypeByColumnType;
  private final Map<String, GraphQLEnumType> graphQLEnumTypeByName = new HashMap<>();
  private final Map<String, GraphQLObjectType> graphQLObjectTypeByName = new HashMap<>();
  private final Map<String, GraphQLInputObjectType> graphQLInputObjectTypeByName = new HashMap<>();

  private DataFetcherGenerator dataFetcherGenerator;
  private ObjectMapper objectMapper;
  private GraphQLCodeRegistry.Builder codeRegistryBuilder;

  protected GraphQLTypeManager(DataFetcherGenerator dataFetcherGenerator,
                               ObjectMapper objectMapper, GraphQLCodeRegistry.Builder codeRegistryBuilder) {
    this.inputObjectTypeByColumnType = new EnumMap<>(ColumnType.class);
    this.dataFetcherGenerator = dataFetcherGenerator;
    this.objectMapper = objectMapper;
    this.codeRegistryBuilder = codeRegistryBuilder;
    initColumnInputObjectType();
  }

  public Set<GraphQLType> getAdditionalTypes() {
    return Stream.of(graphQLInputObjectTypeByName.values(),
      graphQLObjectTypeByName.values(),
      graphQLEnumTypeByName.values()).flatMap(Collection::stream).collect(Collectors.toSet());
  }

  public GraphQLType getObjectTypeByName(String typeName) {
    return GraphQLUtils.wrappedTypeReferenceIfNull(graphQLObjectTypeByName.get(typeName), typeName);
  }

  public GraphQLInputType getInputObjectTypeByName(String typeName) {
    return (GraphQLInputType) GraphQLUtils
      .wrappedTypeReferenceIfNull(graphQLInputObjectTypeByName.get(typeName), typeName);
  }

  public GraphQLType getGraphQLEnumType(String enumTypeName) {
    return GraphQLUtils.wrappedTypeReferenceIfNull(
      graphQLEnumTypeByName.get(enumTypeName), enumTypeName);
  }

  public void registerTable(TableConfig tableConfig) {
    TableMetadata tableMetadata = tableConfig.getTableMetadata();

    // generate table distinct enum type
    GraphQLEnumType distinctEnumType = generateDistinctOnEnumType(tableMetadata);
    graphQLEnumTypeByName.put(distinctEnumType.getName(), distinctEnumType);

    // generate where input object type
    GraphQLInputObjectType whereInputObjectType = generateWhereConditionType(tableMetadata);
    graphQLInputObjectTypeByName.put(whereInputObjectType.getName(), whereInputObjectType);

    // generate order by input object type
    GraphQLInputObjectType orderByInputObjectType = generateOrderByConditionType(tableMetadata);
    graphQLInputObjectTypeByName.put(orderByInputObjectType.getName(), orderByInputObjectType);

    // generate table object type
    GraphQLObjectType graphQLObjectType = generateTableObjectType(tableConfig);
    graphQLObjectTypeByName.put(graphQLObjectType.getName(), graphQLObjectType);

    // generate table aggregate fields object type
    GraphQLObjectType aggregateFieldObjectType = generateTableAggregateFieldObjectType(tableMetadata);
    graphQLObjectTypeByName.put(aggregateFieldObjectType.getName(), aggregateFieldObjectType);

    // generate table aggregate object type
    GraphQLObjectType aggregateObjectType = generateTableAggregateObjectType(tableConfig);
    graphQLObjectTypeByName.put(aggregateObjectType.getName(), aggregateObjectType);

    // generate table mutation response type
    GraphQLObjectType tableMutationResponseType = generateTableMutationResponseType(tableConfig);
    graphQLObjectTypeByName.put(tableMutationResponseType.getName(), tableMutationResponseType);

    // generate table insert input type
    GraphQLInputObjectType insertInputType = generateTableInsertInputType(tableConfig);
    graphQLInputObjectTypeByName.put(insertInputType.getName(), insertInputType);

    // generate table on conflict constraint enum type
    GraphQLEnumType constraintEnumType = generateTableOnConflictConstraintEnumType(tableConfig);
    graphQLEnumTypeByName.put(constraintEnumType.getName(), constraintEnumType);

    // generate table on conflict update column enum type
    GraphQLEnumType updateColumnEnumType = generateTableOnConflictUpdateColumnEnumType(tableConfig);
    graphQLEnumTypeByName.put(updateColumnEnumType.getName(), updateColumnEnumType);

    // generate table insert on conflict type
    GraphQLInputObjectType insertOnConflictType = generateTableInsertOnConflictType(tableMetadata.getName());
    graphQLInputObjectTypeByName.put(insertOnConflictType.getName(), insertOnConflictType);

    // generate update jsonb arg type List
    List<GraphQLInputObjectType> updateJsonbArgTypeList = generateTableUpdateJsonbArgTypeList(tableConfig);
    updateJsonbArgTypeList.stream()
      .forEach(type -> graphQLInputObjectTypeByName.put(type.getName(), type));

    // generate update set type
    GraphQLInputObjectType updateSetType = generateTableUpdateSetOrIncType(
      tableConfig,
      GraphQLConstants.UPDATE_ARG_SET_TYPE_NAME_FORMAT,
      columnMetadata -> true);
    graphQLInputObjectTypeByName.put(updateSetType.getName(), updateSetType);

    // generate update inc type
    GraphQLInputObjectType updateIncType = generateTableUpdateSetOrIncType(tableConfig,
      GraphQLConstants.UPDATE_ARG_INC_TYPE_NAME_FORMAT,
      columnMetadata -> columnMetadata.getType().isNumeric());
    graphQLInputObjectTypeByName.put(updateIncType.getName(), updateIncType);

    GraphQLInputObjectType updatePkColumnType = generateTableUpdatePkColumnType(tableConfig);
    graphQLInputObjectTypeByName.put(updatePkColumnType.getName(), updatePkColumnType);
  }

  private GraphQLInputObjectType generateTableUpdatePkColumnType(TableConfig tableConfig) {
    TableMetadata tableMetadata = tableConfig.getTableMetadata();
    String typeName = String.format(GraphQLConstants
      .UPDATE_ARG_PK_COLUMNS_TYPE_NAME_FORMAT, tableMetadata.getName());
    InputValueDefinition inputValueDefinition = InputValueDefinition
      .newInputValueDefinition()
      .name(GraphQLConstants.ID)
      .type(new NonNullType(new TypeName("Long")))
      .build();
    GraphQLInputObjectField graphQLInputObjectField = GraphQLInputObjectField
      .newInputObjectField()
      .name(GraphQLConstants.ID)
      .definition(inputValueDefinition)
      .type(Scalars.GraphQLLong)
      .build();
    InputObjectTypeDefinition inputObjectTypeDefinition = InputObjectTypeDefinition
      .newInputObjectDefinition()
      .name(typeName)
      .inputValueDefinition(inputValueDefinition)
      .build();
    return GraphQLInputObjectType.newInputObject()
      .name(typeName)
      .field(graphQLInputObjectField)
      .definition(inputObjectTypeDefinition)
      .build();
  }

  private GraphQLInputObjectType generateTableUpdateSetOrIncType(TableConfig tableConfig,
                                                                 String typeNameFormat,
                                                                 Predicate<ColumnMetadata> columnPredicate) {
    TableMetadata tableMetadata = tableConfig.getTableMetadata();
    String typeName = String.format(typeNameFormat, tableMetadata.getName());
    List<GraphQLInputObjectField> graphQLInputObjectFields = tableMetadata.getColumnMetadata().stream()
      .filter(columnPredicate)
      .map(GraphQLUtils::convertColumnMetadataToGraphQLInputObjectField)
      .collect(Collectors.toList());
    List<InputValueDefinition> inputValueDefinitions = graphQLInputObjectFields
        .stream().map(GraphQLInputObjectField::getDefinition).collect(Collectors.toList());
    InputObjectTypeDefinition inputObjectTypeDefinition = InputObjectTypeDefinition
        .newInputObjectDefinition()
        .name(typeName)
        .inputValueDefinitions(inputValueDefinitions)
        .build();
    return GraphQLInputObjectType.newInputObject()
      .name(typeName)
      .fields(graphQLInputObjectFields)
      .definition(inputObjectTypeDefinition)
      .build();
  }

  private List<GraphQLInputObjectType> generateTableUpdateJsonbArgTypeList(TableConfig tableConfig) {
    return List.of();
  }

  private GraphQLEnumType generateTableOnConflictUpdateColumnEnumType(TableConfig tableConfig) {
    TableMetadata tableMetadata = tableConfig.getTableMetadata();
    String typeName = String.format(GraphQLConstants.ON_CONFLICT_ARG_UPDATE_COLUMNS_TYPE_NAME_FORMAT, tableMetadata.getName());
    List<GraphQLEnumValueDefinition> graphQLEnumValueDefinitions = tableMetadata
      .getColumnMetadata().stream().map(columnMetadata -> {
        EnumValueDefinition enumValueDefinition = EnumValueDefinition
          .newEnumValueDefinition()
          .name(columnMetadata.getName())
          .build();
        return GraphQLEnumValueDefinition.newEnumValueDefinition()
          .name(columnMetadata.getName())
          .definition(enumValueDefinition)
          .value(columnMetadata.getName())
          .build();
      }).collect(Collectors.toList());
    List<EnumValueDefinition> enumValueDefinitions = graphQLEnumValueDefinitions.stream()
      .map(GraphQLEnumValueDefinition::getDefinition)
      .collect(Collectors.toList());
    EnumTypeDefinition enumTypeDefinition = EnumTypeDefinition.newEnumTypeDefinition()
      .name(typeName)
      .enumValueDefinitions(enumValueDefinitions)
      .build();
    return GraphQLEnumType.newEnum()
      .name(typeName)
      .values(graphQLEnumValueDefinitions)
      .definition(enumTypeDefinition)
      .build();
  }

  private GraphQLEnumType generateTableOnConflictConstraintEnumType(TableConfig tableConfig) {
    TableMetadata tableMetadata = tableConfig.getTableMetadata();
    String typeName = String.format(GraphQLConstants.ON_CONFLICT_ARG_CONSTRAINT_TYPE_NAME_FORMAT, tableMetadata.getName());
    List<GraphQLEnumValueDefinition> graphQLEnumValueDefinitions = tableMetadata.getConstraintMetadata().stream()
      .filter(constraintMetadata -> constraintMetadata instanceof PrimaryKeyConstraint
        || constraintMetadata instanceof UniqueConstraint)
      .map(constraintMetadata -> {
        String constraintName = constraintMetadata.getName();
        EnumValueDefinition enumValueDefinition = EnumValueDefinition.newEnumValueDefinition()
          .name(constraintName).build();
        return GraphQLEnumValueDefinition.newEnumValueDefinition()
          .name(constraintName)
          .value(constraintName)
          .definition(enumValueDefinition).build();
      }).collect(Collectors.toList());

    List<EnumValueDefinition> enumValueDefinitions = graphQLEnumValueDefinitions.stream()
      .map(GraphQLEnumValueDefinition::getDefinition).collect(Collectors.toList());
    EnumTypeDefinition enumTypeDefinition = EnumTypeDefinition.newEnumTypeDefinition()
      .name(typeName).enumValueDefinitions(enumValueDefinitions).build();
    return GraphQLEnumType.newEnum()
      .name(typeName)
      .values(graphQLEnumValueDefinitions)
      .definition(enumTypeDefinition)
      .build();
  }

  private GraphQLInputObjectType generateTableInsertOnConflictType(String tableName) {
    String typeName = String.format(GraphQLConstants.INSERT_ON_CONFLICT_ARG_TYPE_NAME_FORMAT, tableName);
    GraphQLInputObjectType.Builder builder = GraphQLInputObjectType.newInputObject().name(typeName);
    Builder inputObjectTypeBuilder = InputObjectTypeDefinition.newInputObjectDefinition().name(typeName);
    String constraintTypeName = String.format(GraphQLConstants.ON_CONFLICT_ARG_CONSTRAINT_TYPE_NAME_FORMAT, tableName);
    InputValueDefinition constraintValueDefinition = InputValueDefinition
      .newInputValueDefinition()
      .name(GraphQLConstants.CONSTRAINT)
      .type(new NonNullType(new TypeName(constraintTypeName)))
      .build();
    inputObjectTypeBuilder.inputValueDefinition(constraintValueDefinition);
    GraphQLInputObjectField constraintField = GraphQLInputObjectField.newInputObjectField()
      .name(GraphQLConstants.CONSTRAINT)
      .definition(constraintValueDefinition)
      .type(GraphQLNonNull.nonNull(getGraphQLEnumType(constraintTypeName)))
      .build();
    builder.field(constraintField);
    String updateColumnsTypeName = String.format(GraphQLConstants.ON_CONFLICT_ARG_UPDATE_COLUMNS_TYPE_NAME_FORMAT, tableName);
    InputValueDefinition updateColumnValueDefinition = InputValueDefinition
        .newInputValueDefinition()
        .name(GraphQLConstants.UPDATE_COLUMNS)
        .type(new NonNullType(new ListType(new NonNullType(new TypeName(updateColumnsTypeName)))))
        .build();
    inputObjectTypeBuilder.inputValueDefinition(updateColumnValueDefinition);
    GraphQLInputObjectField updateColumnsField = GraphQLInputObjectField.newInputObjectField()
        .name(GraphQLConstants.UPDATE_COLUMNS)
        .definition(updateColumnValueDefinition)
        .type(GraphQLNonNull.nonNull(GraphQLList.list(GraphQLNonNull.nonNull(getGraphQLEnumType(updateColumnsTypeName)))))
        .build();
    builder.field(updateColumnsField);
    String whereTypeName = String.format(GraphQLConstants.WHERE_TYPE_FORMAT, tableName);
    InputValueDefinition whereValueDefinition = InputValueDefinition
        .newInputValueDefinition()
        .name(GraphQLConstants.WHERE)
        .type(new TypeName(whereTypeName))
        .build();
    inputObjectTypeBuilder.inputValueDefinition(whereValueDefinition);
    GraphQLInputObjectField whereField = GraphQLInputObjectField.newInputObjectField()
        .name(GraphQLConstants.WHERE)
        .definition(whereValueDefinition)
        .type(getInputObjectTypeByName(whereTypeName))
        .build();
    builder.field(whereField);

    return builder.definition(inputObjectTypeBuilder.build()).build();
  }

  private GraphQLInputObjectType generateTableInsertInputType(TableConfig tableConfig) {
    TableMetadata tableMetadata = tableConfig.getTableMetadata();
    String typeName = String.format(GraphQLConstants.INSERT_INPUT_ARG_TYPE_NAME_FORMAT, tableMetadata.getName());
    List<GraphQLInputObjectField> graphQLInputObjectFields = tableMetadata.getColumnMetadata().stream()
      .map(GraphQLUtils::convertColumnMetadataToGraphQLInputObjectField)
      .collect(Collectors.toList());
    if (!CollectionUtils.isEmpty(tableConfig.getRelationAsSourceTable())) {
      List<GraphQLInputObjectField> relationAsSourceInputField = tableConfig.getRelationAsSourceTable().stream()
        .map(relationMetadata -> {
          GraphQLInputObjectType relationArgType = generateRelationAsSourceArgType(relationMetadata);
          graphQLInputObjectTypeByName.put(relationArgType.getName(), relationArgType);
          InputValueDefinition inputValueDefinition = InputValueDefinition
            .newInputValueDefinition()
            .name(relationMetadata.getNameInSource())
            .type(new TypeName(relationArgType.getName()))
            .build();
          return GraphQLInputObjectField.newInputObjectField()
            .name(relationMetadata.getNameInSource())
            .definition(inputValueDefinition)
            .type(relationArgType)
            .build();
        }).collect(Collectors.toList());
      graphQLInputObjectFields.addAll(relationAsSourceInputField);
    }
    List<InputValueDefinition> inputValueDefinitions = graphQLInputObjectFields
      .stream().map(GraphQLInputObjectField::getDefinition).collect(Collectors.toList());
    InputObjectTypeDefinition inputObjectTypeDefinition = InputObjectTypeDefinition
      .newInputObjectDefinition()
      .name(typeName)
      .inputValueDefinitions(inputValueDefinitions)
      .build();
    return GraphQLInputObjectType.newInputObject().name(typeName)
      .fields(graphQLInputObjectFields)
      .definition(inputObjectTypeDefinition)
      .build();
  }

  private GraphQLInputObjectType generateRelationAsSourceArgType(RelationMetadata relationMetadata) {
    boolean isObjectRelation = relationMetadata.getType() == RelationType.ONE_TO_ONE;
    String argTypeName = GraphQLUtils.getRelationInputArgName(relationMetadata);
    GraphQLInputObjectType.Builder argInputObjectBuilder = GraphQLInputObjectType.newInputObject().name(argTypeName);
    Builder inputObjectValueBuilder = InputObjectTypeDefinition.newInputObjectDefinition().name(argTypeName);
    String dataInputTypeName = String.format(GraphQLConstants.INSERT_INPUT_ARG_TYPE_NAME_FORMAT,
      relationMetadata.getTargetTable());
    Type dataValueType = isObjectRelation ? new NonNullType(new TypeName(dataInputTypeName))
      : new NonNullType(new ListType(new NonNullType(new TypeName(dataInputTypeName))));
    InputValueDefinition dataValueDefinition = InputValueDefinition
      .newInputValueDefinition()
      .name(GraphQLConstants.DATA)
      .type(dataValueType)
      .build();
    inputObjectValueBuilder.inputValueDefinition(dataValueDefinition);
    GraphQLInputType dataFieldInputType = isObjectRelation
      ? GraphQLNonNull.nonNull(GraphQLTypeReference.typeRef(dataInputTypeName))
      : GraphQLNonNull.nonNull(GraphQLList.list(GraphQLNonNull.nonNull(GraphQLTypeReference.typeRef(dataInputTypeName))));
    GraphQLInputObjectField dataField = GraphQLInputObjectField.newInputObjectField()
      .name(GraphQLConstants.DATA)
      .definition(dataValueDefinition)
      .type(dataFieldInputType)
      .build();
    argInputObjectBuilder.field(dataField);
    String onConflictTypeName = String.format(
      GraphQLConstants.INSERT_ON_CONFLICT_ARG_TYPE_NAME_FORMAT, relationMetadata.getTargetTable());
    GraphQLInputType onConflictType = getInputObjectTypeByName(onConflictTypeName);
    InputValueDefinition onConflictValueDefinition = InputValueDefinition
      .newInputValueDefinition()
      .name(GraphQLConstants.ON_CONFLICT)
      .type(new TypeName(onConflictTypeName))
      .build();
    inputObjectValueBuilder.inputValueDefinition(onConflictValueDefinition);
    GraphQLInputObjectField onConflictField = GraphQLInputObjectField.newInputObjectField()
      .name(GraphQLConstants.ON_CONFLICT)
      .definition(onConflictValueDefinition)
      .type(onConflictType)
      .build();
    argInputObjectBuilder.field(onConflictField);
    return argInputObjectBuilder.definition(inputObjectValueBuilder.build()).build();
  }

  private GraphQLObjectType generateTableMutationResponseType(TableConfig tableConfig) {
    GraphQLObjectType.Builder responseObjectTypeBuilder = GraphQLObjectType.newObject();
    String tableName = tableConfig.getTableMetadata().getName();
    String mutationResponseTypeName = String.format(
      GraphQLConstants.MUTATION_RESPONSE_TYPE_NAME_FORMAT, tableName);
    responseObjectTypeBuilder.name(mutationResponseTypeName);
    FieldDefinition affectedRowsFieldDefinition = FieldDefinition
      .newFieldDefinition()
      .name(GraphQLConstants.AFFECTED_ROWS)
      .type(new NonNullType(new TypeName(GraphQLConstants.SCALAR_TYPE_INTEGER_NAME)))
      .build();
    FieldCoordinates affectedRowsCoordinates = FieldCoordinates
      .coordinates(mutationResponseTypeName, GraphQLConstants.AFFECTED_ROWS);
    codeRegistryBuilder.dataFetcher(affectedRowsCoordinates, DefaultJsonFieldDataFetcher.fetchingReturning(false));
    GraphQLFieldDefinition affectedRowsGraphQLDefinition = GraphQLFieldDefinition.newFieldDefinition()
      .name(GraphQLConstants.AFFECTED_ROWS)
      .definition(affectedRowsFieldDefinition)
      .type(GraphQLNonNull.nonNull(Scalars.GraphQLInt)).build();
    FieldDefinition returningFieldDefinition = FieldDefinition
      .newFieldDefinition()
      .name(GraphQLConstants.RETURNING)
      .type(new NonNullType(new ListType(new NonNullType(new TypeName(tableName)))))
      .build();
    FieldCoordinates returningCoordinates = FieldCoordinates.coordinates(mutationResponseTypeName, GraphQLConstants.RETURNING);
    codeRegistryBuilder.dataFetcher(returningCoordinates, DefaultJsonFieldDataFetcher.fetchingReturning(true));
    GraphQLFieldDefinition returningFieldGraphQLDefinition = GraphQLFieldDefinition.newFieldDefinition()
      .name(GraphQLConstants.RETURNING)
      .definition(returningFieldDefinition)
      .type(GraphQLNonNull.nonNull(GraphQLList.list(GraphQLNonNull.nonNull(GraphQLTypeReference.typeRef(tableName)))))
      .build();
    responseObjectTypeBuilder.fields(List.of(affectedRowsGraphQLDefinition, returningFieldGraphQLDefinition));
    ObjectTypeDefinition objectTypeDefinition = ObjectTypeDefinition.newObjectTypeDefinition()
      .name(mutationResponseTypeName)
      .fieldDefinition(returningFieldDefinition)
      .fieldDefinition(affectedRowsFieldDefinition)
      .build();
    responseObjectTypeBuilder.definition(objectTypeDefinition);
    return responseObjectTypeBuilder.build();
  }

  public List<GraphQLArgument> generateTableQueryArguments(String tableName) {
    String whereInputTypeName = String.format(GraphQLConstants.WHERE_TYPE_FORMAT, tableName);
    GraphQLArgument whereArgument = GraphQLArgument.newArgument()
        .name(GraphQLConstants.WHERE)
        .type(GraphQLTypeReference.typeRef(whereInputTypeName))
        .definition(generateInputValueDefinition(whereInputTypeName, new TypeName(whereInputTypeName)))
        .build();
    String orderByInputTypeName = String.format(GraphQLConstants.ORDER_BY_TYPE_NAME_FORMAT, tableName);
    GraphQLArgument orderByArgument = GraphQLArgument.newArgument()
        .name(GraphQLConstants.ORDER_BY)
        .type(GraphQLList.list(GraphQLNonNull.nonNull(GraphQLTypeReference.typeRef(orderByInputTypeName))))
        .definition(generateInputValueDefinition(orderByInputTypeName,
            new ListType(new NonNullType(new TypeName(orderByInputTypeName)))))
        .build();
    String distinctOnTypeName = String.format(GraphQLConstants.QUERY_DISTINCT_TYPE_NAME_FORMAT, tableName);
    GraphQLArgument distinctOnArgument = GraphQLArgument.newArgument()
        .name(GraphQLConstants.DISTINCT_ON)
        .type(GraphQLList.list(GraphQLNonNull.nonNull(GraphQLTypeReference.typeRef(distinctOnTypeName))))
        .definition(generateInputValueDefinition(distinctOnTypeName,
            new ListType(new NonNullType(new TypeName(distinctOnTypeName)))))
        .build();
    TypeName intType = GraphQLUtils.mapColumnTypeToTypeName(ColumnType.INTEGER);
    GraphQLArgument offsetArgument = GraphQLArgument.newArgument()
        .name(GraphQLConstants.OFFSET)
        .type(GraphQLUtils.mapColumnToGraphQLScalarType(ColumnType.INTEGER))
        .definition(generateInputValueDefinition(GraphQLConstants.OFFSET, intType))
        .build();
    GraphQLArgument limitArgument = GraphQLArgument.newArgument()
        .name(GraphQLConstants.LIMIT)
        .type(GraphQLUtils.mapColumnToGraphQLScalarType(ColumnType.INTEGER))
        .definition(generateInputValueDefinition(GraphQLConstants.LIMIT, intType))
        .build();
    return List.of(whereArgument, orderByArgument, distinctOnArgument, limitArgument, offsetArgument);
  }

  private GraphQLInputObjectType generateWhereConditionType(TableMetadata tableMetadata) {
    String whereInputTypeName = String.format(GraphQLConstants.WHERE_TYPE_FORMAT, tableMetadata.getName());
    List<GraphQLInputObjectField> inputFields = generateWhereInputObjectFields(tableMetadata, whereInputTypeName);
    List<InputValueDefinition> inputValueDefinitions = generateWhereInputValueDefinitions(tableMetadata);
    InputObjectTypeDefinition definition = InputObjectTypeDefinition
      .newInputObjectDefinition().name(GraphQLConstants.WHERE)
      .inputValueDefinitions(inputValueDefinitions)
      .build();
    return GraphQLInputObjectType.newInputObject()
      .name(whereInputTypeName)
      .fields(inputFields)
      .definition(definition)
      .build();
  }

  private GraphQLInputObjectType generateOrderByConditionType(TableMetadata tableMetadata) {
    GraphQLEnumType orderByEnumType = getOrCreateOrderByEnumType();
    List<GraphQLInputObjectField> orderByInputObjectFields = tableMetadata
      .getColumnMetadata().stream().map(columnMetadata -> {
        InputValueDefinition orderByInputValueDefinition = InputValueDefinition
          .newInputValueDefinition()
          .name(columnMetadata.getName())
          .type(new TypeName(GraphQLConstants.ORDER_BY)).build();
        return GraphQLInputObjectField.newInputObjectField()
          .name(columnMetadata.getName())
          .type(orderByEnumType).definition(orderByInputValueDefinition)
          .build();
      }).collect(Collectors.toList());
    List<InputValueDefinition> inputValueDefinitions = orderByInputObjectFields.stream()
        .map(GraphQLInputObjectField::getDefinition).collect(Collectors.toList());
    String orderByInputTypeName = String.format(GraphQLConstants.ORDER_BY_TYPE_NAME_FORMAT, tableMetadata.getName());
    InputObjectTypeDefinition definition = InputObjectTypeDefinition
      .newInputObjectDefinition().name(orderByInputTypeName)
      .inputValueDefinitions(inputValueDefinitions)
      .build();
    return GraphQLInputObjectType.newInputObject()
      .name(orderByInputTypeName)
      .fields(orderByInputObjectFields)
      .definition(definition)
      .build();
  }

  private List<InputValueDefinition> generateWhereInputValueDefinitions(TableMetadata tableMetadata) {
    return tableMetadata.getColumnMetadata().stream().map(columnMetadata -> {
      return InputValueDefinition.newInputValueDefinition()
        .name(columnMetadata.getName())
        .type(GraphQLUtils.mapColumnTypeToTypeName(columnMetadata.getType()))
        .build();
    }).collect(Collectors.toList());
  }

  private List<GraphQLInputObjectField> generateWhereInputObjectFields(TableMetadata tableMetadata, String whereInputTypeName) {
    List<GraphQLInputObjectField> fields = tableMetadata.getColumnMetadata().stream().map(columnMetadata -> {
      GraphQLInputObjectType inputObjectType = getComparsionObjectType(columnMetadata.getType());
      InputValueDefinition definition = InputValueDefinition.newInputValueDefinition()
        .name(columnMetadata.getName()).type(new TypeName(inputObjectType.getDefinition().getName())).build();
      return GraphQLInputObjectField.newInputObjectField()
        .name(columnMetadata.getName())
        .type(inputObjectType)
        .definition(definition)
        .build();
    }).collect(Collectors.toList());
    GraphQLTypeReference whereInputTypeReference = GraphQLTypeReference.typeRef(whereInputTypeName);
    List<GraphQLInputObjectField> logicOperatorFields = generateWhereLogicOperatorFields(whereInputTypeReference);
    fields.addAll(logicOperatorFields);
    return fields;
  }

  private List<GraphQLInputObjectField> generateWhereLogicOperatorFields(GraphQLTypeReference whereInputTypeReference) {
    InputValueDefinition andDefinition = InputValueDefinition.newInputValueDefinition()
      .name(GraphQLConstants._AND)
      .type(new ListType(new TypeName(whereInputTypeReference.getName())))
      .build();
    GraphQLInputObjectField andInputField = GraphQLInputObjectField.newInputObjectField()
      .name(GraphQLConstants._AND)
      .type(GraphQLList.list(whereInputTypeReference))
      .definition(andDefinition)
      .build();
    InputValueDefinition orDefinition = InputValueDefinition.newInputValueDefinition()
      .name(GraphQLConstants._OR)
      .type(new ListType(new TypeName(whereInputTypeReference.getName())))
      .build();
    GraphQLInputObjectField orInputField = GraphQLInputObjectField.newInputObjectField()
      .name(GraphQLConstants._OR)
      .type(GraphQLList.list(whereInputTypeReference))
      .definition(orDefinition)
      .build();
    InputValueDefinition notDefinition = InputValueDefinition.newInputValueDefinition()
      .name(GraphQLConstants._NOT)
      .type(new TypeName(whereInputTypeReference.getName()))
      .build();
    GraphQLInputObjectField notInputField = GraphQLInputObjectField.newInputObjectField()
      .name(GraphQLConstants._NOT)
      .type(whereInputTypeReference)
      .definition(notDefinition)
      .build();
    return List.of(andInputField, orInputField, notInputField);
  }

  private GraphQLObjectType generateTableAggregateFieldObjectType(TableMetadata tableMetadata) {
    String aggregateFieldsTypeName = String.format(GraphQLConstants.AGGREGATE_FIELD_TYPE_NAME_FORMAT, tableMetadata.getName());
    List<GraphQLFieldDefinition> tableAggregateAggregateFieldFieldsDefinition =
      generateAggregateFieldSubFieldsDefinition(tableMetadata, aggregateFieldsTypeName);
    List<FieldDefinition> fieldDefinitions = tableAggregateAggregateFieldFieldsDefinition.stream()
        .map(GraphQLFieldDefinition::getDefinition).collect(Collectors.toList());
    ObjectTypeDefinition definition = ObjectTypeDefinition.newObjectTypeDefinition()
      .name(aggregateFieldsTypeName)
      .fieldDefinitions(fieldDefinitions)
      .build();
    return GraphQLObjectType.newObject()
      .name(aggregateFieldsTypeName)
      .fields(tableAggregateAggregateFieldFieldsDefinition)
      .definition(definition)
      .build();
  }

  private GraphQLObjectType generateTableAggregateObjectType(TableConfig tableConfig) {
    List<GraphQLFieldDefinition> fieldDefinitions = generateTableAggregateFieldsDefinition(tableConfig);
    String aggregateTypeName = String.format(GraphQLConstants.AGGREGATE_QUERY_TYPE_NAME_FORMAT,
      tableConfig.getTableMetadata().getName());
    ObjectTypeDefinition definition = ObjectTypeDefinition.newObjectTypeDefinition()
      .name(aggregateTypeName)
      .fieldDefinitions(createAggregateObjectFieldDefinitions(tableConfig.getTableMetadata()))
      .build();
    return GraphQLObjectType.newObject()
      .name(aggregateTypeName)
      .fields(fieldDefinitions)
      .definition(definition)
      .build();
  }

  private List<GraphQLFieldDefinition> generateAggregateFieldSubFieldsDefinition(
      TableMetadata tableMetadata, String parentTypeName) {
    GraphQLObjectType avgType = generateTableAggregateType(tableMetadata, AggregateType.AVG);
    FieldDefinition avgDefinition = FieldDefinition.newFieldDefinition()
      .name(AggregateType.AVG.getGraphQLFieldName())
      .type(new TypeName(avgType.getName()))
      .build();

    GraphQLFieldDefinition avgFieldDefinition = GraphQLFieldDefinition.newFieldDefinition()
      .name(AggregateType.AVG.getGraphQLFieldName())
      .type(avgType)
      .definition(avgDefinition)
      .build();
    GraphQLObjectType sumType = generateTableAggregateType(tableMetadata, AggregateType.SUM);
    FieldDefinition sumDefinition = FieldDefinition.newFieldDefinition()
      .name(AggregateType.SUM.getGraphQLFieldName())
      .type(new TypeName(sumType.getName()))
      .build();
    GraphQLFieldDefinition sumFieldDefinition = GraphQLFieldDefinition.newFieldDefinition()
      .name(AggregateType.SUM.getGraphQLFieldName())
      .type(sumType)
      .definition(sumDefinition)
      .build();
    GraphQLObjectType maxType = generateTableAggregateType(tableMetadata, AggregateType.MAX);
    FieldDefinition maxDefinition = FieldDefinition.newFieldDefinition()
      .name(AggregateType.MAX.getGraphQLFieldName())
      .type(new TypeName(maxType.getName()))
      .build();
    GraphQLFieldDefinition maxFieldDefinition = GraphQLFieldDefinition.newFieldDefinition()
      .name(AggregateType.MAX.getGraphQLFieldName())
      .type(maxType)
      .definition(maxDefinition)
      .build();
    GraphQLObjectType minType = generateTableAggregateType(tableMetadata, AggregateType.MIN);
    FieldDefinition minDefinition = FieldDefinition.newFieldDefinition()
      .name(AggregateType.MIN.getGraphQLFieldName())
      .type(new TypeName(minType.getName()))
      .build();
    GraphQLFieldDefinition minFieldDefinition = GraphQLFieldDefinition.newFieldDefinition()
      .name(AggregateType.MIN.getGraphQLFieldName())
      .type(minType)
      .definition(minDefinition)
      .build();
    Stream.of(AggregateType.values())
      .filter(type -> type != AggregateType.COUNT)
      .forEach(type -> {
        FieldCoordinates coordinates = FieldCoordinates.coordinates(parentTypeName, type.getGraphQLFieldName());
        codeRegistryBuilder.dataFetcher(coordinates, new RootObjectDataFetcher(objectMapper));
      });
    FieldDefinition countDefinition = FieldDefinition.newFieldDefinition()
      .name(AggregateType.COUNT.getGraphQLFieldName())
      .type(GraphQLUtils.mapColumnTypeToTypeName(ColumnType.INTEGER))
      .inputValueDefinitions(generateAggregateCountInputValueDefinitions(tableMetadata))
      .build();
    DataFetcher<CompletableFuture<?>> countDataFetcher = dataFetcherGenerator
      .generateAggregateFieldDataFetcher(tableMetadata, ColumnType.INTEGER, "", AggregateType.COUNT);
    FieldCoordinates countCoordinates = FieldCoordinates
      .coordinates(parentTypeName, AggregateType.COUNT.getGraphQLFieldName());
    codeRegistryBuilder.dataFetcher(countCoordinates, countDataFetcher);
    List<GraphQLArgument> arguments = generateAggregateCountArguments(tableMetadata);
    GraphQLFieldDefinition countFieldDefinition = GraphQLFieldDefinition.newFieldDefinition()
      .name(AggregateType.COUNT.getGraphQLFieldName())
      .type(Scalars.GraphQLInt)
      .arguments(arguments)
      .definition(countDefinition)
      .build();

    return List.of(avgFieldDefinition, sumFieldDefinition, maxFieldDefinition, minFieldDefinition, countFieldDefinition);
  }

  private List<GraphQLArgument> generateAggregateCountArguments(TableMetadata tableMetadata) {
    String columnsTypeName = String.format(GraphQLConstants.QUERY_DISTINCT_TYPE_NAME_FORMAT, tableMetadata.getName());
    InputValueDefinition columnsInputValueDefinition = InputValueDefinition.newInputValueDefinition()
      .name(GraphQLConstants.COLUMNS)
      .type(new TypeName(columnsTypeName))
      .build();
    GraphQLArgument columnsArgument = GraphQLArgument.newArgument()
      .name(GraphQLConstants.COLUMNS)
      .type(GraphQLList.list(GraphQLNonNull.nonNull(GraphQLTypeReference.typeRef(columnsTypeName))))
      .definition(columnsInputValueDefinition)
      .build();
    InputValueDefinition distinctInputValueDefinition = InputValueDefinition.newInputValueDefinition()
        .name(GraphQLConstants.DISTINCT)
        .type(GraphQLUtils.mapColumnTypeToTypeName(ColumnType.BOOLEAN))
        .build();
    GraphQLArgument distinctArgument = GraphQLArgument.newArgument()
        .name(GraphQLConstants.DISTINCT)
        .type(Scalars.GraphQLBoolean)
        .definition(distinctInputValueDefinition)
        .build();
    return List.of(columnsArgument, distinctArgument);
  }

  private List<InputValueDefinition> generateAggregateCountInputValueDefinitions(TableMetadata tableMetadata) {
    String columnsTypeName = String.format(GraphQLConstants.QUERY_DISTINCT_TYPE_NAME_FORMAT, tableMetadata.getName());
    InputValueDefinition columnsInputValueDefinition = InputValueDefinition.newInputValueDefinition()
      .name(GraphQLConstants.COLUMNS)
      .type(new TypeName(columnsTypeName))
      .build();
    InputValueDefinition distinctInputValueDefinition = InputValueDefinition.newInputValueDefinition()
      .name(GraphQLConstants.DISTINCT)
      .type(GraphQLUtils.mapColumnTypeToTypeName(ColumnType.BOOLEAN))
      .build();
    return List.of(columnsInputValueDefinition, distinctInputValueDefinition);
  }

  private GraphQLObjectType generateTableAggregateType(TableMetadata tableMetadata, AggregateType type) {
    if (type == AggregateType.COUNT) {
      throw new IllegalStateException("count is not object type");
    }
    List<FieldDefinition> aggregateFieldDefinitions = tableMetadata.getColumnMetadata().stream()
      .filter(type::compatibleWithColumn)
      .map(this::mapColumnMetadataToFieldDefinition)
      .collect(Collectors.toList());
    String typeName = String.format(type.getFieldTypeNameFormat(), tableMetadata.getName());
    List<GraphQLFieldDefinition> fieldDefinitions = tableMetadata.getColumnMetadata().stream()
      .filter(type::compatibleWithColumn)
      .map(metadata -> {
        FieldDefinition fieldDefinition = mapColumnMetadataToFieldDefinition(metadata);
        DataFetcher dataFetcher = dataFetcherGenerator
          .generateAggregateFieldDataFetcher(tableMetadata, metadata.getType(), metadata.getName(), type);
        FieldCoordinates coordinates = FieldCoordinates.coordinates(typeName, metadata.getName());
        codeRegistryBuilder.dataFetcher(coordinates, dataFetcher);
        GraphQLScalarType targetScalar = type.getGraphQLScalarType(metadata);
        return GraphQLFieldDefinition.newFieldDefinition()
          .name(metadata.getName())
          .type(targetScalar)
          .definition(fieldDefinition)
          .build();
      }).collect(Collectors.toList());
    ObjectTypeDefinition definition = ObjectTypeDefinition.newObjectTypeDefinition()
      .name(typeName)
      .fieldDefinitions(aggregateFieldDefinitions)
      .build();
    return GraphQLObjectType.newObject()
      .name(typeName)
      .fields(fieldDefinitions)
      .definition(definition)
      .build();
  }

  private List<GraphQLFieldDefinition> generateTableAggregateFieldsDefinition(TableConfig tableConfig) {
    TableMetadata tableMetadata = tableConfig.getTableMetadata();
    String aggregateFieldsTypeName = String.format(GraphQLConstants.AGGREGATE_FIELD_TYPE_NAME_FORMAT, tableMetadata.getName());
    GraphQLObjectType tableObjectType = graphQLObjectTypeByName.get(tableMetadata.getName());
    GraphQLObjectType aggregateFieldsType = graphQLObjectTypeByName.get(aggregateFieldsTypeName);
    String parentTypeName = String.format(GraphQLConstants.AGGREGATE_QUERY_TYPE_NAME_FORMAT, tableMetadata.getName());
    FieldCoordinates nodesCoordinates = FieldCoordinates.coordinates(parentTypeName, GraphQLConstants.NODES);
    codeRegistryBuilder.dataFetcher(nodesCoordinates, dataFetcherGenerator.generateQueryListDataFetcher(tableConfig));
    GraphQLFieldDefinition nodesFieldDefinition = GraphQLFieldDefinition.newFieldDefinition()
      .name(GraphQLConstants.NODES)
      .definition(generateAggregateNodesFieldDefinition(tableMetadata.getName()))
      .type(GraphQLNonNull.nonNull(GraphQLList.list(GraphQLNonNull.nonNull(tableObjectType))))
      .build();
    FieldCoordinates fieldsCoordinates = FieldCoordinates.coordinates(parentTypeName, GraphQLConstants.AGGREGATE);
    codeRegistryBuilder.dataFetcher(fieldsCoordinates, new RootObjectDataFetcher(objectMapper));

    GraphQLFieldDefinition aggregateFieldDefinition = GraphQLFieldDefinition.newFieldDefinition()
      .name(GraphQLConstants.AGGREGATE)
      .definition(generateAggregateAggregateFieldDefinition(aggregateFieldsTypeName))
      .type(aggregateFieldsType)
      .build();
    return List.of(aggregateFieldDefinition, nodesFieldDefinition);
  }

  private FieldDefinition generateAggregateNodesFieldDefinition(String tableName) {
    return FieldDefinition.newFieldDefinition()
      .name(GraphQLConstants.NODES)
      .type(generateListQueryType(tableName))
      .build();
  }

  private FieldDefinition generateAggregateAggregateFieldDefinition(String aggregateFieldsTypeName) {
    return FieldDefinition.newFieldDefinition()
      .name(GraphQLConstants.AGGREGATE)
      .type(new TypeName(aggregateFieldsTypeName))
      .build();
  }

  public List<FieldDefinition> createAggregateObjectFieldDefinitions(TableMetadata tableMetadata) {
    FieldDefinition aggregateNodesDefinitions = generateAggregateNodesFieldDefinition(tableMetadata.getName());
    String aggregateFieldsTypeName = String.format(GraphQLConstants.AGGREGATE_FIELD_TYPE_NAME_FORMAT, tableMetadata.getName());
    FieldDefinition aggregateFieldDefinitions = generateAggregateAggregateFieldDefinition(aggregateFieldsTypeName);
    return List.of(aggregateFieldDefinitions, aggregateNodesDefinitions);
  }

  private GraphQLObjectType generateTableObjectType(TableConfig tableConfig) {
    List<GraphQLFieldDefinition> fieldDefinitions = generateColumnDefinition(tableConfig);

    return GraphQLObjectType.newObject()
      .description(tableConfig.getTableMetadata().getDescription())
      .name(tableConfig.getTableMetadata().getName())
      .fields(fieldDefinitions)
      .build();
  }

  private List<GraphQLFieldDefinition> generateColumnDefinition(TableConfig tableConfig) {
    List<GraphQLFieldDefinition> fieldDefinitions = tableConfig.getTableMetadata().getColumnMetadata()
      .stream().map(columnMetadata -> {
        FieldCoordinates columnCoordinates = FieldCoordinates
          .coordinates(tableConfig.getTableMetadata().getName(), columnMetadata.getName());
        codeRegistryBuilder.dataFetcher(columnCoordinates, DefaultJsonFieldDataFetcher.fetching(false));
        return GraphQLFieldDefinition.newFieldDefinition()
        .name(columnMetadata.getName())
        .definition(mapColumnMetadataToFieldDefinition(columnMetadata))
        .type(GraphQLUtils.mapColumnToGraphQLScalarType(columnMetadata.getType()))
        .build();
      }).collect(Collectors.toList());
    List<GraphQLFieldDefinition> relationFieldsDefinition = generateRelationFieldDefinitions(tableConfig);
    fieldDefinitions.addAll(relationFieldsDefinition);
    return fieldDefinitions;
  }

  private List<GraphQLFieldDefinition> generateRelationFieldDefinitions(TableConfig tableConfig) {
    return Stream.concat(tableConfig.getRelationAsSourceTable().stream(),
      tableConfig.getRelationAsTargetTable().stream()).map(relationMetadata -> {
        return generateRelationFieldDefinition(tableConfig.getTableMetadata(), relationMetadata);
      }).flatMap(List::stream).collect(Collectors.toList());
  }

  private List<GraphQLFieldDefinition> generateRelationFieldDefinition(
      TableMetadata tableMetadata, RelationMetadata relationMetadata) {
    boolean asSourceTable = relationMetadata.getSourceTable().equals(tableMetadata.getName());
    String tableName = asSourceTable ? relationMetadata.getTargetTable() : relationMetadata.getSourceTable();
    GraphQLOutputType outputType = GraphQLUtils.getRelationFieldGraphQLOutputType(
        GraphQLTypeReference.typeRef(tableName), relationMetadata.getType(), asSourceTable);
    Type fieldType = GraphQLUtils.getRelationFieldType(tableName, relationMetadata.getType(), asSourceTable);
    String fieldName = asSourceTable ? relationMetadata.getNameInSource() : relationMetadata.getNameInTarget();
    FieldDefinition definition = FieldDefinition.newFieldDefinition()
        .name(fieldName)
        .type(fieldType).build();
    DataFetcher<CompletableFuture<?>> dataFetcher = dataFetcherGenerator
      .generateRelationFieldDataFetcher(tableMetadata, relationMetadata);
    FieldCoordinates relationFieldCoordinates = FieldCoordinates
      .coordinates(tableMetadata.getName(), fieldName);
    codeRegistryBuilder.dataFetcher(relationFieldCoordinates, dataFetcher);
    GraphQLFieldDefinition.Builder builder = GraphQLFieldDefinition.newFieldDefinition()
      .name(fieldName)
      .type(outputType)
      .definition(definition);
    if (fieldType instanceof ListType) {
      List<GraphQLArgument> arguments = generateTableQueryArguments(tableName);
      builder.arguments(arguments);
      String relationAggregateTypeName = String.format(GraphQLConstants.AGGREGATE_QUERY_TYPE_NAME_FORMAT, tableName);
      FieldDefinition relationAggregateDefinition = FieldDefinition.newFieldDefinition()
        .name(relationAggregateTypeName)
        .type(new NonNullType(new TypeName(relationAggregateTypeName))).build();
      FieldCoordinates aggregateFieldCoordinates = FieldCoordinates
        .coordinates(tableMetadata.getName(), relationAggregateTypeName);
      codeRegistryBuilder.dataFetcher(aggregateFieldCoordinates, new RootObjectDataFetcher(objectMapper));
      GraphQLFieldDefinition aggregateFieldDefinition = GraphQLFieldDefinition.newFieldDefinition()
        .name(relationAggregateTypeName)
        .type(GraphQLNonNull.nonNull(GraphQLTypeReference.typeRef(relationAggregateTypeName)))
        .arguments(arguments)
        .definition(relationAggregateDefinition)
        .build();
      return List.of(builder.build(), aggregateFieldDefinition);
    } else {
      return List.of(builder.build());
    }
  }

  private FieldDefinition mapColumnMetadataToFieldDefinition(ColumnMetadata columnMetadata) {
    return FieldDefinition.newFieldDefinition().name(columnMetadata.getName())
      .type(mapColumnTypeToGraphQLType(columnMetadata))
      .build();
  }

  private Type<? extends Type> mapColumnTypeToGraphQLType(ColumnMetadata columnMetadata) {
    ColumnType columnType = columnMetadata.getType();
    boolean required = columnMetadata.isRequired() || columnMetadata.isPrimaryKey();
    TypeName typeName = GraphQLUtils.mapColumnTypeToTypeName(columnType);
    return required ? new NonNullType(typeName) : typeName;
  }

  public GraphQLInputObjectType getComparsionObjectType(ColumnType columnType) {
    return inputObjectTypeByColumnType.get(columnType);
  }

  private void initColumnInputObjectType() {
    Stream.of(ColumnType.values()).forEach(columnType -> {
      GraphQLInputObjectType inputObjectType = createComparsionInputObjectType(columnType);
      inputObjectTypeByColumnType.put(columnType, inputObjectType);
    });
  }

  private GraphQLInputObjectType createComparsionInputObjectType(ColumnType columnType) {
    String typeName = String.format(GraphQLConstants.COLUMN_CONDITION_INPUT_TYPE_NAME_FORMAT,
      Utils.toLowerUnderscoreStr(columnType.name()));
    List<InputValueDefinition> inputValueDefinitions = generateCommonInputValueDefinitions(columnType);
    List<InputValueDefinition> specificValueDefinitions = generateSpecificInputValueDefinitions(columnType);
    Builder inputValueDefinitionsBuilder = InputObjectTypeDefinition.newInputObjectDefinition()
      .name(typeName).inputValueDefinitions(inputValueDefinitions);
    specificValueDefinitions.stream()
      .forEach(valueDefinition -> inputValueDefinitionsBuilder.inputValueDefinition(valueDefinition));
    InputObjectTypeDefinition typeDefinition = inputValueDefinitionsBuilder.build();
    List<GraphQLInputObjectField> fields = Stream.concat(inputValueDefinitions.stream(), specificValueDefinitions.stream())
      .map(inputValueDefinition -> {
        GraphQLInputType inputType = parseInputTypeRecursionly(inputValueDefinition.getType(),
          GraphQLUtils.mapColumnToGraphQLScalarType(columnType));
        return GraphQLInputObjectField.newInputObjectField()
          .name(inputValueDefinition.getName())
          .definition(inputValueDefinition)
          .type(inputType).build();
      }).collect(Collectors.toList());
    return GraphQLInputObjectType.newInputObject().name(typeName)
      .fields(fields).definition(typeDefinition).build();
  }

  private GraphQLInputType parseInputTypeRecursionly(Type type, GraphQLInputType wrappedInputType) {
    if (type instanceof TypeName) {
      return wrappedInputType;
    } else if (type instanceof ListType) {
      Type wrappedType = ((ListType) type).getType();
      return GraphQLList.list(parseInputTypeRecursionly(wrappedType, wrappedInputType));
    } else if (type instanceof NonNullType) {
      Type wrappedType = ((NonNullType) type).getType();
      return GraphQLNonNull.nonNull(parseInputTypeRecursionly(wrappedType, wrappedInputType));
    } else {
      throw new IllegalStateException();
    }
  }

  private List<InputValueDefinition> generateCommonInputValueDefinitions(ColumnType columnType) {
    TypeName typeName = GraphQLUtils.mapColumnTypeToTypeName(columnType);
    List<InputValueDefinition> resultList = new ArrayList<>();
    InputValueDefinition eqInputDefinition = InputValueDefinition.newInputValueDefinition()
      .name(GraphQLConstants.COMPARSION_EQ).type(typeName).build();
    resultList.add(eqInputDefinition);
    InputValueDefinition gtInputDefinition = InputValueDefinition.newInputValueDefinition()
      .name(GraphQLConstants.COMPARSION_GT).type(typeName).build();
    resultList.add(gtInputDefinition);
    InputValueDefinition gteInputDefinition = InputValueDefinition.newInputValueDefinition()
      .name(GraphQLConstants.COMPARSION_GTE).type(typeName).build();
    resultList.add(gteInputDefinition);
    InputValueDefinition ltInputDefinition = InputValueDefinition.newInputValueDefinition()
      .name(GraphQLConstants.COMPARSION_LT).type(typeName).build();
    resultList.add(ltInputDefinition);
    InputValueDefinition lteInputDefinition = InputValueDefinition.newInputValueDefinition()
      .name(GraphQLConstants.COMPARSION_LTE).type(typeName).build();
    resultList.add(lteInputDefinition);
    InputValueDefinition neqInputDefinition = InputValueDefinition.newInputValueDefinition()
      .name(GraphQLConstants.COMPARSION_NEQ).type(typeName).build();
    resultList.add(neqInputDefinition);
    TypeName booleanType = GraphQLUtils.mapColumnTypeToTypeName(ColumnType.BOOLEAN);
    InputValueDefinition isNullInputDefinition = InputValueDefinition.newInputValueDefinition()
      .name(GraphQLConstants.COMPARSION_IS_NULL).type(booleanType).build();
    resultList.add(isNullInputDefinition);
    ListType listNonNullType = new ListType(new NonNullType(typeName));
    InputValueDefinition inInputDefinition = InputValueDefinition.newInputValueDefinition()
      .name(GraphQLConstants.COMPARSION_IN).type(listNonNullType).build();
    resultList.add(inInputDefinition);
    InputValueDefinition ninInputDefinition = InputValueDefinition.newInputValueDefinition()
      .name(GraphQLConstants.COMPARSION_NIN).type(listNonNullType).build();
    resultList.add(ninInputDefinition);

    return resultList;
  }

  private List<InputValueDefinition> generateSpecificInputValueDefinitions(ColumnType columnType) {
    switch (columnType) {
      case TEXT:
        return generateStringSpecificInputValueDefinition();
      case JSONB:
        return generateJsonbSpecificInputValueDefinition();
      default:
        return List.of();
    }
  }

  private List<InputValueDefinition> generateStringSpecificInputValueDefinition() {
    TypeName typeName = GraphQLUtils.mapColumnTypeToTypeName(ColumnType.TEXT);
    InputValueDefinition likeInputDefinition = InputValueDefinition.newInputValueDefinition()
      .name(GraphQLConstants.COMPARSION_LIKE).type(typeName).build();
    InputValueDefinition nlikeInputDefinition = InputValueDefinition.newInputValueDefinition()
      .name(GraphQLConstants.COMPARSION_NLIKE).type(typeName).build();
    InputValueDefinition ilikeInputDefinition = InputValueDefinition.newInputValueDefinition()
      .name(GraphQLConstants.COMPARSION_ILIKE).type(typeName).build();
    InputValueDefinition nilikeInputDefinition = InputValueDefinition.newInputValueDefinition()
      .name(GraphQLConstants.COMPARSION_NILIKE).type(typeName).build();
    InputValueDefinition similarInputDefinition = InputValueDefinition.newInputValueDefinition()
      .name(GraphQLConstants.COMPARSION_SIMILAR).type(typeName).build();
    InputValueDefinition nsimilarInputDefinition = InputValueDefinition.newInputValueDefinition()
      .name(GraphQLConstants.COMPARSION_NSIMILAR).type(typeName).build();
    return List.of(likeInputDefinition, nlikeInputDefinition, ilikeInputDefinition,
      nilikeInputDefinition, similarInputDefinition, nsimilarInputDefinition);
  }

  private List<InputValueDefinition> generateJsonbSpecificInputValueDefinition() {
    TypeName typeName = GraphQLUtils.mapColumnTypeToTypeName(ColumnType.JSONB);
    InputValueDefinition hasKeyInputDefinition = InputValueDefinition.newInputValueDefinition()
      .name(GraphQLConstants.COMPARSION_HAS_KEY).type(typeName).build();
    InputValueDefinition hasKeysAllInputDefinition = InputValueDefinition.newInputValueDefinition()
      .name(GraphQLConstants.COMPARSION_HAS_KEYS_ALL).type(typeName).build();
    InputValueDefinition hasKeysAnyInputDefinition = InputValueDefinition.newInputValueDefinition()
      .name(GraphQLConstants.COMPARSION_HAS_KEYS_ANY).type(typeName).build();
    return List.of(hasKeyInputDefinition, hasKeysAllInputDefinition, hasKeysAnyInputDefinition);
  }

  private InputValueDefinition generateInputValueDefinition(String inputName, Type inputType) {
    return InputValueDefinition.newInputValueDefinition()
        .name(inputName)
        .type(inputType)
        .build();
  }

  public GraphQLEnumType generateDistinctOnEnumType(TableMetadata tableMetadata) {
    String distinctOnTypeName = String.format(GraphQLConstants.QUERY_DISTINCT_TYPE_NAME_FORMAT, tableMetadata.getName());
    List<EnumValueDefinition> enumValueDefinitions = tableMetadata.getColumnMetadata().stream()
      .map(ColumnMetadata::getName)
      .map(columnName -> {
        return EnumValueDefinition.newEnumValueDefinition()
          .name(columnName).build();
      }).collect(Collectors.toList());
    EnumTypeDefinition enumTypeDefinition = EnumTypeDefinition.newEnumTypeDefinition()
      .name(distinctOnTypeName)
      .enumValueDefinitions(enumValueDefinitions)
      .build();
    GraphQLEnumType.Builder enumBuilder = GraphQLEnumType.newEnum()
      .name(distinctOnTypeName)
      .definition(enumTypeDefinition);
    enumValueDefinitions.stream().map(enumValueDefinition -> {
      return GraphQLEnumValueDefinition.newEnumValueDefinition()
        .name(enumValueDefinition.getName())
        .value(enumValueDefinition.getName())
        .build();
    }).forEach(graphQLEnumValueDefinition -> enumBuilder.value(graphQLEnumValueDefinition));
    GraphQLEnumType distinctOnColumnEnumType = enumBuilder.build();
    return distinctOnColumnEnumType;
  }

  public GraphQLEnumType getOrCreateOrderByEnumType() {
    if (graphQLEnumTypeByName.containsKey(GraphQLConstants.ORDER_BY)) {
      return graphQLEnumTypeByName.get(GraphQLConstants.ORDER_BY);
    }
    List<EnumValueDefinition> enumValueDefinitions = generateOrderByInputEnumValueDefinition();
    EnumTypeDefinition enumTypeDefinition = EnumTypeDefinition.newEnumTypeDefinition()
      .name(GraphQLConstants.ORDER_BY)
      .enumValueDefinitions(enumValueDefinitions)
      .build();
    GraphQLEnumType.Builder enumBuilder = GraphQLEnumType.newEnum()
      .name(GraphQLConstants.ORDER_BY)
      .definition(enumTypeDefinition);
    enumValueDefinitions.stream().map(enumValueDefinition -> {
      return GraphQLEnumValueDefinition.newEnumValueDefinition()
        .name(enumValueDefinition.getName())
        .value(enumValueDefinition.getName())
        .build();
    }).forEach(graphQLEnumValueDefinition -> enumBuilder.value(graphQLEnumValueDefinition));
    GraphQLEnumType orderByEnumType = enumBuilder.build();
    graphQLEnumTypeByName.put(GraphQLConstants.ORDER_BY, orderByEnumType);
    return orderByEnumType;
  }

  private List<EnumValueDefinition> generateOrderByInputEnumValueDefinition() {
    EnumValueDefinition ascDefinition = EnumValueDefinition
      .newEnumValueDefinition().name(GraphQLConstants.ORDER_BY_ASC).build();
    EnumValueDefinition ascNullsFirstDefinition = EnumValueDefinition
      .newEnumValueDefinition().name(GraphQLConstants.ORDER_BY_ASC_NULLS_FIRST).build();
    EnumValueDefinition ascNullsLastDefinition = EnumValueDefinition
      .newEnumValueDefinition().name(GraphQLConstants.ORDER_BY_ASC_NULLS_LAST).build();
    EnumValueDefinition descDefinition = EnumValueDefinition
      .newEnumValueDefinition().name(GraphQLConstants.ORDER_BY_DESC).build();
    EnumValueDefinition descNullsFirstDefinition = EnumValueDefinition
      .newEnumValueDefinition().name(GraphQLConstants.ORDER_BY_DESC_NULLS_FIRST).build();
    EnumValueDefinition descNullsLastDefinition = EnumValueDefinition
      .newEnumValueDefinition().name(GraphQLConstants.ORDER_BY_DESC_NULLS_LAST).build();
    return List.of(ascDefinition, ascNullsFirstDefinition, ascNullsLastDefinition,
      descDefinition, descNullsFirstDefinition, descNullsLastDefinition);
  }

  private Type generateListQueryType(String typeName) {
    return new NonNullType(new ListType(new NonNullType(new TypeName(typeName))));
  }

  public List<GraphQLArgument> generateTableUpdateMutationArguments(TableMetadata tableMetadata) {
    // TODO: not supported jsonb op arg temporarily
    GraphQLArgument setArgument = generateUpdateSetArgument(tableMetadata);
    GraphQLArgument incArgument = generateUpdateIncArgument(tableMetadata);
    String whereObjectTypeName = String.format(GraphQLConstants.WHERE_TYPE_FORMAT, tableMetadata.getName());
    GraphQLInputObjectType whereObjectType = graphQLInputObjectTypeByName.get(whereObjectTypeName);
    InputValueDefinition whereInputObjectDefinition = InputValueDefinition.newInputValueDefinition()
      .name(GraphQLConstants.WHERE)
      .type(new NonNullType(new TypeName(whereObjectTypeName)))
      .build();
    GraphQLArgument whereArgument = GraphQLArgument.newArgument()
      .name(GraphQLConstants.WHERE)
      .definition(whereInputObjectDefinition)
      .type(GraphQLNonNull.nonNull(whereObjectType))
      .build();
    return List.of(setArgument, incArgument, whereArgument);
  }

  public List<GraphQLArgument> generateTableUpdateByPkMutationArguments(TableMetadata tableMetadata) {
    // TODO: not supported jsonb op arg temporarily
    GraphQLArgument setArgument = generateUpdateSetArgument(tableMetadata);
    GraphQLArgument incArgument = generateUpdateIncArgument(tableMetadata);

    String pkInputObjectTypeName = String.format(
      GraphQLConstants.UPDATE_ARG_PK_COLUMNS_TYPE_NAME_FORMAT, tableMetadata.getName());
    GraphQLInputObjectType pkInputObjectType = graphQLInputObjectTypeByName.get(pkInputObjectTypeName);
    InputValueDefinition pkInputObjectDefinition = InputValueDefinition.newInputValueDefinition()
      .name(GraphQLConstants.PK_COLUMNS)
      .type(new NonNullType(new TypeName(pkInputObjectTypeName)))
      .build();
    GraphQLArgument pkArgument = GraphQLArgument.newArgument()
      .name(GraphQLConstants.PK_COLUMNS)
      .definition(pkInputObjectDefinition)
      .type(GraphQLNonNull.nonNull(pkInputObjectType))
      .build();
    return List.of(setArgument, incArgument, pkArgument);
  }

  public List<GraphQLArgument> generateTableInsertMutationArguments(TableMetadata tableMetadata) {
    String objectsTypeName = String.format(GraphQLConstants.INSERT_INPUT_ARG_TYPE_NAME_FORMAT, tableMetadata.getName());
    GraphQLInputObjectType objectsType = graphQLInputObjectTypeByName.get(objectsTypeName);
    InputValueDefinition objectsDefinition = InputValueDefinition.newInputValueDefinition()
      .name(GraphQLConstants.OBJECTS)
      .type(new NonNullType(new ListType(new NonNullType(new TypeName(objectsTypeName)))))
      .build();
    GraphQLArgument objectsArgument = GraphQLArgument.newArgument()
      .name(GraphQLConstants.OBJECTS)
      .definition(objectsDefinition)
      .type(GraphQLNonNull.nonNull(GraphQLList.list(GraphQLNonNull.nonNull(objectsType))))
      .build();
    GraphQLArgument onConflictArgument = generateTableInsertOnConflictArgument(tableMetadata);
    return List.of(objectsArgument, onConflictArgument);
  }

  public List<GraphQLArgument> generateTableInsertOneMutationArguments(TableMetadata tableMetadata) {
    String objectTypeName = String.format(GraphQLConstants.INSERT_INPUT_ARG_TYPE_NAME_FORMAT, tableMetadata.getName());
    GraphQLInputObjectType objectType = graphQLInputObjectTypeByName.get(objectTypeName);
    InputValueDefinition objectDefinition = InputValueDefinition.newInputValueDefinition()
      .name(GraphQLConstants.OBJECT)
      .type(new NonNullType(new TypeName(objectTypeName)))
      .build();
    GraphQLArgument objectArgument = GraphQLArgument.newArgument()
      .name(GraphQLConstants.OBJECT)
      .definition(objectDefinition)
      .type(GraphQLNonNull.nonNull(objectType))
      .build();
    GraphQLArgument onConflictArgument = generateTableInsertOnConflictArgument(tableMetadata);
    return List.of(objectArgument, onConflictArgument);
  }

  private GraphQLArgument generateTableInsertOnConflictArgument(TableMetadata tableMetadata) {
    String objectTypeName = String.format(GraphQLConstants.INSERT_INPUT_ARG_TYPE_NAME_FORMAT, tableMetadata.getName());
    String onConflictTypeName = String.format(GraphQLConstants.INSERT_ON_CONFLICT_ARG_TYPE_NAME_FORMAT, tableMetadata.getName());
    GraphQLInputType onConflictType = getInputObjectTypeByName(onConflictTypeName);
    InputValueDefinition onConflictDefinition = InputValueDefinition.newInputValueDefinition()
      .name(GraphQLConstants.ON_CONFLICT)
      .type(new TypeName(objectTypeName))
      .build();
    return GraphQLArgument.newArgument()
      .name(GraphQLConstants.ON_CONFLICT)
      .definition(onConflictDefinition)
      .type(onConflictType)
      .build();
  }

  public GraphQLArgument generateTableDeleteMutationArgument(TableMetadata tableMetadata) {
    String whereTypeName = String.format(GraphQLConstants.WHERE_TYPE_FORMAT, tableMetadata.getName());
    InputValueDefinition valueDefinition = InputValueDefinition.newInputValueDefinition()
      .name(GraphQLConstants.WHERE)
      .type(new NonNullType(new TypeName(whereTypeName)))
      .build();
    return GraphQLArgument.newArgument()
      .name(GraphQLConstants.WHERE)
      .type(GraphQLNonNull.nonNull(graphQLInputObjectTypeByName.get(whereTypeName)))
      .definition(valueDefinition)
      .build();
  }

  public GraphQLArgument generateTableDeleteByPkMutationArgument(TableMetadata tableMetadata) {
    InputValueDefinition valueDefinition = InputValueDefinition.newInputValueDefinition()
      .name(GraphQLConstants.ID)
      .type(new NonNullType(new TypeName(Scalars.GraphQLLong.getName())))
      .build();
    return GraphQLArgument.newArgument()
      .name(GraphQLConstants.ID)
      .type(GraphQLNonNull.nonNull(Scalars.GraphQLLong))
      .definition(valueDefinition)
      .build();
  }

  private GraphQLArgument generateUpdateSetArgument(TableMetadata tableMetadata) {
    String setObjectTypeName = String.format(
      GraphQLConstants.UPDATE_ARG_SET_TYPE_NAME_FORMAT, tableMetadata.getName());
    GraphQLInputObjectType setObjectType = graphQLInputObjectTypeByName.get(setObjectTypeName);
    InputValueDefinition setInputObjectDefinition = InputValueDefinition.newInputValueDefinition()
      .name(GraphQLConstants._SET)
      .type(new TypeName(setObjectTypeName))
      .build();
    return  GraphQLArgument.newArgument()
      .name(GraphQLConstants._SET)
      .definition(setInputObjectDefinition)
      .type(setObjectType)
      .build();
  }

  private GraphQLArgument generateUpdateIncArgument(TableMetadata tableMetadata) {
    String incObjectTypeName = String.format(
        GraphQLConstants.UPDATE_ARG_INC_TYPE_NAME_FORMAT, tableMetadata.getName());
    GraphQLInputObjectType incObjectType = graphQLInputObjectTypeByName.get(incObjectTypeName);
    InputValueDefinition incInputObjectDefinition = InputValueDefinition.newInputValueDefinition()
      .name(GraphQLConstants._INC)
      .type(new TypeName(incObjectTypeName))
      .build();
    return GraphQLArgument.newArgument()
      .name(GraphQLConstants._INC)
      .definition(incInputObjectDefinition)
      .type(incObjectType)
      .build();
  }
}
