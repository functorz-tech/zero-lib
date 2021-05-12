package com.functorz.zero.graphql;

import com.functorz.zero.datamodel.ColumnMetadata;
import com.functorz.zero.datamodel.ColumnType;
import com.functorz.zero.datamodel.RelationMetadata;
import com.functorz.zero.datamodel.RelationType;
import com.functorz.zero.datamodel.TableMetadata;
import com.functorz.zero.graphql.generator.AggregateType;
import com.functorz.zero.graphql.generator.TableConfig;
import com.functorz.zero.utils.CollectionUtils;
import com.functorz.zero.utils.DataModelUtils;
import com.functorz.zero.utils.GraphQLUtils;
import com.functorz.zero.utils.Utils;
import graphql.execution.ExecutionStepInfo;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLNonNull;
import graphql.schema.GraphQLScalarType;
import graphql.schema.GraphQLType;
import graphql.schema.GraphQLTypeUtil;
import graphql.schema.SelectedField;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.jooq.AggregateFunction;
import org.jooq.DeleteUsingStep;
import org.jooq.Field;
import org.jooq.InsertOnConflictWhereStep;
import org.jooq.InsertOnDuplicateSetMoreStep;
import org.jooq.InsertOnDuplicateSetStep;
import org.jooq.InsertOnDuplicateStep;
import org.jooq.InsertResultStep;
import org.jooq.InsertSetStep;
import org.jooq.Query;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.SelectJoinStep;
import org.jooq.SelectSelectStep;
import org.jooq.Table;
import org.jooq.Update;
import org.jooq.UpdateConditionStep;
import org.jooq.UpdateSetFirstStep;
import org.jooq.conf.ParamType;
import org.jooq.impl.DSL;

public class DataFetcherSqlGenerator {
  private static final Set<String> SYSTEM_DEFINED_ARG_KEY_SET = Set.of(GraphQLConstants.WHERE,
      GraphQLConstants.ORDER_BY, GraphQLConstants.OFFSET,
      GraphQLConstants.LIMIT, GraphQLConstants.DISTINCT_ON);

  public static String generateQueryListSql(TableConfig tableConfig, DataFetchingEnvironment env) {
    ExecutionStepInfo stepInfo = env.getExecutionStepInfo();
    if (!isListType(stepInfo.getType())) {
      throw new IllegalStateException("only support List Type");
    }
    TableMetadata tableMetadata = tableConfig.getTableMetadata();
    ExecutionStepInfo rootStepInfo = GraphQLUtils.findRootStepRecursionly(stepInfo);
    Map<String, Object> arguments = rootStepInfo.getArguments();
    Set<String> tableColumns = DataModelUtils.generateColumnSet(tableMetadata);
    String tableName = tableMetadata.getName();

    List<Object> distinctOnCondition = (List<Object>) arguments.get(GraphQLConstants.DISTINCT_ON);
    Set<String> selectedColumns = GraphQLUtils.getMutationSelectionSet(env).stream()
        .filter(fieldName -> tableColumns.contains(fieldName))
        .collect(Collectors.toSet());
    Set<String> selectFieldSet = GraphQLUtils.getMutationSelectionSet(env);
    tableConfig.getColumnByGraphQLRelationFieldMap()
      .entrySet().stream()
      .filter(entry -> selectFieldSet.contains(entry.getKey()))
      .forEach(entry -> selectedColumns.add(entry.getValue()));
    List<Field<Object>> subFields = tableMetadata.getColumnMetadata().stream()
      .map(ColumnMetadata::getName)
      .filter(name -> selectedColumns.contains(name))
      .map(DSL::field).collect(Collectors.toList());
    SelectSelectStep<Record> selectStep = CollectionUtils.isEmpty(distinctOnCondition)
      ? Utils.CONTEXT.select(mapColumnNamesToFields(selectedColumns))
      : Utils.CONTEXT.select(subFields);
    SelectJoinStep<Record> builder = GraphQLUtils
      .mapDistinctConditionToQueryBuilder(selectStep, tableName, distinctOnCondition);
    GraphQLUtils.addArgumentsToSelectBuilder(builder, arguments);
    return builder.getSQL(ParamType.INLINED);
  }

  public static String generateQueryByPkSql(TableConfig tableConfig, DataFetchingEnvironment env) {
    ExecutionStepInfo stepInfo = env.getExecutionStepInfo();
    if (!GraphQLTypeUtil.isNotWrapped(stepInfo.getType())) {
      throw new IllegalStateException("only support object Type");
    }
    TableMetadata tableMetadata = tableConfig.getTableMetadata();
    Set<String> tableColumns = DataModelUtils.generateColumnSet(tableMetadata);
    Map<String, Object> arguments = env.getArguments();
    String pkName = tableMetadata.getPrimaryKeyColumn().getName();
    Object argValue = arguments.get(pkName);
    if (argValue == null) {
      throw new IllegalStateException("argValue is expected not null");
    }
    Set<String> selectedColumns = env.getSelectionSet().getFields().stream()
        .filter(field -> field.getFieldDefinition().getType() instanceof GraphQLScalarType)
        .filter(field -> tableColumns.contains(field))
        .map(selectedField -> selectedField.getName())
        .collect(Collectors.toSet());
    Set<String> selectFieldSet = env.getSelectionSet().getFields().stream()
        .map(SelectedField::getName).collect(Collectors.toSet());
    tableConfig.getColumnByGraphQLRelationFieldMap()
        .entrySet().stream()
        .filter(entry -> selectFieldSet.contains(entry.getKey()))
        .forEach(entry -> selectedColumns.add(entry.getValue()));
    Query query = Utils.CONTEXT.select(mapColumnNamesToFields(selectedColumns))
        .from(tableMetadata.getName()).where(DSL.field(pkName).eq(argValue));
    return query.getSQL(ParamType.INLINED);
  }

  public static String generateAggregateQuerySql(TableMetadata tableMetadata, ColumnType columnType,
                                                 String columnName, DataFetchingEnvironment env, AggregateType type) {
    if (type == AggregateType.COUNT) {
      return generateQueryCountSql(tableMetadata, env);
    }
    ExecutionStepInfo stepInfo = env.getExecutionStepInfo();
    Table<Record> subQuery = generateAggregateSubQuery(tableMetadata.getName(), stepInfo);
    AggregateFunction<?> aggregateFunction;
    Method method = type.getJooqAggregateMethod();
    try {
      switch (type) {
        case AVG:
        case SUM:
          Class<? extends Number> clazz = GraphQLUtils.mapNumberColumnToNumberType(columnType);
          aggregateFunction = (AggregateFunction<?>) method.invoke(null, DSL.field(columnName, clazz));
          break;
        case MAX:
        case MIN:
          aggregateFunction = (AggregateFunction<?>) method.invoke(null, DSL.field(columnName));
          break;
        default:
          throw new IllegalStateException();
      }
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new IllegalStateException(e);
    }

    return Utils.CONTEXT.select(aggregateFunction).from(subQuery).getSQL(ParamType.INLINED);
  }

  public static String generateUpdateByPkMutationSql(TableMetadata tableMetadata,
                                                     DataFetchingEnvironment env) {
    Map<String, Object> arguments = env.getArguments();

    UpdateSetFirstStep<Record> builder = Utils.CONTEXT.update(DSL.table(tableMetadata.getName()));
    Map<String, Class<?>> javaTypeByColumn = generateJavaTypeByColumn(tableMetadata);
    GraphQLUtils.addArgumentsToUpdateBuilder(builder, arguments, true, javaTypeByColumn);
    UpdateConditionStep conditionStep = (UpdateConditionStep) builder;
    Set<String> columnSet = DataModelUtils.generateColumnSet(tableMetadata);
    Set<Field<Object>> selectFields = GraphQLUtils.getMutationSelectionSet(env).stream()
      .filter(columnSet::contains)
      .map(DSL::field)
      .collect(Collectors.toSet());
    selectFields.add(DSL.field(GraphQLConstants.ID));
    conditionStep.returningResult(selectFields);
    return ((Update) builder).getSQL(ParamType.INLINED);
  }

  public static String generateUpdateMutationSql(TableMetadata tableMetadata,
                                                 DataFetchingEnvironment env) {
    Map<String, Object> arguments = env.getArguments();

    UpdateSetFirstStep<Record> builder = Utils.CONTEXT.update(DSL.table(tableMetadata.getName()));
    Map<String, Class<?>> javaTypeByColumn = generateJavaTypeByColumn(tableMetadata);
    GraphQLUtils.addArgumentsToUpdateBuilder(builder, arguments, false, javaTypeByColumn);
    Set<String> selectColumns = GraphQLUtils.getMutationSelectionSet(env);
    boolean containsReturning = selectColumns.contains(GraphQLConstants.RETURNING);
    UpdateConditionStep conditionStep = (UpdateConditionStep) builder;
    if (containsReturning) {
      Set<Field<Object>> returningColumns = GraphQLUtils.getMutationReturningSelectionSet(env)
        .stream().map(DSL::field).collect(Collectors.toSet());
      conditionStep.returning(returningColumns);
    } else {
      conditionStep.returning(DSL.field("1"));
    }
    return conditionStep.getSQL(ParamType.INLINED);
  }

  public static String generateInsertOneMutationSql(String tableName,
                                                    Set<String> columnSet,
                                                    Set<Field<Object>> selectedFields,
                                                    Map<String, Object> objectInput,
                                                    Map<String, Object> onConflict) {
    InsertSetStep<Record> builder = Utils.CONTEXT.insertInto(DSL.table(tableName));
    objectInput.keySet().stream()
      .filter(key -> columnSet.contains(key))
      .forEach(column -> {
        builder.set(DSL.field(column), objectInput.get(column));
      });
    InsertOnDuplicateStep<Record> duplicateStep = (InsertOnDuplicateStep) builder;
    if (onConflict == null) {
      return duplicateStep.onConflictDoNothing().returning(selectedFields).getSQL(ParamType.INLINED);
    }
    String conflictConstraint = Utils.uncheckedCast(onConflict.get(GraphQLConstants.CONSTRAINT));
    InsertOnDuplicateSetStep<Record> doUpdateStep = duplicateStep
      .onConflictOnConstraint(DSL.constraint(conflictConstraint)).doUpdate();
    List<String> updateColumns = Utils.uncheckedCast(onConflict.get(GraphQLConstants.UPDATE_COLUMNS));
    updateColumns.stream().forEach(column -> {
      doUpdateStep.set(DSL.field(column), objectInput.get(column));
    });
    Map<String, Object> whereArgument = Utils.uncheckedCast(onConflict.get(GraphQLConstants.WHERE));
    GraphQLUtils.convertObjectBoolExpToCondition(whereArgument)
        .ifPresent(condition -> {
          ((InsertOnDuplicateSetMoreStep) doUpdateStep).where(condition);
        });
    InsertResultStep insertResultStep = ((InsertOnConflictWhereStep) doUpdateStep).returning(selectedFields);
    return insertResultStep.getSQL(ParamType.INLINED);
  }

  public static String generateDeleteMutationSql(TableMetadata tableMetadata, DataFetchingEnvironment env) {
    ExecutionStepInfo stepInfo = env.getExecutionStepInfo();

    if (!GraphQLTypeUtil.isNotWrapped(stepInfo.getType())) {
      throw new IllegalStateException("only object type is supported");
    }
    Map<String, Object> arguments = env.getArguments();
    Set<String> selectionSet = GraphQLUtils.getMutationSelectionSet(env);
    boolean containsReturning = selectionSet.contains(GraphQLConstants.RETURNING);
    DeleteUsingStep<Record> deleteUsingStep = Utils.CONTEXT.delete(DSL.table(tableMetadata.getName()));
    GraphQLUtils.addArgumentsToDeleteBuilder(deleteUsingStep, arguments);
    if (containsReturning) {
      Set<Field<Object>> selectColumns = GraphQLUtils.getMutationReturningSelectionSet(env)
        .stream().map(DSL::field).collect(Collectors.toSet());
      deleteUsingStep.returning(selectColumns);
    } else {
      deleteUsingStep.returning(DSL.field("1"));
    }
    return deleteUsingStep.getSQL(ParamType.INLINED);
  }

  public static String generateDeleteByPkMutationSql(TableMetadata tableMetadata, DataFetchingEnvironment env) {
    ExecutionStepInfo stepInfo = env.getExecutionStepInfo();

    if (!GraphQLTypeUtil.isNotWrapped(stepInfo.getType())) {
      throw new IllegalStateException("only support object Type");
    }
    Map<String, Object> arguments = env.getArguments();
    long id = (Long) arguments.get(GraphQLConstants.ID);
    Set<Field<Object>> selectColumns = GraphQLUtils.getMutationSelectionSet(env)
      .stream().map(DSL::field).collect(Collectors.toSet());
    return Utils.CONTEXT.delete(DSL.table(tableMetadata.getName()))
      .where(DSL.field(GraphQLConstants.ID).eq(id)).returning(selectColumns).getSQL(ParamType.INLINED);
  }

  private static String generateQueryCountSql(TableMetadata tableMetadata, DataFetchingEnvironment env) {
    ExecutionStepInfo stepInfo = env.getExecutionStepInfo();
    Table<Record> subQuery = generateAggregateSubQuery(tableMetadata.getName(), stepInfo);
    Map<String, Object> arguments = env.getArguments();
    List<Object> columnsCondition = Utils.uncheckedCast(arguments.get(GraphQLConstants.COLUMNS));
    Boolean distinctCondition = (Boolean) arguments.get(GraphQLConstants.DISTINCT);
    AggregateFunction<Integer> aggregateFunction = generateCountFunction(columnsCondition, distinctCondition);
    SelectJoinStep<Record1<Integer>> query = Utils.CONTEXT.select(aggregateFunction).from(subQuery);
    return query.getSQL(ParamType.INLINED);
  }

  private static AggregateFunction<Integer> generateCountFunction(List<Object> columnsCondition, Boolean distinctCondition) {
    List<Field<Object>> fields = columnsCondition == null ? List.of()
      : columnsCondition.stream().map(column -> {
        return DSL.field((String) column);
      }).collect(Collectors.toList());
    if (distinctCondition == null || !distinctCondition) {
      return fields.size() == 1 ? DSL.count(fields.get(0)) : DSL.count();
    } else {
      return fields.size() == 1 ? DSL.countDistinct(fields.get(0))
        : DSL.countDistinct(fields.toArray(new Field[0]));
    }
  }

  private static Table<Record> generateAggregateSubQuery(String tableName, ExecutionStepInfo stepInfo) {
    if (!GraphQLTypeUtil.isScalar(stepInfo.getType())) {
      throw new IllegalStateException("only supports scalar type");
    }
    ExecutionStepInfo rootStepInfo = GraphQLUtils.findRootStepRecursionly(stepInfo);
    Map<String, Object> arguments = rootStepInfo.getArguments();
    SelectSelectStep<Record> selectStep = Utils.CONTEXT.select();
    List<Object> distinctOnCondition = Utils.uncheckedCast(arguments.get(GraphQLConstants.DISTINCT_ON));
    SelectJoinStep<Record> subQuery = GraphQLUtils
      .mapDistinctConditionToQueryBuilder(selectStep, tableName, distinctOnCondition);
    GraphQLUtils.addArgumentsToSelectBuilder(subQuery, arguments);
    return subQuery.asTable();
  }

  private static List<Field<Object>> mapColumnNamesToFields(Collection<String> columns) {
    return columns.stream().map(DSL::field).collect(Collectors.toList());
  }

  private static boolean isListType(GraphQLType type) {
    return GraphQLTypeUtil.isList(type)
      || (type instanceof GraphQLNonNull && ((GraphQLNonNull) type).getWrappedType() instanceof GraphQLList);
  }

  private static boolean isMultipleInserts(RelationMetadata relationMetadata) {
    return relationMetadata.getType() != RelationType.ONE_TO_ONE;
  }

  private static Map<String, Class<?>> generateJavaTypeByColumn(TableMetadata tableMetadata) {
    return tableMetadata.getColumnMetadata().stream()
      .collect(Collectors.toMap(ColumnMetadata::getName,
        columnMetadata -> columnMetadata.getType().getJavaType()));
  }
}
