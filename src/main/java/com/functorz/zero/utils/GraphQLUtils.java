package com.functorz.zero.utils;

import com.functorz.zero.datamodel.ColumnMetadata;
import com.functorz.zero.datamodel.ColumnType;
import com.functorz.zero.datamodel.RelationMetadata;
import com.functorz.zero.datamodel.RelationType;
import com.functorz.zero.graphql.GraphQLConstants;
import graphql.Assert;
import graphql.Scalars;
import graphql.execution.ExecutionStepInfo;
import graphql.language.InputValueDefinition;
import graphql.language.ListType;
import graphql.language.Selection;
import graphql.language.Type;
import graphql.language.TypeName;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.GraphQLInputObjectField;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLNonNull;
import graphql.schema.GraphQLOutputType;
import graphql.schema.GraphQLScalarType;
import graphql.schema.GraphQLType;
import graphql.schema.GraphQLTypeReference;
import io.leangen.graphql.module.common.jackson.JacksonObjectScalars;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.jooq.Condition;
import org.jooq.DeleteUsingStep;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.SelectJoinStep;
import org.jooq.SelectSelectStep;
import org.jooq.SortOrder;
import org.jooq.UpdateFromStep;
import org.jooq.UpdateSetFirstStep;
import org.jooq.impl.DSL;

public class GraphQLUtils {
  private static final Set<String> WHERE_LOGIC_FIELDS = Set.of(
    GraphQLConstants._NOT, GraphQLConstants._AND, GraphQLConstants._OR);

  public static ExecutionStepInfo findRootStepRecursionly(ExecutionStepInfo stepInfo) {
    if (stepInfo.getPath().getLevel() == 1) {
      return stepInfo;
    }
    return findRootStepRecursionly(stepInfo.getParent());
  }

  public static GraphQLScalarType mapColumnToGraphQLScalarType(ColumnType columnType) {
    switch (columnType) {
      case BIGINT:
      case BIGSERIAL:
        return Scalars.GraphQLLong;
      case INTEGER:
        return Scalars.GraphQLInt;
      case FLOAT8:
        return Scalars.GraphQLFloat;
      case DECIMAL:
        return Scalars.GraphQLBigDecimal;
      case TEXT:
        return Scalars.GraphQLString;
      case BOOLEAN:
        return Scalars.GraphQLBoolean;
      case TIMESTAMPTZ:
      case TIMETZ:
        return io.leangen.graphql.util.Scalars.GraphQLOffsetDateTime;
      case DATE:
        return io.leangen.graphql.util.Scalars.GraphQLLocalDate;
      case JSONB:
        return JacksonObjectScalars.JsonAnyNode;
      default:
        throw new IllegalStateException();
    }
  }

  public static Class<? extends Number> mapNumberColumnToNumberType(ColumnType columnType) {
    Utils.isTrue(columnType.isNumeric(), "only support number column");
    switch (columnType) {
      case BIGINT:
      case BIGSERIAL:
        return Long.class;
      case INTEGER:
        return Integer.class;
      case FLOAT8:
        return Double.class;
      case DECIMAL:
        return BigDecimal.class;
      default:
        throw new IllegalStateException(String.format("unsupported number columnType: %s", columnType));
    }
  }

  public static Class<?> mapComparableColumnToJavaType(ColumnType columnType) {
    Utils.isTrue(columnType.isComparable(), "only support comparable column");
    if (columnType.isNumeric()) {
      return mapNumberColumnToNumberType(columnType);
    }
    switch (columnType) {
      case TEXT:
        return String.class;
      case DATE:
        return LocalDate.class;
      case TIMESTAMPTZ:
      case TIMETZ:
        return OffsetDateTime.class;
      default:
        throw new IllegalStateException(String.format("unsupported comparable columnType: %s", columnType));
    }
  }

  public static TypeName mapColumnTypeToTypeName(ColumnType columnType) {
    if (columnType == null) {
      throw new IllegalArgumentException();
    }

    switch (columnType) {
      case BIGSERIAL:
      case BIGINT:
        return new TypeName("Long");
      case INTEGER:
        return new TypeName("Int");
      case FLOAT8:
        return new TypeName("Double");
      case DECIMAL:
        return new TypeName("BigDecimal");
      case TEXT:
        return new TypeName("String");
      case BOOLEAN:
        return new TypeName("Boolean");
      case TIMESTAMPTZ:
      case TIMETZ:
        return new TypeName("OffsetDateTime");
      case DATE:
        return new TypeName("LocalDate");
      case JSONB:
        return new TypeName("Json");
      default:
        throw new IllegalStateException(String.format("unKnown ColumnType: %s", columnType));
    }
  }

  public static GraphQLOutputType getRelationFieldGraphQLOutputType(GraphQLOutputType wrappedType,
                                                                    RelationType relationType, boolean isSource) {
    GraphQLNonNull nonNullWrapper = GraphQLNonNull.nonNull(wrappedType);
    switch (relationType) {
      case ONE_TO_ONE:
        return nonNullWrapper;
      case ONE_TO_MANY:
        return isSource ? GraphQLNonNull.nonNull(GraphQLList.list(nonNullWrapper)) : nonNullWrapper;
      case MANY_TO_MANY:
        return GraphQLNonNull.nonNull(GraphQLList.list(nonNullWrapper));
      default:
        throw new UnsupportedOperationException();
    }
  }

  public static Type getRelationFieldType(String typeName, RelationType relationType, boolean isSource) {
    TypeName type = new TypeName(typeName);
    switch (relationType) {
      case ONE_TO_ONE:
        return type;
      case ONE_TO_MANY:
        return isSource ? new ListType(type) : type;
      case MANY_TO_MANY:
        return new ListType(type);
      default:
        throw new UnsupportedOperationException();
    }
  }

  public static GraphQLType wrappedTypeReferenceIfNull(GraphQLType graphQLType, String typeName) {
    return graphQLType == null ? GraphQLTypeReference.typeRef(typeName) : graphQLType;
  }

  public static SelectJoinStep<Record> mapDistinctConditionToQueryBuilder(
    SelectSelectStep<Record> selectStep, String tableName, List<Object> distinctOnCondition) {
    List<Field<Object>> fields = distinctOnCondition == null
      ? List.of()
      : distinctOnCondition.stream().map(column -> {
        return DSL.field((String) column);
      }).collect(Collectors.toList());
    return CollectionUtils.isEmpty(fields)
      ? selectStep.from(tableName)
      : selectStep.distinctOn(fields).from(tableName);
  }

  public static void addArgumentsToSelectBuilder(SelectJoinStep<Record> builder, Map<String, Object> arguments) {
    Map<String, Object> whereArgument = Utils.uncheckedCast(arguments.get(GraphQLConstants.WHERE));
    convertObjectBoolExpToCondition(whereArgument)
      .ifPresent(condition -> builder.where(condition));
    Integer limitCondition = (Integer) arguments.get(GraphQLConstants.LIMIT);
    Integer offsetCondition = (Integer) arguments.get(GraphQLConstants.OFFSET);
    if (limitCondition != null) {
      builder.limit(limitCondition);
    }
    if (offsetCondition != null) {
      builder.offset(offsetCondition);
    }
    List<Map<String, Object>> orderByCondition = Utils
      .uncheckedCast(arguments.get(GraphQLConstants.ORDER_BY));
    addOrderByArgumentToSelectBuilder(builder, orderByCondition);
  }

  public static Optional<Condition> convertObjectBoolExpToCondition(Map<String, Object> objectBoolExp) {
    if (objectBoolExp == null || objectBoolExp.isEmpty()) {
      return Optional.empty();
    }
    return objectBoolExp.entrySet().stream()
      .map(entry -> parseCondition(entry))
      .filter(Optional::isPresent)
      .map(Optional::get)
      .reduce((c1, c2) -> c1.and(c2));
  }

  private static Optional<Condition> parseCondition(Map.Entry<String, Object> entry) {
    if (WHERE_LOGIC_FIELDS.contains(entry.getKey())) {
      switch (entry.getKey()) {
        case GraphQLConstants._AND:
        case GraphQLConstants._OR:
          List<Map<String, Object>> objectBoolExp = Utils.uncheckedCast(entry.getValue());
          if (objectBoolExp == null) {
            return Optional.empty();
          }
          BinaryOperator<Condition> reduceCondition = GraphQLConstants._AND.equals(entry.getKey())
            ? (c1, c2) -> c1.and(c2) : (c1, c2) -> c1.or(c2);
          return objectBoolExp.stream()
            .map(GraphQLUtils::convertObjectBoolExpToCondition)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .reduce(reduceCondition);
        case GraphQLConstants._NOT:
          Map<String, Object> notObjectBoolExp = Utils.uncheckedCast(entry.getValue());
          if (notObjectBoolExp == null) {
            return Optional.empty();
          }
          return convertObjectBoolExpToCondition(notObjectBoolExp).map(Condition::not);
        default:
          throw new IllegalStateException(String.format("unknown where logic field:%s", entry.getKey()));
      }
    }
    Field<Object> field = DSL.field(entry.getKey());
    Map<String, Object> valueByComparisonOperator = Utils.uncheckedCast(entry.getValue());
    return valueByComparisonOperator.entrySet().stream().map(subEntry -> {
      String key = subEntry.getKey();
      try {
        Method targetMethod = parseComparisonMethod(key);
        return (Condition) targetMethod.invoke(field, subEntry.getValue());
      } catch (Exception e) {
        throw new IllegalStateException(String.format("parse condition failed, entry: %s", entry.toString()), e);
      }
    }).reduce((c1, c2) -> c1.and(c2));
  }

  private static Method parseComparisonMethod(String key) throws NoSuchMethodException {
    switch (key) {
      case GraphQLConstants.COMPARSION_EQ:
        return Field.class.getDeclaredMethod("eq", Object.class);
      case GraphQLConstants.COMPARSION_NEQ:
        return Field.class.getDeclaredMethod("ne", Object.class);
      case GraphQLConstants.COMPARSION_GT:
        return Field.class.getDeclaredMethod("gt", Object.class);
      case GraphQLConstants.COMPARSION_GTE:
        return Field.class.getDeclaredMethod("greaterOrEqual", Object.class);
      case GraphQLConstants.COMPARSION_LT:
        return Field.class.getDeclaredMethod("lt", Object.class);
      case GraphQLConstants.COMPARSION_LTE:
        return Field.class.getDeclaredMethod("lessOrEqual", Object.class);
      case GraphQLConstants.COMPARSION_IS_NULL:
        return Field.class.getDeclaredMethod("isNull");
      case GraphQLConstants.COMPARSION_LIKE:
        return Field.class.getDeclaredMethod("like", String.class);
      case GraphQLConstants.COMPARSION_NLIKE:
        return Field.class.getDeclaredMethod("notLike", String.class);
      case GraphQLConstants.COMPARSION_ILIKE:
        return Field.class.getDeclaredMethod("likeIgnoreCase", String.class);
      case GraphQLConstants.COMPARSION_NILIKE:
        return Field.class.getDeclaredMethod("notLikeIgnoreCase", String.class);
      case GraphQLConstants.COMPARSION_SIMILAR:
        return Field.class.getDeclaredMethod("similarTo", String.class);
      case GraphQLConstants.COMPARSION_NSIMILAR:
        return Field.class.getDeclaredMethod("notSimilarTo", String.class);
      case GraphQLConstants.COMPARSION_HAS_KEY:
      case GraphQLConstants.COMPARSION_HAS_KEYS_ALL:
      case GraphQLConstants.COMPARSION_HAS_KEYS_ANY:
        // TODO: json select generate
        return Field.class.getDeclaredMethod("isJson");
      default:
        throw new IllegalStateException("unknown comparsion key");
    }
  }

  private static void addOrderByArgumentToSelectBuilder(SelectJoinStep<Record> builder,
                                            List<Map<String, Object>> orderByCondition) {
    if (!CollectionUtils.isEmpty(orderByCondition)) {
      orderByCondition.stream()
        .filter(map -> !map.isEmpty())
        .forEach(orderByConditionValue -> {
          orderByConditionValue.entrySet().stream().forEach(entry -> {
            String orderBy = (String) entry.getValue();
            String field = entry.getKey();
            Field<Object> orderField = DSL.field(field);
            switch (orderBy) {
              case GraphQLConstants.ORDER_BY_ASC:
                builder.orderBy(orderField.sort(SortOrder.ASC));
                break;
              case GraphQLConstants.ORDER_BY_ASC_NULLS_FIRST:
                builder.orderBy(orderField.sort(SortOrder.ASC).nullsFirst());
                break;
              case GraphQLConstants.ORDER_BY_ASC_NULLS_LAST:
                builder.orderBy(orderField.sort(SortOrder.ASC).nullsLast());
                break;
              case GraphQLConstants.ORDER_BY_DESC:
                builder.orderBy(orderField.sort(SortOrder.DESC));
                break;
              case GraphQLConstants.ORDER_BY_DESC_NULLS_FIRST:
                builder.orderBy(orderField.sort(SortOrder.DESC).nullsFirst());
                break;
              case GraphQLConstants.ORDER_BY_DESC_NULLS_LAST:
                builder.orderBy(orderField.sort(SortOrder.DESC).nullsLast());
                break;
              default:
                throw new IllegalStateException(String.format("unknown orderBy enum value: %s", orderBy));
            }
          });
        });
    }
  }

  public static void addArgumentsToDeleteBuilder(DeleteUsingStep<Record> builder, Map<String, Object> arguments) {
    Map<String, Object> whereArgument = (Map<String, Object>) arguments.get(GraphQLConstants.WHERE);
    convertObjectBoolExpToCondition(whereArgument).ifPresent(condition -> builder.where(condition));
  }

  public static String getRelationInputArgName(RelationMetadata relationMetadata) {
    boolean isObjectRelation = relationMetadata.getType() == RelationType.ONE_TO_ONE;
    String format = isObjectRelation
        ? GraphQLConstants.INSERT_OBJECT_RELATION_ARG_TYPE_NAME_FORMAT
        : GraphQLConstants.INSERT_ARRAY_RELATION_ARG_TYPE_NAME_FORMAT;
    return String.format(format, relationMetadata.getTargetTable());
  }

  public static GraphQLInputObjectField convertColumnMetadataToGraphQLInputObjectField(ColumnMetadata columnMetadata) {
    InputValueDefinition inputValueDefinition = InputValueDefinition
      .newInputValueDefinition()
      .name(columnMetadata.getName())
      .type(GraphQLUtils.mapColumnTypeToTypeName(columnMetadata.getType()))
      .build();
    return GraphQLInputObjectField.newInputObjectField()
      .name(columnMetadata.getName())
      .definition(inputValueDefinition)
      .type(GraphQLUtils.mapColumnToGraphQLScalarType(columnMetadata.getType()))
      .build();
  }

  public static void addArgumentsToUpdateBuilder(UpdateSetFirstStep builder, Map<String, Object> arguments,
                                                 boolean byPk, Map<String, Class<?>> javaTypeByColumn) {
    Map<String, Object> incArgMap = (Map<String, Object>) arguments.get(GraphQLConstants._INC);
    Map<String, Object> setArgMap = (Map<String, Object>) arguments.get(GraphQLConstants._SET);

    if (!CollectionUtils.isEmpty(incArgMap)) {
      incArgMap.entrySet().forEach(entry -> {
        Class<? extends Number> numberClass = (Class<? extends Number>) javaTypeByColumn.get(entry.getKey());
        Field<? extends Number> field = DSL.field(entry.getKey(), numberClass);
        builder.set(field, field.plus((Number) entry.getValue()));
      });
    }
    if (!CollectionUtils.isEmpty(setArgMap)) {
      setArgMap.entrySet().forEach(entry -> {
        builder.set(DSL.field(entry.getKey()), entry.getValue());
      });
    }
    if (byPk) {
      Map<String, Object> pkMap = (Map<String, Object>) arguments.get(GraphQLConstants.PK_COLUMNS);
      Long id = (Long) pkMap.get(GraphQLConstants.ID);
      ((UpdateFromStep) builder).where(DSL.field(GraphQLConstants.ID).eq(id));
    } else {
      Map<String, Object> whereArg = (Map<String, Object>) arguments.get(GraphQLConstants.WHERE);
      convertObjectBoolExpToCondition(whereArg)
        .ifPresent(condition -> ((UpdateFromStep) builder).where(condition));
    }
  }

  public static Set<String> getMutationSelectionSet(DataFetchingEnvironment env) {
    return env.getMergedField().getSingleField()
      .getSelectionSet().getSelections().stream()
      .map(selection -> (graphql.language.Field) selection)
      .map(graphql.language.Field::getName)
      .collect(Collectors.toSet());
  }

  public static Set<String> getMutationReturningSelectionSet(DataFetchingEnvironment env) {
    Function<Selection, graphql.language.Field> selectionFieldFunction =
      selection -> (graphql.language.Field) selection;
    return env.getMergedField().getSingleField()
      .getSelectionSet().getSelections().stream()
      .map(selectionFieldFunction)
      .filter(field -> GraphQLConstants.RETURNING.equals(field.getName()))
      .findFirst()
      .map(field -> {
        return field.getSelectionSet().getSelections().stream()
          .map(selectionFieldFunction)
          .map(graphql.language.Field::getName)
          .collect(Collectors.toSet());
      }).orElse(Set.of());
  }
}
