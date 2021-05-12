package com.functorz.zero.graphql.generator;

import com.functorz.zero.datamodel.ColumnMetadata;
import com.functorz.zero.datamodel.ColumnType;
import com.functorz.zero.utils.GraphQLUtils;
import graphql.Scalars;
import graphql.schema.GraphQLScalarType;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import lombok.Getter;
import org.jooq.Field;
import org.jooq.impl.DSL;

@Getter
public enum AggregateType {
  AVG("%s_avg_fields") {
    @Override
    public boolean compatibleWithColumn(ColumnMetadata columnMetadata) {
      return columnMetadata.getType().isNumeric();
    }

    @Override
    public GraphQLScalarType getGraphQLScalarType(ColumnMetadata columnMetadata) {
      return Scalars.GraphQLBigDecimal;
    }

    @Override
    public Class<?> aggregateResultJavaType(ColumnType columnType) {
      return BigDecimal.class;
    }
  },
  MAX("%s_max_fields") {
    @Override
    public boolean compatibleWithColumn(ColumnMetadata columnMetadata) {
      return columnMetadata.getType().isComparable();
    }
  },
  MIN("%s_min_fields") {
    @Override
    public boolean compatibleWithColumn(ColumnMetadata columnMetadata) {
      return columnMetadata.getType().isComparable();
    }
  },
  SUM("%s_sum_fields") {
    @Override
    public boolean compatibleWithColumn(ColumnMetadata columnMetadata) {
      return columnMetadata.getType().isNumeric();
    }
  },
  COUNT("") {
    @Override
    public Class<?> aggregateResultJavaType(ColumnType columnType) {
      return Integer.class;
    }
  },
  ;

  private String fieldTypeNameFormat;

  AggregateType(String fieldTypeNameFormat) {
    this.fieldTypeNameFormat = fieldTypeNameFormat;
  }

  public boolean compatibleWithColumn(ColumnMetadata columnMetadata) {
    return true;
  }

  public GraphQLScalarType getGraphQLScalarType(ColumnMetadata columnMetadata) {
    return GraphQLUtils.mapColumnToGraphQLScalarType(columnMetadata.getType());
  }

  public String getGraphQLFieldName() {
    return name().toLowerCase();
  }

  public Class<?> aggregateResultJavaType(ColumnType columnType) {
    if (!columnType.isComparable()) {
      throw new IllegalStateException(String.format("columnType: %s, aggregateType: %s", columnType, this));
    }
    return GraphQLUtils.mapComparableColumnToJavaType(columnType);
  }

  public Method getJooqAggregateMethod() {
    try {
      return DSL.class.getDeclaredMethod(getGraphQLFieldName(), Field.class);
    } catch (NoSuchMethodException e) {
      throw new IllegalStateException(e);
    }
  }
}
