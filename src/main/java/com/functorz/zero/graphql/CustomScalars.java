package com.functorz.zero.graphql;

import com.functorz.zero.graphql.coercing.JsonCoercing;
import graphql.schema.GraphQLScalarType;

public class CustomScalars {
  public static final GraphQLScalarType GraphQLJson = GraphQLScalarType
    .newScalar().name("Json").description("Json Scalar").coercing(new JsonCoercing()).build();
}
