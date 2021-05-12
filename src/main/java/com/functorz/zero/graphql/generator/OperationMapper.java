package com.functorz.zero.graphql.generator;

import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLObjectType;
import java.util.ArrayList;
import java.util.List;

public class OperationMapper {
  private List<GraphQLFieldDefinition> queries = new ArrayList<>();
  private List<GraphQLFieldDefinition> mutations = new ArrayList<>();
  private List<GraphQLFieldDefinition> subscriptions = new ArrayList<>();

  public void registerRootQueryOperation(GraphQLFieldDefinition queryFieldDefinition) {
    queries.add(queryFieldDefinition);
  }

  public void registerRootMutationOperation(GraphQLFieldDefinition mutationFieldDefinition) {
    mutations.add(mutationFieldDefinition);
  }

  public void registerRootSubscriptionOperation(GraphQLFieldDefinition subscriptionFieldDefinition) {
    subscriptions.add(subscriptionFieldDefinition);
  }

  public GraphQLObjectType getRootQuery(String queryRoot) {
    return GraphQLObjectType.newObject()
      .name(queryRoot).fields(queries).build();
  }

  public GraphQLObjectType getRootMutation(String mutationRoot) {
    return GraphQLObjectType.newObject()
      .name(mutationRoot).fields(mutations).build();
  }

  public GraphQLObjectType getRootSubscription(String subscriptionRoot) {
    return GraphQLObjectType.newObject()
      .name(subscriptionRoot).fields(subscriptions).build();
  }
}
