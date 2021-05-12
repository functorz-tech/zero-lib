package com.functorz.zero.graphql.subscription.apollo;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@NoArgsConstructor
@AllArgsConstructor
public class ApolloMessage<T> {
  private String id;
  private String type;
  private T payload;

  @Setter
  public static class IncomeMessage extends ApolloMessage<JsonNode> {
  }
}
