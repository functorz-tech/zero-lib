package com.functorz.zero.graphql.io;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Getter
@AllArgsConstructor
@NoArgsConstructor
@ToString(of = {"query", "operationName"})
@JsonIgnoreProperties(ignoreUnknown = true)
public class GraphQLRequest {
  private String query;
  private String operationName;
  private Map<String, Object> variables;
}
