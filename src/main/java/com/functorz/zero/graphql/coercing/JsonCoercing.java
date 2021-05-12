package com.functorz.zero.graphql.coercing;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.functorz.zero.utils.Utils;
import graphql.language.StringValue;
import graphql.schema.Coercing;
import graphql.schema.CoercingParseLiteralException;
import graphql.schema.CoercingParseValueException;
import graphql.schema.CoercingSerializeException;
import java.io.IOException;

public class JsonCoercing implements Coercing<JsonNode, String> {

  @Override
  public String serialize(Object dataFetcherResult) {
    try {
      return Utils.OBJECT_MAPPER.writeValueAsString(dataFetcherResult);
    } catch (IOException e) {
      throw new CoercingSerializeException(e);
    }
  }

  @Override
  public JsonNode parseValue(Object input) {
    try {
      if (input instanceof String) {
        return Utils.OBJECT_MAPPER.readValue((String) input, JsonNode.class);
      } else if (input instanceof JsonNode) {
        return (JsonNode) input;
      } else {
        throw new CoercingParseValueException(String.format("unsupported input type: %s", input));
      }
    } catch (JsonProcessingException e) {
      throw new CoercingParseValueException(e);
    }
  }

  @Override
  public JsonNode parseLiteral(Object input) {
    try {
      if (input instanceof StringValue) {
        return Utils.OBJECT_MAPPER.readValue(((StringValue) input).getValue(), JsonNode.class);
      }
      throw new CoercingParseLiteralException("parse literal input error");
    } catch (JsonProcessingException e) {
      throw new CoercingParseLiteralException(e);
    }
  }
  
}
