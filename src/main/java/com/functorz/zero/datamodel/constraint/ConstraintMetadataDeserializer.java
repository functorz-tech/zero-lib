package com.functorz.zero.datamodel.constraint;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.functorz.zero.utils.Utils;
import java.io.IOException;

public class ConstraintMetadataDeserializer extends StdDeserializer<ConstraintMetadata> {
  public static final ConstraintMetadataDeserializer INSTANCE = new ConstraintMetadataDeserializer(
      ConstraintMetadata.class);

  public ConstraintMetadataDeserializer(Class<?> vc) {
    super(vc);
  }

  @Override
  public ConstraintMetadata deserialize(JsonParser parser, DeserializationContext ctxt)
      throws IOException {
    ObjectNode node = (ObjectNode) parser.getCodec().readTree(parser);
    Class<? extends ConstraintMetadata> clazz = Utils.findImplementationClass(node, ConstraintMetadata.class);
    return Utils.OBJECT_MAPPER.convertValue(node, clazz);
  }
}
