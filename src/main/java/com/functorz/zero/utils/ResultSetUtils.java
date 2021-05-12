package com.functorz.zero.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import org.postgresql.util.PGobject;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.StatementCallback;
import org.springframework.util.CollectionUtils;

public class ResultSetUtils {
  public static List<ObjectNode> mapResultSetToListJson(ResultSet rs) throws SQLException {
    List<ObjectNode> result = new ArrayList<>();
    Consumer<ObjectNode> consumer = node -> result.add(node);
    consumeResultSet(rs, consumer);
    return result;
  }

  public static ArrayNode mapResultSetToArrayNode(ResultSet rs) throws SQLException {
    ArrayNode arrayNode = Utils.newArrayNode();
    Consumer<ObjectNode> consumer = node -> arrayNode.add(node);
    consumeResultSet(rs, consumer);
    return arrayNode;
  }

  public static ObjectNode mapResultSetToObjectNode(ResultSet rs) throws SQLException {
    List<ObjectNode> objectNodes = mapResultSetToListJson(rs);
    return CollectionUtils.isEmpty(objectNodes) ? null : objectNodes.get(0);
  }

  public static <T> StatementCallback<T> generateStatementCallback(String sql,
                                              ResultSetExtractor<T> rsExtractor) {
    return statement -> {
      ResultSet rs = statement.executeQuery(sql);
      return rsExtractor.extractData(rs);
    };
  }

  private static void consumeResultSet(ResultSet rs, Consumer<ObjectNode> consumer) throws SQLException {
    ResultSetMetaData metaData = rs.getMetaData();
    int columnCount = metaData.getColumnCount();
    while (rs.next()) {
      ObjectNode node = Utils.OBJECT_MAPPER.createObjectNode();

      for (int i = 1; i <= columnCount; i++) {
        String columnName = metaData.getColumnName(i);
        Object value = rs.getObject(i);
        if (value instanceof PGobject && "jsonb".equals(((PGobject) value).getType())) {
          value = Utils.readStringToJson(((PGobject)value).getValue());
          node.set(columnName, (JsonNode) value);
        } else {
          node.set(columnName, Utils.OBJECT_MAPPER.convertValue(value, JsonNode.class));
        }
      }
      consumer.accept(node);
    }
  }
}
