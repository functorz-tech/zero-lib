package com.functorz.zero.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.NumericNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.deser.InstantDeserializer;
import com.fasterxml.jackson.datatype.jsr310.ser.OffsetDateTimeSerializer;
import com.functorz.zero.datamodel.constraint.ConstraintMetadata;
import com.functorz.zero.datamodel.constraint.ConstraintMetadataDeserializer;
import com.google.common.base.CaseFormat;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.ClassUtils;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.reflections.Reflections;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.context.ApplicationContext;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;

public class Utils {
  public static final ObjectMapper OBJECT_MAPPER = Jackson2ObjectMapperBuilder.json()
      .failOnEmptyBeans(false)
      .featuresToDisable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
      .serializerByType(OffsetDateTime.class, OffsetDateTimeSerializer.INSTANCE)
      .deserializerByType(OffsetDateTime.class, InstantDeserializer.OFFSET_DATE_TIME)
      .deserializerByType(ConstraintMetadata.class, ConstraintMetadataDeserializer.INSTANCE)
      .timeZone(TimeZone.getTimeZone("Asia/Shanghai"))
      .build().enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS);
  public static final DSLContext CONTEXT = DSL.using(SQLDialect.POSTGRES);
  private static final Reflections REFLECTIONS = new Reflections("com.functorz.zero");

  public static final String toString(Object object) {
    try {
      return OBJECT_MAPPER.writeValueAsString(object);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("object to string error");
    }
  }

  public static <T> Class<? extends T> findImplementationClass(ObjectNode node, Class<T> interfaceName) {
    Set<Class<? extends T>> subTypes = REFLECTIONS.getSubTypesOf(interfaceName);
    return subTypes.stream().filter(type -> {
      java.lang.reflect.Field[] fields = type.getDeclaredFields();
      if (fields.length == 0) {
        return "{}".equals(node.toString());
      }
      Map<String, java.lang.reflect.Field> fieldByMap = Stream.of(fields)
        .collect(Collectors.toMap(java.lang.reflect.Field::getName, Function.identity()));
      Iterator<Map.Entry<String, JsonNode>> entryIterator = node.fields();
      while (entryIterator.hasNext()) {
        Map.Entry<String, JsonNode> entry = entryIterator.next();
        if (!fieldByMap.containsKey(entry.getKey())) {
          return false;
        }
        if (!ifValueNodeThenMatchesField(entry.getValue(), fieldByMap.get(entry.getKey()))) {
          return false;
        }
      }
      return true;
    }).reduce((t1, t2) -> {
      throw new IllegalStateException(String.format("duplicate subType: %s %s", t1, t2));
    }).orElseThrow(() -> new IllegalStateException("appropriate subType does not exist for " + node));
  }

  private static boolean ifValueNodeThenMatchesField(JsonNode jsonNode, java.lang.reflect.Field field) {
    if (!jsonNode.isValueNode() || field.getType().isInterface()) {
      // array, object, missing
      return true;
    }
    if (jsonNode.isNull()) {
      // null node
      return !field.getType().isPrimitive();
    }
    Class<?> fieldType = ClassUtils.primitiveToWrapper(field.getType());
    JsonNodeType nodeType = jsonNode.getNodeType();
    switch (nodeType) {
      case STRING:
        return fieldType == String.class;
      case BOOLEAN:
        return Boolean.class == fieldType;
      case NUMBER:
        return matchesNumberType(jsonNode, fieldType);
      default:
        // pojo, binary
        return true;
    }
  }

  private static boolean matchesNumberType(JsonNode node, Class<?> clazz) {
    return (node.isInt() && clazz == Integer.class)
        || (node.isShort() && clazz == Short.class)
        || (node.isLong() && clazz == Long.class)
        || (node.isFloat() && clazz == Float.class)
        || (node.isDouble() && clazz == Double.class)
        || (node.isBigDecimal() && clazz == BigDecimal.class)
        || (node.isBigInteger() && clazz == BigInteger.class);
  }

  public static final String toLowerUnderscoreStr(String source) {
    return CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_UNDERSCORE, source.toLowerCase());
  }

  public static final String writeObjectToStringUnchecked(Object object) {
    try {
      return OBJECT_MAPPER.writeValueAsString(object);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException(e);
    }
  }

  public static <T, R> List<T> sortByProperty(List<R> propertyList, List<T> list, Function<T, R> propertyGetter) {
    Map<R, T> map = list.stream().collect(Collectors.toMap(propertyGetter, Function.identity()));
    return propertyList.stream().map(k -> map.get(k)).collect(Collectors.toList());
  }

  public static <K, R> Map<K, List<R>> groupByKey(List<R> sourceList, Function<R, K> keyGetter) {
    return groupByKey(sourceList, keyGetter, Function.identity());
  }

  public static <K, S, R> Map<K, List<R>> groupByKey(Collection<? extends S> sourceList,
                                                     Function<S, K> keyMapper, Function<S, R> valueMapper) {
    BiConsumer<Map<K, List<R>>, S> accumulator = (map, val) -> {
      K key = keyMapper.apply(val);
      R value = valueMapper.apply(val);
      if (map.containsKey(key)) {
        map.get(key).add(value);
      } else {
        List<R> list = new ArrayList<>();
        list.add(value);
        map.put(key, list);
      }
    };

    BinaryOperator<Map<K, List<R>>> mergeFunction = (map1, map2) -> {
      map1.entrySet().stream().forEach(entry -> {
        K key = entry.getKey();
        if (map2.containsKey(key)) {
          map2.get(key).addAll(entry.getValue());
        } else {
          map2.put(key, entry.getValue());
        }
      });
      return map2;
    };
    return sourceList.stream().collect(Collector.of(() -> new HashMap<>(), accumulator, mergeFunction));
  }

  public static <T> List<T> notNullList(List<T> list) {
    return list == null ? List.of() : list;
  }

  public static JsonNode readStringToJson(String source) {
    try {
      return OBJECT_MAPPER.readValue(source, JsonNode.class);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException(e);
    }
  }

  public static Object parseScalarNodeValue(JsonNode node) {
    if (node == null) {
      return null;
    }
    switch (node.getNodeType()) {
      case STRING:
        return node.asText();
      case NULL:
        return null;
      case BOOLEAN:
        return node.asBoolean();
      case NUMBER:
        return ((NumericNode) node).numberValue();
      default:
        throw new IllegalStateException(String.format("node: %s is not scalar node", node));
    }
  }

  public static ObjectNode newObjectNode() {
    return OBJECT_MAPPER.createObjectNode();
  }

  public static ArrayNode newArrayNode() {
    return OBJECT_MAPPER.createArrayNode();
  }

  @SuppressWarnings("unchecked")
  public static <T> T uncheckedCast(Object object) {
    return (T) object;
  }

  public static Set<Field<Object>> mapColumnSetToFieldSet(Set<String> columnSet) {
    return columnSet.stream().map(DSL::field).collect(Collectors.toSet());
  }

  public static void isTrue(boolean expression, String message) {
    if (!expression) {
      throw new IllegalArgumentException(message);
    }
  }

  public static <T> T getBeanByTypeOrThrow(ApplicationContext context, Class<T> clazz) {
    try {
      return context.getBean(clazz);
    } catch (NoSuchBeanDefinitionException e) {
      throw new IllegalStateException(String.format("there is no bean of type: %s exists in applicationContext", clazz.getSimpleName()));
    }
  }

  public static <T> T getBeanByTypeOrElse(ApplicationContext context, Class<T> clazz, T obj) {
    try {
      return context.getBean(clazz);
    } catch (NoSuchBeanDefinitionException e) {
      return obj;
    }
  }
}
