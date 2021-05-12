package com.functorz.zero.datamodel;

import com.fasterxml.jackson.databind.JsonNode;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import lombok.Getter;

@Getter
public enum ColumnType {
  BIGSERIAL(true, true, long.class),
  INTEGER(true, true, int.class),
  BIGINT(true, true, long.class),
  FLOAT8(true, true, double.class),
  DECIMAL(true, true, BigDecimal.class),
  TEXT(false, true, String.class),
  TIMESTAMPTZ(false, true, OffsetDateTime.class),
  TIMETZ(false, true, OffsetDateTime.class),
  DATE(false, true, LocalDate.class),
  BOOLEAN(false, false, boolean.class),
  JSONB(false, false, JsonNode.class),
  ;

  private boolean numeric;
  private boolean comparable;
  private Class<?> javaType;

  ColumnType(boolean numeric, boolean comparable, Class<?> javaType) {
    this.numeric = numeric;
    this.comparable = comparable;
    this.javaType = javaType;
  }
}
