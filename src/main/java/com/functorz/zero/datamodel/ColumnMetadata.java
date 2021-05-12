package com.functorz.zero.datamodel;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@EqualsAndHashCode
@JsonInclude(value = JsonInclude.Include.NON_NULL)
public class ColumnMetadata {
  @JsonProperty(required = true)
  private UUID id;
  @JsonProperty(required = true)
  private String name;
  @JsonProperty(required = true)
  private ColumnType type;
  private boolean primaryKey;
  private boolean required;
  private boolean unique;
  private boolean uiHidden;
  private boolean systemDefined;
}
