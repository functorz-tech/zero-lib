package com.functorz.zero.datamodel.constraint;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class NotNullConstraint implements ConstraintMetadata {
  @JsonProperty(required = true)
  private String name;
  @JsonProperty(required = true)
  private String columnName;
}
