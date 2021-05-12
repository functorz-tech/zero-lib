package com.functorz.zero.datamodel;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import com.functorz.zero.datamodel.constraint.ConstraintMetadata;
import java.util.List;
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
@JsonInclude(value = JsonInclude.Include.NON_NULL)
public class TableMetadata {
  private String name;
  private String displayName;
  private String description;
  private List<ColumnMetadata> columnMetadata;
  private List<JsonNode> apiDefinitions;
  private List<ConstraintMetadata> constraintMetadata;

  public ColumnMetadata getPrimaryKeyColumn() {
    return columnMetadata.stream().filter(ColumnMetadata::isPrimaryKey).reduce((c1, c2) -> {
      throw new UnsupportedOperationException("not supported");
    }).get();
  }
}
