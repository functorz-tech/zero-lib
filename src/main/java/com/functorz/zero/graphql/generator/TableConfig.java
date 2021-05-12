package com.functorz.zero.graphql.generator;

import com.functorz.zero.datamodel.RelationMetadata;
import com.functorz.zero.datamodel.TableMetadata;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@AllArgsConstructor
@Getter
@Setter
@NoArgsConstructor
public class TableConfig {
  private TableMetadata tableMetadata;
  private List<RelationMetadata> relationAsSourceTable;
  private List<RelationMetadata> relationAsTargetTable;

  public Map<String, String> getColumnByGraphQLRelationFieldMap() {
    Map<String, String> map = new HashMap<>();
    relationAsSourceTable.stream().forEach(relationMetadata -> {
      map.put(relationMetadata.getNameInSource(), relationMetadata.getSourceColumn());
    });
    relationAsTargetTable.stream().forEach(relationMetadata -> {
      map.put(relationMetadata.getNameInTarget(), relationMetadata.getTargetColumn());
    });
    return map;
  }
}
