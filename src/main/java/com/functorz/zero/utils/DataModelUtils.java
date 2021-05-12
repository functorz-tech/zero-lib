package com.functorz.zero.utils;

import com.functorz.zero.datamodel.ColumnMetadata;
import com.functorz.zero.datamodel.DataModel;
import com.functorz.zero.datamodel.RelationMetadata;
import com.functorz.zero.datamodel.TableMetadata;
import com.functorz.zero.graphql.generator.TableConfig;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class DataModelUtils {
  public static String getQueryTable(TableMetadata tableMetadata, RelationMetadata relationMetadata) {
    return relationMetadata.getSourceTable().equals(tableMetadata.getName())
        ? relationMetadata.getTargetTable() : relationMetadata.getSourceTable();
  }

  public static String getQueryColumnName(TableMetadata tableMetadata, RelationMetadata relationMetadata) {
    return relationMetadata.getSourceTable().equals(getQueryTable(tableMetadata, relationMetadata))
        ? relationMetadata.getSourceColumn() : relationMetadata.getTargetColumn();
  }

  public static Map<String, TableConfig> generateTableConfigMap(DataModel dataModel) {
    return dataModel.getTableMetadata().stream().map(tableMetadata -> {
      Map<String, List<RelationMetadata>> relationBySourceTable = Utils
          .groupByKey(dataModel.getRelationMetadata(), RelationMetadata::getSourceTable);
      Map<String, List<RelationMetadata>> relationByTargetTable = Utils
          .groupByKey(dataModel.getRelationMetadata(), RelationMetadata::getTargetTable);
      List<RelationMetadata> relationsAsSourceTable = Utils.notNullList(relationBySourceTable.get(tableMetadata.getName()));
      List<RelationMetadata> relationsAsTargetTable = Utils.notNullList(relationByTargetTable.get(tableMetadata.getName()));
      return new TableConfig(tableMetadata, relationsAsSourceTable, relationsAsTargetTable);
    }).collect(Collectors.toMap(config -> config.getTableMetadata().getName(), config -> config));
  }

  public static Set<String> generateColumnSet(TableMetadata tableMetadata) {
    return tableMetadata.getColumnMetadata().stream()
      .map(ColumnMetadata::getName).collect(Collectors.toSet());
  }
}
