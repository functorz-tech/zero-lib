package com.functorz.zero.graphql.dataloader;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.functorz.zero.datamodel.RelationMetadata;
import com.functorz.zero.graphql.GraphQLConstants;
import com.functorz.zero.graphql.generator.TableConfig;
import com.functorz.zero.utils.DataModelUtils;
import com.functorz.zero.utils.GraphQLUtils;
import com.functorz.zero.utils.ResultSetUtils;
import com.functorz.zero.utils.Utils;
import graphql.schema.GraphQLScalarType;
import graphql.schema.SelectedField;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import org.dataloader.BatchLoader;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.SelectConditionStep;
import org.jooq.SelectJoinStep;
import org.jooq.SelectSelectStep;
import org.jooq.conf.ParamType;
import org.jooq.impl.DSL;
import org.springframework.jdbc.core.JdbcTemplate;

@AllArgsConstructor
public abstract class RelationDataLoader<V> implements BatchLoader<DataLoaderKeyWrapper, V> {
  private TableConfig tableConfig;
  private RelationMetadata relationMetadata;
  private JdbcTemplate jdbcTemplate;

  public List<ObjectNode> getMergedResult(List<DataLoaderKeyWrapper> keyWrappers) {
    List<Object> keys = keyWrappers.stream()
      .map(DataLoaderKeyWrapper::getKey).collect(Collectors.toList());
    String queryTable = DataModelUtils.getQueryTable(tableConfig.getTableMetadata(), relationMetadata);
    List<SelectedField> selectedFields = keyWrappers.get(0).getSelectedFields();
    Set<String> selectFieldSet = selectedFields.stream().map(SelectedField::getName).collect(Collectors.toSet());
    Set<String> selectedColumns = selectedFields.stream()
      .filter(field -> field.getFieldDefinition().getType() instanceof GraphQLScalarType)
      .map(selectedField -> selectedField.getName())
      .collect(Collectors.toSet());
    tableConfig.getColumnByGraphQLRelationFieldMap()
      .entrySet().stream()
      .filter(entry -> selectFieldSet.contains(entry.getKey()))
      .forEach(entry -> selectedColumns.add(entry.getValue()));
    List<Field<Object>> fields = selectedColumns.stream().map(DSL::field).collect(Collectors.toList());
    SelectSelectStep<Record> selectStep = Utils.CONTEXT.select(fields);
    SelectConditionStep<Record> builder;
    if (getClass() == ListDataLoader.class) {
      Map<String, Object> arguments = keyWrappers.get(0).getArguments();
      List<Object> distinctOnCondition = (List<Object>) arguments.get(GraphQLConstants.DISTINCT_ON);
      SelectJoinStep<Record> selectJoinStep = GraphQLUtils
        .mapDistinctConditionToQueryBuilder(selectStep, queryTable, distinctOnCondition);
      GraphQLUtils.addArgumentsToSelectBuilder(selectJoinStep, arguments);
      builder = (SelectConditionStep<Record>) selectJoinStep;
    } else {
      builder = (SelectConditionStep<Record>) selectStep.from(queryTable);
    }
    builder.and(DSL.field(getConditionColumnName()).in(keys));

    String querySql = builder.getSQL(ParamType.INLINED);
    return jdbcTemplate.query(querySql, ResultSetUtils::mapResultSetToListJson);
  }

  public String getConditionColumnName() {
    return DataModelUtils.getQueryColumnName(tableConfig.getTableMetadata(), relationMetadata);
  }
}
