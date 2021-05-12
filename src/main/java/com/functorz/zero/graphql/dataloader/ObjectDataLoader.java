package com.functorz.zero.graphql.dataloader;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.functorz.zero.datamodel.RelationMetadata;
import com.functorz.zero.graphql.generator.TableConfig;
import com.functorz.zero.utils.Utils;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import org.springframework.jdbc.core.JdbcTemplate;

public class ObjectDataLoader extends RelationDataLoader<ObjectNode> {
  public ObjectDataLoader(TableConfig tableConfig,
                          RelationMetadata relationMetadata,
                          JdbcTemplate jdbcTemplate) {
    super(tableConfig, relationMetadata, jdbcTemplate);
  }

  @Override
  public CompletionStage<List<ObjectNode>> load(List<DataLoaderKeyWrapper> keyWrappers) {
    return CompletableFuture.supplyAsync(() -> {
      List<ObjectNode> mergedResult = getMergedResult(keyWrappers);
      List<Object> keys = keyWrappers.stream()
        .map(DataLoaderKeyWrapper::getKey).collect(Collectors.toList());
      return Utils.sortByProperty(keys, mergedResult, node ->
        (Object) Utils.parseScalarNodeValue(node.get(getConditionColumnName())));
    });
  }
}
