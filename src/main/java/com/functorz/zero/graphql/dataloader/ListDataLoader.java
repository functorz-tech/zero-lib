package com.functorz.zero.graphql.dataloader;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.functorz.zero.datamodel.RelationMetadata;
import com.functorz.zero.graphql.generator.TableConfig;
import com.functorz.zero.utils.Utils;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import org.springframework.jdbc.core.JdbcTemplate;

public class ListDataLoader extends RelationDataLoader<List<ObjectNode>> {
  public ListDataLoader(TableConfig tableConfig,
                        RelationMetadata relationMetadata,
                        JdbcTemplate jdbcTemplate) {
    super(tableConfig, relationMetadata, jdbcTemplate);
  }

  @Override
  public CompletionStage<List<List<ObjectNode>>> load(List<DataLoaderKeyWrapper> keyWrappers) {
    return CompletableFuture.supplyAsync(() -> {
      List<ObjectNode> mergedResult = getMergedResult(keyWrappers);
      List<Object> keys = keyWrappers.stream()
          .map(DataLoaderKeyWrapper::getKey).collect(Collectors.toList());
      Map<Object, List<ObjectNode>> groupMap = Utils.groupByKey(mergedResult, node ->
        Utils.parseScalarNodeValue(node.get(getConditionColumnName())));
      return keys.stream().map(key -> groupMap.get(key)).collect(Collectors.toList());
    });
  }
}
