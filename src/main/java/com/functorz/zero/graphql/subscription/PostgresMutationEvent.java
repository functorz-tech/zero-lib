package com.functorz.zero.graphql.subscription;

import java.util.Set;
import org.springframework.util.CollectionUtils;

public interface PostgresMutationEvent extends GraphQLSubscriptionEvent {
  String getTableName();

  @Override
  default boolean matches(Set<String> tableSet) {
    return !CollectionUtils.isEmpty(tableSet) && tableSet.contains(getTableName());
  }
}
