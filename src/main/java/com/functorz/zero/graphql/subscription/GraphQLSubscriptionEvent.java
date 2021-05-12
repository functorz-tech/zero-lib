package com.functorz.zero.graphql.subscription;

import java.util.Set;
import java.util.UUID;

public interface GraphQLSubscriptionEvent {
  default String getId() {
    return UUID.randomUUID().toString();
  }

  boolean matches(Set<String> tableSet);
}
