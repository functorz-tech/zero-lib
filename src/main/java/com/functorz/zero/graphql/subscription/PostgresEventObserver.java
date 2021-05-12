package com.functorz.zero.graphql.subscription;

import java.util.Set;
import reactor.core.publisher.Flux;

public interface PostgresEventObserver {
  /**
   * called when update, insert, delete on a table in tableSet.
   */
  <T> Flux<T> on(String key, Set<String> tableSet);
}
