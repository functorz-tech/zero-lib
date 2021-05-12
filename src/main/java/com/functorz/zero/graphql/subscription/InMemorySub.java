package com.functorz.zero.graphql.subscription;

import java.util.Set;
import java.util.function.Consumer;
import lombok.AllArgsConstructor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

@AllArgsConstructor
public class InMemorySub implements PostgresEventObserver {
  private GraphQLSubscriptionEventObserver graphQLSubscriptionEventObserver;

  @Override
  public <T> Flux<T> on(String key, Set<String> tableSet) {
    Consumer<FluxSink> consumer = emitter -> {
      graphQLSubscriptionEventObserver.registerConsumer(key, event -> {
        if (!event.matches(tableSet)) {
          return;
        }
        emitter.next(event);
      });
      emitter.onCancel(() -> graphQLSubscriptionEventObserver.removeConsumerByName(key));
    };
    return Flux.create(consumer, FluxSink.OverflowStrategy.LATEST);
  }
}
