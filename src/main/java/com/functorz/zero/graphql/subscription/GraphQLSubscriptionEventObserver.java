package com.functorz.zero.graphql.subscription;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.functorz.zero.graphql.subscription.event.DeleteInTableEvent;
import com.functorz.zero.graphql.subscription.event.InsertInTableEvent;
import com.functorz.zero.graphql.subscription.event.PostgresNotifyEvent;
import com.functorz.zero.graphql.subscription.event.UpdateInTableEvent;
import com.functorz.zero.utils.Utils;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;

public class GraphQLSubscriptionEventObserver implements PostgresNotifyListener {
  private Map<String, Consumer<GraphQLSubscriptionEvent>> consumerByName = new ConcurrentHashMap<>();
  private ApplicationEventPublisher eventPublisher;

  public GraphQLSubscriptionEventObserver(ApplicationEventPublisher eventPublisher) {
    this.eventPublisher = eventPublisher;
  }

  public void registerConsumer(String name, Consumer<GraphQLSubscriptionEvent> consumer) {
    consumerByName.put(name, consumer);
  }

  public void removeConsumerByName(String name) {
    consumerByName.remove(name);
  }

  @Override
  public void notification(int processId, String channelName, String payload) {
    eventPublisher.publishEvent(new PostgresNotifyEvent(processId, channelName, payload));
  }

  @EventListener
  @Async
  public void consumePgNotify(PostgresNotifyEvent postgresNotifyEvent) {
    ObjectNode payloadJson = Utils.uncheckedCast(Utils.readStringToJson(postgresNotifyEvent.getPayload()));
    String operator = payloadJson.get(PostgresNotifyConstants.OP).asText();
    String tableName = payloadJson.get(PostgresNotifyConstants.TABLE).asText();
    GraphQLSubscriptionEvent event;
    long id;
    switch (operator) {
      case PostgresNotifyConstants.INSERT:
        id = payloadJson.get(PostgresNotifyConstants.ID).asLong();
        event = new InsertInTableEvent(tableName, id);
        break;
      case PostgresNotifyConstants.UPDATE:
        id = payloadJson.get(PostgresNotifyConstants.ID).asLong();
        event = new UpdateInTableEvent(tableName, id);
        break;
      case PostgresNotifyConstants.DELETE:
        event = new DeleteInTableEvent(tableName);
        break;
      default:
        throw new IllegalStateException(String.format("unknown op:%s", operator));
    }
    consumerByName.values().stream()
        .forEach(consumer -> consumer.accept(event));
  }
}
