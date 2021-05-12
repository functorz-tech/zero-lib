package com.functorz.zero.graphql.subscription.apollo;

import com.fasterxml.jackson.databind.JsonNode;
import com.functorz.zero.graphql.DataLoaderRegistryFactory;
import com.functorz.zero.graphql.io.GraphQLRequest;
import com.functorz.zero.utils.Utils;
import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.GraphQL;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import org.springframework.util.CollectionUtils;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.ConcurrentWebSocketSessionDecorator;

@Slf4j
public class ApolloMessageHandler {
  private static final int BUFFER_SIZE_LIMIT = 128 * 1024;
  private static final int SEND_TIME_LIMIT = 1000;

  public static final String GQL_CONNECTION_INIT = "connection_init";              // Client -> Server
  public static final String GQL_CONNECTION_ACK = "connection_ack";                // Server -> Client
  public static final String GQL_CONNECTION_ERROR = "connection_error";            // Server -> Client
  public static final String GQL_CONNECTION_KEEP_ALIVE = "ka";                     // Server -> Client
  public static final String GQL_CONNECTION_TERMINATE = "connection_terminate";    // Client -> Server
  public static final String GQL_START = "start";                                  // Client -> Server
  public static final String GQL_DATA = "data";                                    // Server -> Client
  public static final String GQL_ERROR = "error";                                  // Server -> Client
  public static final String GQL_COMPLETE = "complete";                            // Server -> Client
  public static final String GQL_STOP = "stop";                                    // Client -> Server

  private final WebSocketSession session;
  private final GraphQL graphQL;
  private final DataLoaderRegistryFactory dataLoaderRegistryFactory;
  private final Map<String, ExecutionResultSubscriber> subscriptionByApolloMessageId = new ConcurrentHashMap<>();
  private final Map<String, Map<String, Object>> subscriptionLastResultByApolloMessageId = new ConcurrentHashMap<>();
  private Object graphQLContext;

  protected ApolloMessageHandler(WebSocketSession session,
                                 GraphQL graphQL,
                                 DataLoaderRegistryFactory factory) {
    this.session = new ConcurrentWebSocketSessionDecorator(session, SEND_TIME_LIMIT, BUFFER_SIZE_LIMIT);
    this.graphQL = graphQL;
    this.dataLoaderRegistryFactory = factory;
    this.graphQLContext = null;
  }

  public void handleMessage(String messageBody) throws Exception {
    ApolloMessage.IncomeMessage incomeMessage = decodeMessage(messageBody);
    switch (incomeMessage.getType()) {
      case GQL_CONNECTION_INIT:
        log.debug("connection init:{}", session.getId());
        validateTokenOrClose(incomeMessage.getId(), incomeMessage.getPayload());
        sendConnectionAckMessage();
        return;
      case GQL_CONNECTION_TERMINATE:
        session.close();
        return;
      case GQL_START:
        unsubscribeIfSubscribed(incomeMessage.getId());
        GraphQLRequest request = Utils.OBJECT_MAPPER.treeToValue(incomeMessage.getPayload(), GraphQLRequest.class);
        ExecutionInput input = ExecutionInput.newExecutionInput()
            .query(request.getQuery())
            .operationName(request.getOperationName())
            .dataLoaderRegistry(dataLoaderRegistryFactory.buildCachelessRegistry())
            .variables(request.getVariables())
            .context(graphQLContext)
            .build();
        ExecutionResult result = graphQL.execute(input);
        if (result.getData() instanceof Publisher<?>) {
          handleSubscription(incomeMessage, result.getData());
        }
        if (!CollectionUtils.isEmpty(result.getErrors())) {
          result.getErrors().forEach(error -> sendErrorMessage(incomeMessage.getId(), error.getMessage()));
        }
        return;
      case GQL_STOP:
        unsubscribeIfSubscribed(incomeMessage.getId());
        return;
      default:
        sendErrorMessage(incomeMessage.getId(), "Invalid message type");
        return;
    }
  }

  public void destroy() {
    log.debug("destroying");
    subscriptionByApolloMessageId.keySet().forEach(this::unsubscribeIfSubscribed);
  }

  public void keepAlive() {
    sendKeepAliveMessage();
  }

  private void unsubscribeIfSubscribed(String messageId) {
    log.debug("cancel subscription: {}", messageId);
    clearCacheByMessageId(messageId);
    Optional.ofNullable(subscriptionByApolloMessageId.remove(messageId))
        .ifPresent(ExecutionResultSubscriber::cancel);
  }

  private void handleSubscription(ApolloMessage.IncomeMessage incomeMessage, Publisher<ExecutionResult> publisher) {
    Consumer<ExecutionResult> onDataConsumer = result -> {
      boolean cached = subscriptionLastResultByApolloMessageId.containsKey(incomeMessage.getId());
      Map<String, Object> resultMap = result.toSpecification();
      if (cached) {
        Map<String, Object> lastResultMap = subscriptionLastResultByApolloMessageId.get(incomeMessage.getId());
        if (resultMap != null && !resultMap.equals(lastResultMap)) {
          sendDataMessage(incomeMessage.getId(), resultMap);
        }
      } else {
        subscriptionLastResultByApolloMessageId.put(incomeMessage.getId(), resultMap);
        sendDataMessage(incomeMessage.getId(), resultMap);
      }
    };
    Runnable onComplete = () -> {
      clearCacheByMessageId(incomeMessage.getId());
      sendCompleteMessage(incomeMessage.getId());
    };
    ExecutionResultSubscriber subscriber = ExecutionResultSubscriber.builder()
        .onData(onDataConsumer)
        .onComplete(onComplete)
        .onError(ex -> sendErrorMessage(incomeMessage.getId(), ex.getMessage()))
        .build();
    subscriptionByApolloMessageId.put(incomeMessage.getId(), subscriber);
    publisher.subscribe(subscriber);
  }

  private ApolloMessage.IncomeMessage decodeMessage(String messageBody) throws IOException {
    return Utils.OBJECT_MAPPER.readValue(messageBody, ApolloMessage.IncomeMessage.class);
  }

  private void sendDataMessage(String id, Map<String, Object> result) {
    sendMessage(new ApolloMessage<>(id, GQL_DATA, result));
  }

  private void sendCompleteMessage(String id) {
    sendMessage(new ApolloMessage<>(id, GQL_COMPLETE, null));
  }

  private void sendConnectionAckMessage() {
    sendMessage(new ApolloMessage<>(null, GQL_CONNECTION_ACK, null));
  }

  private void sendErrorMessage(String id, String message) {
    sendMessage(new ApolloMessage<>(id, GQL_ERROR, Map.of("message", message)));
  }

  private void sendKeepAliveMessage() {
    sendMessage(new ApolloMessage<>(null, GQL_CONNECTION_KEEP_ALIVE, null));
  }

  private void sendMessage(ApolloMessage<?> message) {
    try {
      session.sendMessage(new TextMessage(Utils.OBJECT_MAPPER.writeValueAsString(message)));
    } catch (IOException ex) {
      log.warn("send message error: {}", ex);
    }
  }

  private void clearCacheByMessageId(String messageId) {
    if (subscriptionLastResultByApolloMessageId.containsKey(messageId)) {
      subscriptionLastResultByApolloMessageId.remove(messageId);
    }
  }

  private void validateTokenOrClose(String messageId, JsonNode payload) {
    // TODO: jwt
    return;
  }
}
