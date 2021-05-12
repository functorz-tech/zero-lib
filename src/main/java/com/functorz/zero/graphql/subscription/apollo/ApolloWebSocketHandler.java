package com.functorz.zero.graphql.subscription.apollo;

import com.functorz.zero.graphql.DataLoaderRegistryFactory;
import graphql.GraphQL;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.SubProtocolCapable;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

@Slf4j
public class ApolloWebSocketHandler extends TextWebSocketHandler implements SubProtocolCapable {
  private static final long KEEP_ALIVE_INTERVAL_MS = 20 * 1000;

  private final Map<WebSocketSession, ApolloMessageHandler> apolloMessageHandlerBySession = new ConcurrentHashMap<>();
  private final Map<WebSocketSession, ScheduledFuture<?>> keepAliveTaskBySession = new ConcurrentHashMap<>();
  private final GraphQL graphQL;
  private final DataLoaderRegistryFactory dataLoaderRegistryFactory;
  private final TaskScheduler taskScheduler;

  public ApolloWebSocketHandler(GraphQL graphQL, DataLoaderRegistryFactory dataLoaderRegistryFactory,
                                TaskScheduler taskScheduler) {
    this.graphQL = graphQL;
    this.dataLoaderRegistryFactory = dataLoaderRegistryFactory;
    this.taskScheduler = taskScheduler;
  }

  @Override
  public List<String> getSubProtocols() {
    return List.of("graphql-ws");
  }

  @Override
  public void handleTransportError(WebSocketSession session, Throwable exception) throws Exception {
    session.close(CloseStatus.SERVER_ERROR);
  }

  @Override
  public void afterConnectionEstablished(WebSocketSession session) {
    log.debug("connected");
    ApolloMessageHandler handler = new ApolloMessageHandler(session, graphQL, dataLoaderRegistryFactory);
    apolloMessageHandlerBySession.put(session, handler);
    keepAliveTaskBySession.put(session, taskScheduler.scheduleWithFixedDelay(handler::keepAlive, KEEP_ALIVE_INTERVAL_MS));
  }

  @Override
  public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
    log.debug("afterConnectionClosed");
    Optional.ofNullable(keepAliveTaskBySession.remove(session))
        .ifPresent(task -> task.cancel(false));
    Optional.ofNullable(apolloMessageHandlerBySession.remove(session))
        .ifPresent(ApolloMessageHandler::destroy);
  }

  @Override
  protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
    apolloMessageHandlerBySession.get(session).handleMessage(message.getPayload());
  }
}
