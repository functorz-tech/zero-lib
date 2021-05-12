package com.functorz.zero.graphql.subscription;

import com.functorz.zero.graphql.subscription.apollo.ApolloWebSocketHandler;
import lombok.AllArgsConstructor;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

@AllArgsConstructor
public class GraphQLWebSocketConfigurer implements WebSocketConfigurer {
  private ApolloWebSocketHandler handler;

  @Override
  public void registerWebSocketHandlers(WebSocketHandlerRegistry webSocketHandlerRegistry) {
    webSocketHandlerRegistry.addHandler(handler, "/api/graphql-subscription")
                            .setAllowedOrigins("*");
  }
}
