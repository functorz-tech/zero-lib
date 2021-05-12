package com.functorz.zero.graphql.subscription.event;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class PostgresNotifyEvent {
  private int processId;
  private String channelName;
  private String payload;
}
