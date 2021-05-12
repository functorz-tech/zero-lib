package com.functorz.zero.graphql.subscription.event;

import com.functorz.zero.graphql.subscription.PostgresMutationEvent;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@AllArgsConstructor
@EqualsAndHashCode
@Getter
public class UpdateInTableEvent implements PostgresMutationEvent {
  private String tableName;
  private Long updatedId;
}
