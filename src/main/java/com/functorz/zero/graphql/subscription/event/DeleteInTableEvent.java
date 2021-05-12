package com.functorz.zero.graphql.subscription.event;

import com.functorz.zero.graphql.subscription.PostgresMutationEvent;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@EqualsAndHashCode
public class DeleteInTableEvent implements PostgresMutationEvent {
  private String tableName;
}
