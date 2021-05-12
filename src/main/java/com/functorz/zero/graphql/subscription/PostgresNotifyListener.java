package com.functorz.zero.graphql.subscription;

import com.impossibl.postgres.api.jdbc.PGNotificationListener;

public interface PostgresNotifyListener extends PGNotificationListener {
  default String getName() {
    return this.getClass().getSimpleName();
  }

  /**
   * Channel name based notification filter (Regular Expression).
   */
  default String getChannelNameFilter() {
    return null;
  }
}
