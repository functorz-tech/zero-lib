package com.functorz.zero.graphql.subscription;

import com.impossibl.postgres.api.jdbc.PGConnection;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import javax.sql.DataSource;

public class PostgresListenNotifyManager {
  private PGConnection pgConnection;

  private Set<String> listenChannels;

  public PostgresListenNotifyManager(DataSource dataSource,
                                     Set<String> listenChannels,
                                     Collection<PostgresNotifyListener> listeners) throws SQLException {
    this.listenChannels = listenChannels;
    Connection connection = dataSource.getConnection();
    pgConnection = (PGConnection) connection;
    listeners.stream().forEach(listener -> {
      pgConnection.addNotificationListener(listener.getName(), listener.getChannelNameFilter(), listener);
    });
  }

  public void init() {
    try (Statement statement = pgConnection.createStatement()) {
      for (String channel : listenChannels) {
        statement.execute(String.format("listen %s", channel));
      }
    } catch (SQLException e) {
      throw new IllegalStateException("init pgListenNotifyManager failed", e);
    }
  }

  public void destroy() {
    try (Statement statement = pgConnection.createStatement()) {
      for (String channel : listenChannels) {
        statement.execute(String.format("unlisten %s", channel));
      }
    } catch (SQLException e) {
      throw new IllegalStateException("destroy pgListenNotifyManager failed", e);
    } finally {
      try {
        pgConnection.close();
      } catch (SQLException e) {
        throw new IllegalStateException(e);
      }
    }
  }
}
