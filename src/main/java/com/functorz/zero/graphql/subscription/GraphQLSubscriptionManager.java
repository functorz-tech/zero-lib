package com.functorz.zero.graphql.subscription;

import com.functorz.zero.datamodel.DataModel;
import com.functorz.zero.datamodel.TableMetadata;
import com.functorz.zero.graphql.DataLoaderRegistryFactory;
import com.functorz.zero.graphql.subscription.apollo.ApolloWebSocketHandler;
import graphql.GraphQL;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

public class GraphQLSubscriptionManager {
  private PostgresListenNotifyManager postgresListenNotifyManager;
  private Set<String> listenChannels;

  private ApolloWebSocketHandler apolloWebSocketHandler;

  public GraphQLSubscriptionManager(DataSource dataSource, DataModel dataModel,
    GraphQL graphQL, DataLoaderRegistryFactory factory, Collection<PostgresNotifyListener> listeners) throws SQLException {
    this.listenChannels = dataModel.getTableMetadata().stream().map(TableMetadata::getName)
      .map(this::transformTableNameToChannelName).collect(Collectors.toSet());
    this.postgresListenNotifyManager = new PostgresListenNotifyManager(dataSource, listenChannels, listeners);
    this.apolloWebSocketHandler = new ApolloWebSocketHandler(graphQL, factory, subscriptionWsKeepAliveTaskScheduler());
  }

  public void initialize(GenericApplicationContext applicationContext) {
    postgresListenNotifyManager.init();
    applicationContext.registerBean("graphQLWebSocketConfigurer", GraphQLWebSocketConfigurer.class,
        apolloWebSocketHandler);
  }

  public void destroy() {
    postgresListenNotifyManager.destroy();
  }

  private String transformTableNameToChannelName(String tableName) {
    // default channel
    return "mutation";
  }

  private TaskScheduler subscriptionWsKeepAliveTaskScheduler() {
    ThreadPoolTaskScheduler threadPoolScheduler = new ThreadPoolTaskScheduler();
    threadPoolScheduler.setThreadNamePrefix("GraphQLWSKeepAlive-");
    threadPoolScheduler.setPoolSize(Runtime.getRuntime().availableProcessors());
    threadPoolScheduler.setRemoveOnCancelPolicy(true);
    threadPoolScheduler.setRejectedExecutionHandler(new ThreadPoolExecutor.DiscardPolicy());
    threadPoolScheduler.initialize();
    threadPoolScheduler.shutdown();
    return threadPoolScheduler;
  }
}
