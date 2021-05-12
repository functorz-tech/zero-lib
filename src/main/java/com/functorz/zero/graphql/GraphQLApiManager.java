package com.functorz.zero.graphql;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.functorz.zero.datamodel.DataModel;
import com.functorz.zero.datamodel.RelationMetadata;
import com.functorz.zero.datamodel.RelationType;
import com.functorz.zero.datamodel.TableMetadata;
import com.functorz.zero.graphql.dataloader.ListDataLoader;
import com.functorz.zero.graphql.dataloader.ObjectDataLoader;
import com.functorz.zero.graphql.generator.GraphQLSchemaGenerator;
import com.functorz.zero.graphql.generator.RequestIdProvider;
import com.functorz.zero.graphql.subscription.GraphQLSubscriptionEventObserver;
import com.functorz.zero.graphql.subscription.GraphQLSubscriptionManager;
import com.functorz.zero.graphql.subscription.PostgresNotifyListener;
import com.functorz.zero.utils.DataModelUtils;
import com.functorz.zero.utils.Utils;
import graphql.GraphQL;
import graphql.execution.AsyncExecutionStrategy;
import graphql.schema.GraphQLSchema;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.Collection;
import java.util.stream.Stream;
import org.apache.commons.io.IOUtils;
import org.dataloader.BatchLoader;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.ResourceUtils;

public class GraphQLApiManager {
  private static final String CREATE_INSERT_OR_UPDATE_TRIGGER_FORMAT =
      "select create_insert_or_update_trigger_if_not_exists('%s', '%s');";
  private static final String CREATE_DELETE_TRIGGER_FORMAT =
      "select create_delete_trigger_if_not_exists('%s', '%s');";

  private GraphQL graphQL;
  private DataModel dataModel;
  private JdbcTemplate jdbcTemplate;
  private ObjectMapper objectMapper;
  private PlatformTransactionManager transactionManager;
  private ApplicationEventPublisher eventPublisher;
  private DataFetcherGenerator dataFetcherGenerator;
  private GraphQLSchemaGenerator schemaGenerator;
  private DataLoaderRegistryFactory dataLoaderRegistryFactory;
  private GraphQLSubscriptionManager subscriptionManager;

  public GraphQLApiManager(DataModel dataModel, ApplicationContext applicationContext) throws SQLException {
    GenericApplicationContext genericApplicationContext = (GenericApplicationContext) applicationContext;
    this.dataModel = dataModel;
    this.jdbcTemplate = Utils.getBeanByTypeOrThrow(applicationContext, JdbcTemplate.class);
    this.objectMapper = Utils.getBeanByTypeOrElse(applicationContext, ObjectMapper.class, Utils.OBJECT_MAPPER);
    this.transactionManager = Utils.getBeanByTypeOrThrow(applicationContext, PlatformTransactionManager.class);
    this.eventPublisher = applicationContext;
    GraphQLSubscriptionEventObserver graphQLSubscriptionEventObserver = new GraphQLSubscriptionEventObserver(eventPublisher);
    genericApplicationContext.registerBean("graphQLSubscriptionEventObserver", GraphQLSubscriptionEventObserver.class, applicationContext);
    this.dataFetcherGenerator = new DataFetcherGenerator(dataModel, objectMapper, jdbcTemplate, transactionManager, graphQLSubscriptionEventObserver);
    this.schemaGenerator = new GraphQLSchemaGenerator(dataFetcherGenerator, objectMapper);
    this.dataLoaderRegistryFactory = new DataLoaderRegistryFactory();
    initialize();
    genericApplicationContext.registerBean("dataLoaderRegistryFactory", DataLoaderRegistryFactory.class);
    Collection<PostgresNotifyListener> listeners = applicationContext.getBeansOfType(PostgresNotifyListener.class).values();
    this.subscriptionManager = new GraphQLSubscriptionManager(jdbcTemplate.getDataSource(),
      dataModel, graphQL, dataLoaderRegistryFactory, listeners);
    this.subscriptionManager.initialize(genericApplicationContext);
  }

  public GraphQL getGraphQL() {
    return graphQL;
  }

  public void initialize() {
    executeInitSql();
    initTableMutationTrigger(dataModel);
    DataModelUtils.generateTableConfigMap(dataModel).values().stream()
      .forEach(config -> {
        Stream.concat(config.getRelationAsSourceTable().stream(), config.getRelationAsTargetTable().stream())
          .forEach(relationMetadata -> {
            BatchLoader loader = needsListDataLoader(config.getTableMetadata(), relationMetadata)
              ? new ListDataLoader(config, relationMetadata, jdbcTemplate)
              : new ObjectDataLoader(config, relationMetadata, jdbcTemplate);
            dataLoaderRegistryFactory.addDataLoader(relationMetadata.getId().toString(), loader);
          });
        schemaGenerator.registerTableMetadata(config);
      });
    this.graphQL = generateGraphQL();
  }

  public void destroy() {
    subscriptionManager.destroy();
  }

  private boolean needsListDataLoader(TableMetadata tableMetadata, RelationMetadata relationMetadata) {
    return (relationMetadata.getType() == RelationType.ONE_TO_MANY
        && relationMetadata.getSourceTable().equals(tableMetadata.getName()))
        || relationMetadata.getType() == RelationType.MANY_TO_MANY;
  }

  private void executeInitSql() {
    try (InputStream inputStream = ResourceUtils.getURL("classpath:sql/init.sql").openStream()) {
      String initSql = IOUtils.toString(inputStream, "utf-8");
      jdbcTemplate.execute(initSql);
    } catch (IOException e) {
      throw new IllegalStateException("execute init sql failed");
    }
  }

  private void initTableMutationTrigger(DataModel dataModel) {
    dataModel.getTableMetadata().stream()
        .map(TableMetadata::getName)
        .forEach(tableName -> {
          String createInsertOrUpdateTriggerSql = String.format(CREATE_INSERT_OR_UPDATE_TRIGGER_FORMAT,
              tableName, String.format("%s_insert_or_update", tableName));
          String createDeleteTriggerSql = String.format(CREATE_DELETE_TRIGGER_FORMAT,
              tableName, String.format("%s_delete_or_truncate", tableName));
          jdbcTemplate.execute(createInsertOrUpdateTriggerSql);
          jdbcTemplate.execute(createDeleteTriggerSql);
        });
  }

  private GraphQL generateGraphQL() {
    GraphQLSchema graphQLSchema = schemaGenerator.generate();
    return GraphQL.newGraphQL(graphQLSchema)
      .executionIdProvider(new RequestIdProvider())
      .queryExecutionStrategy(new AsyncExecutionStrategy())
      .build();
  }
}
