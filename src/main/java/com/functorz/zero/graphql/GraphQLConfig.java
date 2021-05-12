package com.functorz.zero.graphql;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.functorz.zero.datamodel.DataModel;
import lombok.Builder;
import lombok.Getter;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

@Builder
@Getter
public class GraphQLConfig {
  private DataModel dataModel;
  private ObjectMapper objectMapper;
  private JdbcTemplate jdbcTemplate;
  private PlatformTransactionManager transactionManager;
  private ApplicationEventPublisher eventPublisher;
}
