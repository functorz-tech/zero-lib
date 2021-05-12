# 一、Intro

**Zero-lib** is a library which aims to generate **GraphQL Schema** using PostgresQL Database schema.

## 二、Config

PostgresQL Database schema example:

```java
public class DataModel {
  private List<TableMetadata> tableMetadata;
  private List<RelationMetadata> relationMetadata;
}
```

```java
public class TableMetadata {
  private String name;
  private String displayName;
  private String description;
  private List<ColumnMetadata> columnMetadata;
  private List<JsonNode> apiDefinitions;
  private List<ConstraintMetadata> constraintMetadata;

  public ColumnMetadata getPrimaryKeyColumn() {
    return columnMetadata.stream().filter(ColumnMetadata::isPrimaryKey).reduce((c1, c2) -> {
      throw new UnsupportedOperationException("not supported");
    }).get();
  }
}
```

``` java
public class RelationMetadata {
  private UUID id;
  private String nameInSource;
  private String nameInTarget;
  private RelationType type;
  private String sourceTable;
  private String sourceColumn;
  private String targetTable;
  private String targetColumn;
}
```

```json
{
  "tableMetadata": [
    {
      "name": "account",
      "displayName": "Account",
      "columnMetadata": [
        {
          "id": "3bd37424-7bbe-4a49-bf2d-03b32101469c",
          "name": "id",
          "type": "BIGSERIAL",
          "primaryKey": true,
          "required": false,
          "unique": true,
          "uiHidden": false,
          "systemDefined": true
        },
        {
          "id": "b29a315c-f0d1-4ae8-b707-83a6812b4469",
          "name": "username",
          "type": "TEXT",
          "primaryKey": false,
          "required": true,
          "unique": true,
          "uiHidden": false,
          "systemDefined": true
        },
        {
          "id": "07db9c99-3bc8-41da-8a23-1474159da638",
          "name": "profile_image_id",
          "type": "BIGINT",
          "primaryKey": false,
          "required": true,
          "unique": true,
          "uiHidden": false,
          "systemDefined": true
        },
        {
          "id": "290d85f0-570e-47df-a59d-5b372c054111",
          "name": "password",
          "type": "TEXT",
          "primaryKey": false,
          "required": true,
          "unique": false,
          "uiHidden": true,
          "systemDefined": true
        },
        {
          "id": "69084c23-e6db-467a-87bb-4c6ea9e7cfdf",
          "name": "oauth2_user_info_map",
          "type": "JSONB",
          "primaryKey": false,
          "required": true,
          "unique": false,
          "uiHidden": true,
          "systemDefined": true
        },
        {
          "id": "75ad5118-c7a0-42bb-ab59-361bcaf9aa17",
          "name": "created_at",
          "type": "TIMESTAMPTZ",
          "primaryKey": false,
          "required": false,
          "unique": false,
          "uiHidden": false,
          "systemDefined": true
        },
        {
          "id": "85a79c8a-a6ca-4fa4-a6c5-36df0c0927ad",
          "name": "updated_at",
          "type": "TIMESTAMPTZ",
          "primaryKey": false,
          "required": true,
          "unique": false,
          "uiHidden": false,
          "systemDefined": true
        },
        {
          "id": "eb6ff340-e4a0-40e9-ab0b-8ebe779970fd",
          "name": "trigger_arg",
          "type": "JSONB",
          "primaryKey": false,
          "required": false,
          "unique": false,
          "uiHidden": true,
          "systemDefined": true
        }
      ],
      "apiDefinitions": [],
      "constraintMetadata": [
        {
          "name": "account_pkey",
          "primaryKeyColumns": ["id"]
        },
        {
          "name": "account_username_key",
          "compositeUniqueColumns": ["username"]
        }
      ]
    },
    {
      "name": "post",
      "displayName": "post",
      "columnMetadata": [
        {
          "id": "44324fb5-f090-4fa0-ad69-acd83633b4d3",
          "name": "id",
          "type": "BIGSERIAL",
          "primaryKey": true,
          "required": true,
          "unique": true,
          "uiHidden": false,
          "systemDefined": true
        },
        {
          "id": "72d933b2-679b-4f0f-9cc9-0004254c5f4e",
          "name": "trigger_arg",
          "type": "JSONB",
          "primaryKey": false,
          "required": false,
          "unique": false,
          "uiHidden": true,
          "systemDefined": true
        },
        {
          "id": "1d03d963-0494-49be-a09d-c4a8cea88128",
          "name": "created_at",
          "type": "TIMESTAMPTZ",
          "primaryKey": false,
          "required": false,
          "unique": false,
          "uiHidden": false,
          "systemDefined": true
        },
        {
          "id": "b77f6d7f-91c8-4e6c-ba86-f405939395b5",
          "name": "updated_at",
          "type": "TIMESTAMPTZ",
          "primaryKey": false,
          "required": false,
          "unique": false,
          "uiHidden": false,
          "systemDefined": true
        },
        {
          "name": "author_id",
          "type": "BIGINT",
          "required": true,
          "unique": false,
          "id": "e553a912-8298-4d08-a4c5-252f3d63863b",
          "primaryKey": false,
          "uiHidden": false
        },
        {
          "name": "content",
          "type": "TEXT",
          "required": false,
          "unique": false,
          "id": "f47d780e-7265-4039-98f8-d651699f32a4",
          "primaryKey": false,
          "uiHidden": false
        },
        {
          "id": "048da02b-07ea-473a-8cb7-ecdbb51a60c9",
          "name": "author_account",
          "type": "BIGINT",
          "primaryKey": false,
          "required": false,
          "unique": false,
          "uiHidden": false,
          "systemDefined": true
        },
        {
          "id": "f6a6d205-2ad2-447c-8332-43bfe993fb12",
          "name": "Timeline_timeline",
          "type": "BIGINT",
          "primaryKey": false,
          "required": false,
          "unique": true,
          "uiHidden": false,
          "systemDefined": true
        }
      ],
      "apiDefinitions": [],
      "constraintMetadata": [
        {
          "name": "comment_pkey",
          "primaryKeyColumns": ["id"]
        },
        {
          "name": "fkey_comment_to_post",
          "sourceUnitedColumns": ["targetPost_post"],
          "targetTable": "post",
          "targetUnitedColumns": ["id"]
        }
      ]
    },
    {
      "name": "comment",
      "displayName": "comment",
      "columnMetadata": [
        {
          "id": "c5cd4459-6c43-4448-9833-576180f98934",
          "name": "id",
          "type": "BIGSERIAL",
          "primaryKey": true,
          "required": true,
          "unique": true,
          "uiHidden": false,
          "systemDefined": true
        },
        {
          "id": "ab76b342-e388-4a39-939b-1d7be2e8c9ac",
          "name": "trigger_arg",
          "type": "JSONB",
          "primaryKey": false,
          "required": false,
          "unique": false,
          "uiHidden": true,
          "systemDefined": true
        },
        {
          "id": "4a72115e-79ed-4a47-8212-3061f8d32efb",
          "name": "created_at",
          "type": "TIMESTAMPTZ",
          "primaryKey": false,
          "required": false,
          "unique": false,
          "uiHidden": false,
          "systemDefined": true
        },
        {
          "id": "d96ad712-bd37-4b83-8eb0-ec0fe1c1f1fa",
          "name": "updated_at",
          "type": "TIMESTAMPTZ",
          "primaryKey": false,
          "required": false,
          "unique": false,
          "uiHidden": false,
          "systemDefined": true
        },
        {
          "name": "target_id",
          "type": "BIGINT",
          "required": true,
          "unique": false,
          "id": "9f6f5350-4d59-4537-a1c8-1414ddc1e30b",
          "primaryKey": false,
          "uiHidden": false
        },
        {
          "name": "user_id",
          "type": "BIGINT",
          "required": true,
          "unique": false,
          "id": "e841f681-3e0f-41ec-8ea0-9c873ff3c61d",
          "primaryKey": false,
          "uiHidden": false
        },
        {
          "id": "f832d9d9-ace4-4f78-8252-2aef621111b9",
          "name": "targetPost_post",
          "type": "BIGINT",
          "primaryKey": false,
          "required": false,
          "unique": false,
          "uiHidden": false,
          "systemDefined": true
        }
      ],
      "apiDefinitions": [],
      "constraintMetadata": [
        {
          "name": "post_pkey",
          "primaryKeyColumns": ["id"]
        },
        {
          "name": "fkey_post_to_account",
          "sourceUnitedColumns": ["author_account"],
          "targetTable": "account",
          "targetUnitedColumns": ["id"]
        },
        {
          "name": "fkey_post_to_timeline",
          "sourceUnitedColumns": ["Timeline_timeline"],
          "targetTable": "timeline",
          "targetUnitedColumns": ["id"]
        }
      ]
    },
    {
      "name": "timeline",
      "displayName": "timeline",
      "columnMetadata": [
        {
          "id": "b13ed5ae-fef4-4576-9728-eedf88a3f54b",
          "name": "id",
          "type": "BIGSERIAL",
          "primaryKey": true,
          "required": true,
          "unique": true,
          "uiHidden": false,
          "systemDefined": true
        },
        {
          "id": "078eea48-b81f-4ec9-86e3-8a9d59319257",
          "name": "trigger_arg",
          "type": "JSONB",
          "primaryKey": false,
          "required": false,
          "unique": false,
          "uiHidden": true,
          "systemDefined": true
        },
        {
          "id": "70f878b1-c382-4428-b106-5ba0846cc8e9",
          "name": "created_at",
          "type": "TIMESTAMPTZ",
          "primaryKey": false,
          "required": false,
          "unique": false,
          "uiHidden": false,
          "systemDefined": true
        },
        {
          "id": "4e85f5ae-5e3d-4788-af03-ab1a19f9cab9",
          "name": "updated_at",
          "type": "TIMESTAMPTZ",
          "primaryKey": false,
          "required": false,
          "unique": false,
          "uiHidden": false,
          "systemDefined": true
        }
      ],
      "apiDefinitions": [],
      "constraintMetadata": [
        {
          "name": "timeline_pkey",
          "primaryKeyColumns": ["id"]
        }
      ]
    }
  ],
  "relationMetadata": [
    {
      "nameInSource": "posts",
      "type": "ONE_TO_MANY",
      "nameInTarget": "author",
      "targetTable": "post",
      "sourceTable": "account",
      "sourceColumn": "id",
      "id": "3cb1ea9a-4bdf-4c49-8254-9ca393031175",
      "targetColumn": "author_account"
    },
    {
      "nameInSource": "comments",
      "type": "ONE_TO_MANY",
      "nameInTarget": "targetPost",
      "targetTable": "comment",
      "sourceTable": "post",
      "sourceColumn": "id",
      "id": "b109ecb9-e62f-4534-91b8-6d2eb72157b7",
      "targetColumn": "targetPost_post"
    },
    {
      "nameInSource": "Post",
      "type": "ONE_TO_ONE",
      "nameInTarget": "Timeline",
      "targetTable": "post",
      "sourceTable": "timeline",
      "sourceColumn": "id",
      "id": "e8fc132c-a5e4-4b3a-844f-4dee7e763808",
      "targetColumn": "Timeline_timeline"
    }
  ]
}
```

## 三、Dependency

```java
package com.example.demo.configuration;

import com.functorz.zero.datamodel.DataModel;
import com.functorz.zero.graphql.GraphQLApiManager;
import com.functorz.zero.utils.Utils;
import graphql.GraphQL;
import java.io.InputStream;
import javax.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.ResourceUtils;

@Configuration
public class GraphQLConfiguration {
  @Autowired
  private ApplicationContext applicationContext;

  private GraphQLApiManager apiManager;

  @PostConstruct
  public void init() {
    DataModel dataModel;
    try (InputStream inputStream = ResourceUtils.getURL("classpath:json/data.json").openStream()) {
      dataModel = Utils.OBJECT_MAPPER.readValue(inputStream, DataModel.class);
      apiManager = new GraphQLApiManager(dataModel, applicationContext);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  @Bean
  public GraphQL graphQL() {
    return apiManager.getGraphQL();
  }
}

```

**Note:** Beans of type **JdbcTemplate** and **PlatformTransactionManager** are requierd in ApplicationContext.

