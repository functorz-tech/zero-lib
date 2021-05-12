package com.functorz.zero.graphql;

public class GraphQLConstants {
  // logic op
  public static final String _AND = "_and";
  public static final String _NOT = "_not";
  public static final String _OR = "_or";

  // root query arg name
  public static final String WHERE = "where";
  public static final String ID = "id";
  public static final String ORDER_BY = "order_by";
  public static final String DISTINCT_ON = "distinct_on";
  public static final String OFFSET = "offset";
  public static final String LIMIT = "limit";

  public static final String COLUMNS = "columns";
  public static final String DISTINCT = "distinct";

  public static final String AGGREGATE = "aggregate";
  public static final String NODES = "nodes";

  public static final String AFFECTED_ROWS = "affected_rows";
  public static final String RETURNING = "returning";
  public static final String OBJECTS = "objects";
  public static final String OBJECT = "object";
  public static final String ON_CONFLICT = "on_conflict";
  public static final String CONSTRAINT = "constraint";
  public static final String UPDATE_COLUMNS = "update_columns";
  public static final String DATA = "data";
  public static final String _SET = "_set";
  public static final String _INC = "_inc";
  public static final String _APPEND = "_append";
  public static final String _DELETE_AT_PATH = "_delete_at_path";
  public static final String _DELETE_ELEM = "_delete_elem";
  public static final String _DELETE_KEY = "_delete_key";
  public static final String _PREPEND = "_prepend";
  public static final String PK_COLUMNS = "pk_columns";

  // type name format
  public static final String WHERE_TYPE_FORMAT = "%s_bool_exp";
  public static final String ORDER_BY_TYPE_NAME_FORMAT = "%s_order_by";
  public static final String QUERY_BY_PK_NAME_FORMAT = "%s_by_pk";
  public static final String AGGREGATE_QUERY_TYPE_NAME_FORMAT = "%s_aggregate";
  public static final String AGGREGATE_FIELD_TYPE_NAME_FORMAT = "%s_aggregate_fields";
  public static final String QUERY_DISTINCT_TYPE_NAME_FORMAT = "%s_select_column";
  public static final String COLUMN_CONDITION_INPUT_TYPE_NAME_FORMAT = "%s_comparison_exp";
  public static final String INSERT_INPUT_ARG_TYPE_NAME_FORMAT = "%s_insert_input";
  public static final String INSERT_ON_CONFLICT_ARG_TYPE_NAME_FORMAT = "%s_on_conflict";
  public static final String ON_CONFLICT_ARG_CONSTRAINT_TYPE_NAME_FORMAT = "%s_constraint";
  public static final String ON_CONFLICT_ARG_UPDATE_COLUMNS_TYPE_NAME_FORMAT = "%s_update_column";
  public static final String INSERT_OBJECT_RELATION_ARG_TYPE_NAME_FORMAT = "%s_obj_rel_insert_input";
  public static final String INSERT_ARRAY_RELATION_ARG_TYPE_NAME_FORMAT = "%s_arr_rel_insert_input";
  public static final String UPDATE_ARG_SET_TYPE_NAME_FORMAT = "%s_set_input";
  public static final String UPDATE_ARG_INC_TYPE_NAME_FORMAT = "%s_inc_input";
  public static final String UPDATE_ARG_APPEND_TYPE_NAME_FORMAT = "%s_append_input";
  public static final String UPDATE_ARG_DELETE_AT_PATH_TYPE_NAME_FORMAT = "%s_delete_at_path_input";
  public static final String UPDATE_ARG_DELETE_ELEM_TYPE_NAME_FORMAT = "%s_delete_elem_input";
  public static final String UPDATE_ARG_DELETE_KEY_TYPE_NAME_FORMAT = "%s_delete_key_input";
  public static final String UPDATE_ARG_PREPEND_TYPE_NAME_FORMAT = "%s_prepend_input";
  public static final String UPDATE_ARG_PK_COLUMNS_TYPE_NAME_FORMAT = "%s_pk_columns_input";

  // mutation name format
  public static final String DELETE_MUTATION_NAME_FORMAT = "delete_%s";
  public static final String DELETE_MUTATION_BY_PK_NAME_FORMAT = "delete_%s_by_pk";
  public static final String INSERT_MUTATION_NAME_FORMAT = "insert_%s";
  public static final String INSERT_ONE_MUTATION_NAME_FORMAT = "insert_%s_one";
  public static final String UPDATE_MUTATION_NAME_FORMAT = "update_%s";
  public static final String UPDATE_BY_PK_MUTATION_NAME_FORMAT = "update_%s_by_pk";
  public static final String MUTATION_RESPONSE_TYPE_NAME_FORMAT = "%s_mutation_response";

  // order_by
  public static final String ORDER_BY_ASC = "asc";
  public static final String ORDER_BY_ASC_NULLS_FIRST = "asc_nulls_first";
  public static final String ORDER_BY_ASC_NULLS_LAST = "asc_nulls_last";
  public static final String ORDER_BY_DESC = "desc";
  public static final String ORDER_BY_DESC_NULLS_FIRST = "desc_nulls_first";
  public static final String ORDER_BY_DESC_NULLS_LAST = "desc_nulls_last";

  // common comparsion name
  public static final String COMPARSION_EQ = "_eq";
  public static final String COMPARSION_GT = "_gt";
  public static final String COMPARSION_GTE = "_gte";
  public static final String COMPARSION_IN = "_in";
  public static final String COMPARSION_IS_NULL = "_is_null";
  public static final String COMPARSION_LT = "_lt";
  public static final String COMPARSION_LTE = "_lte";
  public static final String COMPARSION_NEQ = "_neq";
  public static final String COMPARSION_NIN = "_nin";

  // text specific comparsion name
  public static final String COMPARSION_ILIKE = "_ilike";
  public static final String COMPARSION_LIKE = "_like";
  public static final String COMPARSION_NILIKE = "_nilike";
  public static final String COMPARSION_NLIKE = "_nlike";
  public static final String COMPARSION_SIMILAR = "_similar";
  public static final String COMPARSION_NSIMILAR = "_nsimilar";

  // jsonb specifix comparsion name
  public static final String COMPARSION_HAS_KEY = "_has_key";
  public static final String COMPARSION_HAS_KEYS_ALL = "_has_keys_all";
  public static final String COMPARSION_HAS_KEYS_ANY = "_has_keys_any";

  // graphql scalar type name
  public static final String SCALAR_TYPE_LONG_NAME = "Long";
  public static final String SCALAR_TYPE_INTEGER_NAME = "Int";
  public static final String SCALAR_TYPE_FLOAT_NAME = "Float";
  public static final String SCALAR_TYPE_BIGDECIMAL_NAME = "BigDecimal";
  public static final String SCALAR_TYPE_STRING_NAME = "String";
  public static final String SCALAR_TYPE_BOOLEAN_NAME = "Boolean";
  public static final String SCALAR_TYPE_OFFSETDATETIME_NAME = "OffsetDateTime";
  public static final String SCALAR_TYPE_LOCALDATE_NAME = "LocalDate";
  public static final String SCALAR_TYPE_JSON_NAME = "Json";
}
