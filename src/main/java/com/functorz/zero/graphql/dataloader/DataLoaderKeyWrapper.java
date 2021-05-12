package com.functorz.zero.graphql.dataloader;

import graphql.schema.SelectedField;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@AllArgsConstructor
@Getter
@NoArgsConstructor
@Setter
public class DataLoaderKeyWrapper {
  private Object key;
  private List<SelectedField> selectedFields;
  private Map<String, Object> arguments;
}
