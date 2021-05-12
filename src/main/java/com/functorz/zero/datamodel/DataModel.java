package com.functorz.zero.datamodel;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class DataModel {
  private List<TableMetadata> tableMetadata;
  private List<RelationMetadata> relationMetadata;
}
