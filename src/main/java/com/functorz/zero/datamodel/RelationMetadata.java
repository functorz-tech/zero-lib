package com.functorz.zero.datamodel;

import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
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
