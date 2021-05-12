package com.functorz.zero.datamodel.constraint;

public enum ConstraintType {
  NOT_NULL(NotNullConstraint.class),
  UNIQUE(UniqueConstraint.class),
  PRIMARY_KEY(PrimaryKeyConstraint.class),
  FOREIGN_KEY(ForeignKeyConstraint.class),
  ;

  private Class<? extends ConstraintMetadata> clazz;

  ConstraintType(Class<? extends ConstraintMetadata> clazz) {
    this.clazz = clazz;
  }

  public static ConstraintType parseTypeFromClass(Class<? extends ConstraintMetadata> clazz) {
    for (ConstraintType type : ConstraintType.values()) {
      if (clazz == type.clazz) {
        return type;
      }
    }
    throw new IllegalStateException();
  }
}
