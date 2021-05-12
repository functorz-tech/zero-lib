package com.functorz.zero.datamodel.constraint;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.io.Serializable;

public interface ConstraintMetadata extends Serializable {
  @JsonIgnore
  default ConstraintType getType() {
    return ConstraintType.parseTypeFromClass(this.getClass());
  }

  String getName();
}
