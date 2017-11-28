// (c) Copyright 2017 Cloudera, Inc. All rights reserved.
package com.cloudera.nav.sdk.examples.hivelineage;

import com.cloudera.nav.sdk.model.CustomIdGenerator;
import com.cloudera.nav.sdk.model.SourceType;
import com.cloudera.nav.sdk.model.annotations.MClass;
import com.cloudera.nav.sdk.model.annotations.MRelation;
import com.cloudera.nav.sdk.model.entities.EndPointProxy;
import com.cloudera.nav.sdk.model.entities.Entity;
import com.cloudera.nav.sdk.model.entities.EntityType;
import com.cloudera.nav.sdk.model.entities.HiveDatabase;
import com.cloudera.nav.sdk.model.entities.HiveOperation;
import com.cloudera.nav.sdk.model.entities.HiveTable;
import com.cloudera.nav.sdk.model.relations.RelationRole;
import com.google.common.base.Preconditions;

import org.apache.commons.lang.StringUtils;

@MClass(model = "stetson_op")
public class R2D2Script extends Entity {

  @MRelation(role = RelationRole.TARGET)
  private Entity hiveEntity;

  public R2D2Script(String namespace) {
    // Because the namespace is given to input/output we ensure it
    // exists when it is used by adding it as a c'tor parameter
    Preconditions.checkArgument(StringUtils.isNotEmpty(namespace));
    setNamespace(namespace);
  }

  /**
   * The script template is uniquely defined by the namespace, the name and
   * the attributes of the specific entity.
   */
  @Override
  public String generateId() {
    return CustomIdGenerator.generateIdentity(getNamespace(), getName(),
        hiveEntity.getIdAttrsMap().values().toString());
  }

  @Override
  public SourceType getSourceType() {
    return SourceType.SDK;
  }

  @Override
  public EntityType getEntityType() {
    return EntityType.FILE;
  }

  public Entity getHiveEntity() {
    return hiveEntity;
  }

  public void setHiveEntity(Entity hiveEntity) {
    this.hiveEntity = new EndPointProxy(
        hiveEntity.getIdAttrsMap(), hiveEntity.getSourceType(),
        hiveEntity.getEntityType());
    this.hiveEntity.setSourceId(hiveEntity.getSourceId());
  }
}
