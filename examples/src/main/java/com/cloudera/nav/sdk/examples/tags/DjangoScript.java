package com.cloudera.nav.sdk.examples.tags;

import com.cloudera.nav.sdk.model.CustomIdGenerator;
import com.cloudera.nav.sdk.model.SourceType;
import com.cloudera.nav.sdk.model.annotations.MClass;
import com.cloudera.nav.sdk.model.annotations.MRelation;
import com.cloudera.nav.sdk.model.entities.EndPointProxy;
import com.cloudera.nav.sdk.model.entities.Entity;
import com.cloudera.nav.sdk.model.entities.EntityType;
import com.cloudera.nav.sdk.model.relations.RelationRole;
import com.google.common.base.Preconditions;

import org.apache.commons.lang.StringUtils;

@MClass(model = "Django_op")
public class DjangoScript extends Entity {

  public DjangoScript(String namespace) {
    // Because the namespace is given to input/output we ensure it
    // exists when it is used by adding it as a c'tor parameter
    Preconditions.checkArgument(StringUtils.isNotEmpty(namespace));
    setNamespace(namespace);
  }

  /**
   * The script template is uniquely defined by the name and the owner
   */
  @Override
  public String generateId() {
    return CustomIdGenerator.generateIdentity(getNamespace(), "MyCustom");
  }

  @Override
  public SourceType getSourceType() {
    return SourceType.SDK;
  }

  /**
   * The StetsonScript represents a template and is therefore always an
   * OPERATION entity
   */
  @Override
  public EntityType getEntityType() {
    return EntityType.OPERATION;
  }
}
