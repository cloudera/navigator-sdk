package com.cloudera.nav.plugin.client;

import com.cloudera.nav.plugin.model.SourceType;
import com.cloudera.nav.plugin.model.annotations.MClass;
import com.cloudera.nav.plugin.model.annotations.MProperty;
import com.cloudera.nav.plugin.model.annotations.MRelation;
import com.cloudera.nav.plugin.model.entities.CustomEntity;
import com.cloudera.nav.plugin.model.entities.EntityType;
import com.cloudera.nav.plugin.model.relations.RelationRole;

/**
 * Represents a template defined by a script in a custom DSL
 */
@MClass
public class CustomOperation extends CustomEntity {

  private String pigOperationId;
  private String script;

  /**
   * Extend to include all fields that uniquely determine a custom entity
   */
  @Override
  protected String[] getIdComponents() {
    return new String[] { getName(), getOwner() };
  }

  @MRelation(role= RelationRole.PHYSICAL, sourceType= SourceType.PIG)
  public String getPigOperationId() {
    return pigOperationId;
  }

  @Override
  @MProperty
  public EntityType getType() {
    return EntityType.OPERATION;
  }

  public void setPigOperationId(String pigOperationId) {
    this.pigOperationId = pigOperationId;
  }

  @MProperty
  public String getScript() {
    return script;
  }

  public void setScript(String script) {
    this.script = script;
  }
}
