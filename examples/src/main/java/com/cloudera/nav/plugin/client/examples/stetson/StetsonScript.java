package com.cloudera.nav.plugin.client.examples.stetson;

import com.cloudera.nav.plugin.model.SourceType;
import com.cloudera.nav.plugin.model.annotations.MClass;
import com.cloudera.nav.plugin.model.annotations.MProperty;
import com.cloudera.nav.plugin.model.annotations.MRelation;
import com.cloudera.nav.plugin.model.entities.CustomEntity;
import com.cloudera.nav.plugin.model.entities.EntityType;
import com.cloudera.nav.plugin.model.relations.RelationRole;

/**
 * Represents a template defined by a script in a hypothetical custom DSL
 */
@MClass
public class StetsonScript extends CustomEntity {

  private String pigOperationId;
  private String script;

  /**
   * The StetsonScript represents a template and is therefore always an
   * OPERATION entity
   */
  @Override
  @MProperty
  public EntityType getType() {
    return EntityType.OPERATION;
  }

  /**
   * The script template is uniquely defined by the name and the owner
   */
  @Override
  protected String[] getIdComponents() {
    return new String[] { getName(), getOwner() };
  }

  /**
   * The StetsonScript is linked to a PIG operation via a Logical-Physical
   * relationship where the Pig operation is the PHYSICAL node
   */
  @MRelation(role= RelationRole.PHYSICAL, sourceType= SourceType.PIG)
  public String getPigOperationId() {
    return pigOperationId;
  }

  /**
   * The script contents in the custom DSL
   */
  @MProperty
  public String getScript() {
    return script;
  }

  public void setPigOperationId(String pigOperationId) {
    this.pigOperationId = pigOperationId;
  }

  public void setScript(String script) {
    this.script = script;
  }
}
