package com.cloudera.nav.plugin.client;

import com.cloudera.nav.plugin.model.SourceType;
import com.cloudera.nav.plugin.model.annotations.MClass;
import com.cloudera.nav.plugin.model.annotations.MProperty;
import com.cloudera.nav.plugin.model.annotations.MRelation;
import com.cloudera.nav.plugin.model.entities.CustomEntity;
import com.cloudera.nav.plugin.model.entities.EntityType;
import com.cloudera.nav.plugin.model.relations.RelationRole;

/**
 * Represents a specific execution of a StetsonScript
 */
@MClass
public class CustomOperationExecution extends CustomEntity {

  private CustomOperation template;
  private Long startTime;
  // MD5(pig.script.id) from the job conf
  private String pigExecutionId;
  private String customOperationInstanceId;

  @Override
  @MProperty
  public EntityType getType() {
    return EntityType.OPERATION_EXECUTION;
  }

  @Override
  protected String[] getIdComponents() {
    return new String[] { getTemplate().getIdentity(),
        getCustomOperationInstanceId() };
  }

  @MRelation(role = RelationRole.INSTANCE)
  public CustomOperation getTemplate() {
    return template;
  }

  public void setTemplate(CustomOperation template) {
    this.template = template;
  }

  @MRelation(role = RelationRole.PHYSICAL, sourceType = SourceType.PIG)
  public String getPigExecutionId() {
    return pigExecutionId;
  }

  public void setPigExecutionId(String pigExecutionId) {
    this.pigExecutionId = pigExecutionId;
  }

  @MProperty
  public Long getStartTime() {
    return startTime;
  }

  public void setStartTime(Long startTime) {
    this.startTime = startTime;
  }

  @MProperty
  public String getCustomOperationInstanceId() {
    return customOperationInstanceId;
  }

  public void setCustomOperationId(String customOperationInstanceId) {
    this.customOperationInstanceId = customOperationInstanceId;
  }
}
