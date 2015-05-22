package com.cloudera.nav.plugin.model.entities;

import static org.junit.Assert.assertEquals;

import com.cloudera.nav.plugin.model.MD5IdGenerator;
import com.cloudera.nav.plugin.model.Source;
import com.cloudera.nav.plugin.model.SourceType;

import org.junit.*;

public class EntityTest {

  @Test
  public void testHdfsEntity() {
    Source hdfs1 = new Source("HDFS-1", SourceType.HDFS, "Cluster",
        "http://ns1");
    HdfsEntity entity = new HdfsEntity();
    entity.setType(EntityType.DIRECTORY);
    entity.setFileSystemPath("/user/test");
    entity.setSourceId(hdfs1.getIdentity());
    String id = MD5IdGenerator.generateIdentity(hdfs1.getIdentity(),
        entity.getFileSystemPath());
    assertEquals(id, entity.generateId());
  }

  @Test
  public void testHiveTable() {
    Source hive1 = new Source("HIVE-1", SourceType.HIVE, "Cluster",
        "http://hive-server:port");
    HiveTable entity = new HiveTable();
    entity.setDatabaseName("db");
    entity.setTableName("table");
    entity.setSourceId(hive1.getIdentity());
    String id = MD5IdGenerator.generateIdentity(hive1.getIdentity(),
        entity.getDatabaseName(), entity.getTableName());
    assertEquals(id, entity.generateId());
  }

  @Test
  public void testHiveColumn() {
    Source hive1 = new Source("HIVE-1", SourceType.HIVE, "Cluster",
        "http://hive-server:port");
    HiveColumn entity = new HiveColumn();
    entity.setDatabaseName("db");
    entity.setTableName("table");
    entity.setColumnName("column");
    entity.setSourceId(hive1.getIdentity());
    String id = MD5IdGenerator.generateIdentity(hive1.getIdentity(),
        entity.getDatabaseName(), entity.getTableName(),
        entity.getColumnName());
    assertEquals(id, entity.generateId());
  }
}
