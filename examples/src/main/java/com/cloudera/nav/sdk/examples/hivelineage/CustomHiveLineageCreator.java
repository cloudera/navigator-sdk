// (c) Copyright 2017 Cloudera, Inc. All rights reserved.
package com.cloudera.nav.sdk.examples.hivelineage;

import com.cloudera.nav.sdk.client.NavigatorPlugin;
import com.cloudera.nav.sdk.client.writer.ResultSet;
import com.cloudera.nav.sdk.model.entities.Entity;
import com.cloudera.nav.sdk.model.entities.HiveDatabase;
import com.cloudera.nav.sdk.model.entities.HiveOperation;
import com.cloudera.nav.sdk.model.entities.HiveTable;

public class CustomHiveLineageCreator {

  public static void main(String[] args) {
    CustomHiveLineageCreator lineageCreator =
        new CustomHiveLineageCreator(args[0]);
    lineageCreator.run();
  }

  protected final NavigatorPlugin plugin;

  public CustomHiveLineageCreator(String configFilePath) {
    this.plugin = NavigatorPlugin.fromConfigFile(configFilePath);
  }

  public void run() {
    // register all models in example
    plugin.registerModels(getClass().getPackage().getName());

    /*R2D2Script script = createR2D2Script(createHiveTable("db_cjmexitter",
        "tbl_cjmexitter_1"),"temp_table");
    script.setIdentity(script.generateId());

    // Write metadata
    ResultSet results = plugin.write(script);

    if (results.hasErrors()) {
      throw new RuntimeException(results.toString());
    }

    R2D2Script script2 = createR2D2Script(createHiveDatabase(
        "db_cjmexitter"), "temp_database");

    results = plugin.write(script2);

    if (results.hasErrors()) {
      throw new RuntimeException(results.toString());
    }*/

    R2D2Script script3 = createR2D2Script(createHiveOperation("select " +
        "description from sample_08 where salary > 20"), "Hive OP1");

    ResultSet results = plugin.write(script3);

    if (results.hasErrors()) {
      throw new RuntimeException(results.toString());
    }
  }

  private R2D2Script createR2D2Script(Entity hiveEntity, String
      entityName) {
    R2D2Script script = new R2D2Script(plugin.getNamespace());
    script.setHiveEntity(hiveEntity);
    script.setName(entityName);
    script.setOwner("Aadarsh");
    script.setDescription("The Last Jedi");
    return script;
  }

  /**
   * This method returns a HiveTable object.
   * @param databaseName
   * @param tableName
   * @return
   */
  private Entity createHiveTable(String databaseName, String tableName) {

    return new HiveTable(plugin.getClient().getHMSSource().getIdentity(),
        databaseName, tableName);
  }

  /**
   * This method returns a Hive Database
   * @param databaseName
   * @return
   */
  private Entity createHiveDatabase(String databaseName) {
    return new HiveDatabase(plugin.getClient().getHMSSource().getIdentity(),
        databaseName);
  }


  /**
   * This method returns a Hive Database
   * @param databaseName
   * @return
   */
  private Entity createHiveOperation(String queryText) {
    return new HiveOperation(plugin.getClient().getHMSSource().getIdentity(),
        queryText);
  }
}
