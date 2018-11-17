package org.apache.solr.index;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.schema.SchemaResponse;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.Utils;
import org.apache.solr.schema.FieldType;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class IndexUpgradeIntegrationTest extends AbstractFullDistribZkTestBase {
  private static final Logger log = LoggerFactory.getLogger(IndexUpgradeIntegrationTest.class);

  private static String ID_FIELD = "id";
  private static String TEST_FIELD = "string_add_dv_later";

  public IndexUpgradeIntegrationTest() {
    schemaString = "schema-docValues.xml";
  }

  @Override
  protected String getCloudSolrConfig() {
    return "solrconfig-pluggablemergepolicyfactory.xml";
  }


  @Test
  public void testIndexUpgrade() throws Exception {
    String collectionName = "indexUpgrade_test";
    CollectionAdminRequest.Create createCollectionRequest = new CollectionAdminRequest.Create()
        .setCollectionName(collectionName)
        .setNumShards(2)
        .setReplicationFactor(1)
        .setConfigName("conf1");
    CollectionAdminResponse response = createCollectionRequest.process(cloudClient);
    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());
    Thread.sleep(5000);

    // set plugin configuration
    Map<String, Object> pluginProps = new HashMap<>();
    pluginProps.put(FieldType.CLASS_NAME, UninvertDocValuesMergePolicyFactory.class.getName());
    pluginProps.put(WrapperMergePolicyFactory.WRAPPED_PREFIX, "addDV");
    pluginProps.put("addDV.class", AddDocValuesMergePolicyFactory.class.getName());
    String propValue = Utils.toJSONString(pluginProps);
    CollectionAdminRequest.ClusterProp clusterProp = new CollectionAdminRequest.ClusterProp()
        .setPropertyName(PluggableMergePolicyFactory.MERGE_POLICY_PROP + collectionName)
        .setPropertyValue(propValue);
    clusterProp.process(cloudClient);

    log.info("-- completed set cluster props");


    cloudClient.setDefaultCollection(collectionName);
    // this indexes still without DV because plugin props haven't taken effect yet

    // create several segments
    for (int i = 0; i < 1000; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField(ID_FIELD, i);
      doc.addField(TEST_FIELD, String.valueOf(i));
      cloudClient.add(doc);
      if (i > 0 && i % 200 == 0) {
        cloudClient.commit();
      }
    }
    cloudClient.commit();

    // retrieve current schema
    SchemaRequest schemaRequest = new SchemaRequest();
    SchemaResponse schemaResponse = schemaRequest.process(cloudClient);
    Map<String, Object> field = getSchemaField(TEST_FIELD, schemaResponse);
    assertNotNull("missing " + TEST_FIELD + " field", field);
    assertEquals("wrong flags: " + field, Boolean.FALSE, field.get("docValues"));

    // update schema
    field.put("docValues", true);
    SchemaRequest.ReplaceField replaceRequest = new SchemaRequest.ReplaceField(field);
    SchemaResponse.UpdateResponse replaceResponse = replaceRequest.process(cloudClient);

    log.info("-- completed schema update");

    // bounce the collection
    Map<String, Long> urlToTimeBefore = new HashMap<>();
    collectStartTimes(collectionName, cloudClient, urlToTimeBefore);
    CollectionAdminRequest<CollectionAdminRequest.Reload> reload = new CollectionAdminRequest.Reload()
        .setCollectionName(collectionName);
    reload.process(cloudClient);

    boolean reloaded = waitForReloads(collectionName, cloudClient, urlToTimeBefore);
    assertTrue("could not reload collection in time", reloaded);

    log.info("-- completed collection reload");

    // verify that schema doesn't match the actual fields anymore
    CollectionAdminRequest<CollectionAdminRequest.ColStatus> status = new CollectionAdminRequest.ColStatus()
        .setCollectionName(collectionName)
        .setWithFieldInfos(true)
        .setWithSegments(true);
    CollectionAdminResponse rsp = status.process(cloudClient);
    List<String> nonCompliant = (List<String>)rsp.getResponse().findRecursive(collectionName, "schemaNonCompliant");
    assertNotNull("nonCompliant missing: " + rsp, nonCompliant);
    assertEquals("nonCompliant: " + nonCompliant, 1, nonCompliant.size());
    assertEquals("nonCompliant: " + nonCompliant, TEST_FIELD, nonCompliant.get(0));

    log.info("-- start optimize");
    // request optimize to make sure all segments are rewritten
    cloudClient.optimize(collectionName, true, true, 1);
    cloudClient.commit();

    log.info("-- completed optimize");

    rsp = status.process(cloudClient);
    nonCompliant = (List<String>)rsp.getResponse().findRecursive(collectionName, "schemaNonCompliant");
    assertNotNull("nonCompliant missing: " + rsp, nonCompliant);
    assertEquals("nonCompliant: " + nonCompliant, 1, nonCompliant.size());
    assertEquals("nonCompliant: " + nonCompliant, "(NONE)", nonCompliant.get(0));
  }

  private Map<String, Object> getSchemaField(String name, SchemaResponse schemaResponse) {
    List<Map<String, Object>> fields = schemaResponse.getSchemaRepresentation().getFields();
    for (Map<String, Object> field : fields) {
      if (name.equals(field.get("name"))) {
        return field;
      }
    }
    return null;
  }
}
