/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.index;

import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.index.MergePolicy;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.cloud.ZkSolrResourceLoader;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.util.SolrPluginUtils;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class PluggableMergePolicyFactory extends SimpleMergePolicyFactory implements SolrCoreAware {
  public static Logger log = LoggerFactory.getLogger(PluggableMergePolicyFactory.class);

  public static final String MERGE_POLICY_PROP = "mergePolicyFactory";
  private final MergePolicyFactory defaultMergePolicyFactory;
  private PluginInfo pluginInfo;

  public PluggableMergePolicyFactory(SolrResourceLoader resourceLoader, MergePolicyFactoryArgs args, IndexSchema schema) {
    super(resourceLoader, args, schema);
    defaultMergePolicyFactory = new TieredMergePolicyFactory(resourceLoader, args, schema);
  }

  @Override
  public void inform(SolrCore core) {
    CloudDescriptor cd = core.getCoreDescriptor().getCloudDescriptor();
    if (cd == null) {
      log.info("Solr not in Cloud mode - using default MergePolicy");
      return;
    }
    // we can safely assume here that our loader is ZK enabled
    ZkController zkController = ((ZkSolrResourceLoader)resourceLoader).getZkController();
    DocCollection coll = zkController.getClusterState().getCollectionOrNull(cd.getCollectionName());
    if (coll == null) {
      log.warn("Can't find collection " + cd.getCollectionName() + " in cluster state");
      return;
    }
    Object o = coll.getProperties().get(MERGE_POLICY_PROP);
    if (o == null) {
      // XXX nocommit add cluster-level override
      return;
    }
    if (o instanceof String) {
      // simple class name, no args
      Map<String, Object> props = new HashMap<>();
      props.put(CoreAdminParams.NAME, MERGE_POLICY_PROP);
      props.put(FieldType.CLASS_NAME, String.valueOf(o));
      pluginInfo = new PluginInfo(MERGE_POLICY_PROP, props);
    } else if (o instanceof Map) {
      Map props = (Map)o;
      if (!props.containsKey(FieldType.CLASS_NAME)) {
        log.error("MergePolicy plugin info missing class name, using default: " + props);
        return;
      }
      pluginInfo = new PluginInfo(MERGE_POLICY_PROP, props);
    }
  }

  @Override
  protected MergePolicy getMergePolicyInstance() {
    if (pluginInfo != null) {
      String mpClassName = pluginInfo.className;
      MergePolicy policy = resourceLoader.newInstance(mpClassName, MergePolicy.class);
      SolrPluginUtils.invokeSetters(policy, pluginInfo.initArgs);
      return policy;
    } else {
      return defaultMergePolicyFactory.getMergePolicy();
    }
  }

}
