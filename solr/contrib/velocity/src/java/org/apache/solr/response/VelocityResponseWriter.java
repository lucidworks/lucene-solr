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
package org.apache.solr.response;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.io.Writer;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.ResourceBundle;

import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.SolrResponseBase;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.runtime.RuntimeConstants;
import org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader;
import org.apache.velocity.tools.ConversionUtils;
import org.apache.velocity.tools.generic.ComparisonDateTool;
import org.apache.velocity.tools.generic.DisplayTool;
import org.apache.velocity.tools.generic.EscapeTool;
import org.apache.velocity.tools.generic.ListTool;
import org.apache.velocity.tools.generic.LocaleConfig;
import org.apache.velocity.tools.generic.MathTool;
import org.apache.velocity.tools.generic.NumberTool;
import org.apache.velocity.tools.generic.ResourceTool;
import org.apache.velocity.tools.generic.SortTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.CommonParams.SORT;

public class VelocityResponseWriter implements QueryResponseWriter, SolrCoreAware {
  // init param names, these are _only_ loaded at init time (no per-request control of these)
  //   - multiple different named writers could be created with different init params
  public static final String TEMPLATE_BASE_DIR = "template.base.dir";
  public static final String PROPERTIES_FILE = "init.properties.file";
  public static final String PARAMS_RESOURCE_LOADER_ENABLED = "params.resource.loader.enabled";
  public static final String SOLR_RESOURCE_LOADER_ENABLED = "solr.resource.loader.enabled";

  // request param names
  public static final String TEMPLATE = "v.template";
  public static final String LAYOUT = "v.layout";
  public static final String LAYOUT_ENABLED = "v.layout.enabled";
  public static final String CONTENT_TYPE = "v.contentType";
  public static final String JSON = "v.json";
  public static final String LOCALE = "v.locale";

  public static final String TEMPLATE_EXTENSION = ".vm";
  public static final String DEFAULT_CONTENT_TYPE = "text/html;charset=UTF-8";
  public static final String JSON_CONTENT_TYPE = "application/json;charset=UTF-8";

  private File fileResourceLoaderBaseDir;
  private String initPropertiesFileName;  // used just to hold from init() to inform()

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final SolrVelocityLogger velocityLogger = new SolrVelocityLogger(log);
  private Properties velocityInitProps = new Properties();
  private Map<String,String> customTools = new HashMap<String,String>();

  @Override
  public void init(NamedList args) {
  }

  @Override
  public void inform(SolrCore core) {
  }

  @Override
  public String getContentType(SolrQueryRequest request, SolrQueryResponse response) {
	return null;
  }

  @Override
  public void write(Writer writer, SolrQueryRequest request, SolrQueryResponse response) throws IOException {
  }

  private VelocityContext createContext(SolrQueryRequest request, SolrQueryResponse response) {
	return null;
  }

  private VelocityEngine createEngine(SolrQueryRequest request) {
    return null;
  }

  private Template getTemplate(VelocityEngine engine, SolrQueryRequest request) throws IOException {
    return null;
  }

  private String getJSONWrap(String xmlResult) {  // maybe noggit or Solr's JSON utilities can make this cleaner?
	return null;
  }

  private static class SolrVelocityResourceTool extends ResourceTool {
  }
}
