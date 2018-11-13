package org.apache.solr.index;

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


import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.store.IOContext;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RewriteUninvertedSegmentsMergePolicyFactory extends MergePolicyFactory {

  public RewriteUninvertedSegmentsMergePolicyFactory(SolrResourceLoader resourceLoader, MergePolicyFactoryArgs args, IndexSchema schema) {
    super(resourceLoader, args, schema);
    //nocommit parse any arguments here.
    if (!args.keys().isEmpty()) {
      throw new IllegalArgumentException("Arguments were " + args + " but " + getClass().getSimpleName() + " takes no arguments.");
    }
  }

  @Override
  public MergePolicy getMergePolicy() {
    return new RewriteUninvertedSegmentsMergePolicy(schema);
  }
}

// Let all the usual functions work through the (default) TieredMergePolicy, only overriding the optimize step.
// nocommit would a different merge policy be better?
class RewriteUninvertedSegmentsMergePolicy extends TieredMergePolicy implements RewriteSegments {

  private IndexSchema schema;
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  RewriteUninvertedSegmentsMergePolicy(IndexSchema schema) {
    this.schema = schema;
  }

  @Override
  public boolean shouldRewrite(SegmentCommitInfo info) {
    //nocommit
    // Need to get a reader for this segment
    try (SegmentReader reader = new SegmentReader(info, IOContext.DEFAULT)) {
      for (Map.Entry<String, SchemaField> ent : schema.getFields().entrySet()) {
        SchemaField sf = ent.getValue();
        FieldInfo fi = reader.getFieldInfos().fieldInfo(ent.getKey());
        if (null != sf &&
            sf.hasDocValues() &&
            fi.getDocValuesType() == DocValuesType.NONE &&
            fi.getIndexOptions() != IndexOptions.NONE) {
          return true;
        }
      }
    } catch (IOException e) {
      //nocommit, this may lead to a lot of work.
      log.warn("Could not get a reader for field {}, will rewrite segment", info.toString());
      return true;
    }
    return false;
  }

  @Override
  public MergeSpecification findForcedMerges(SegmentInfos infos, int maxSegmentCount, Map<SegmentCommitInfo, Boolean> segmentsToMerge, IndexWriter writer) throws IOException {
    MergeSpecification spec = new MergeSpecification();

    final Set<SegmentCommitInfo> merging = new HashSet<>(writer.getMergingSegments());

    Iterator<SegmentCommitInfo> iter = infos.iterator();
    while (iter.hasNext()) {
      SegmentCommitInfo info = iter.next();
      final Boolean isOriginal = segmentsToMerge.get(info);
      if (isOriginal == null || isOriginal == false || merging.contains(info)) {
      } else {
        if (shouldRewrite(info)) {
          spec.add(new OneMerge(Collections.singletonList(info)));
        }
      }
    }
    return (spec.merges.size() == 0) ? null : spec;
  }
}

