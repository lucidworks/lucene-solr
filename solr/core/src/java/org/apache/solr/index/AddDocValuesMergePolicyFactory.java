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
import java.util.List;
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

/**
 * A merge policy to add docValues to existing indexes where fields have docValues added to the schema after some
 * documents have already been indexed.
 * <p>
 * When this merge policy is in effect, calling optimize/forceMerge will rewrite all segments (singleton merges) and
 * add docValues to the new segment. All operations except optimize/forceMerge are handled by the superclass
 * (TieredMergePolicy), see the AddDocValuesMergePolicy below.
 * <p>
 * No segments are merged during the optimize/forceMerge.
 * <p>
 * This merge policy can be configured in solrconfig.xml or used to temporarily override the merge policy on a
 * per-collection basis to add docValues one collection at a time.
 * <p>
 * This factory can be used when indexing is active.
 */

//nocommit. ab: Can we put a better example in here and maybe a little example of how to use the pluggable bits in
// SolrCloud?

public class AddDocValuesMergePolicyFactory extends MergePolicyFactory {

  public static final String REWRITE_INFO_PROP = "rewriteInfo";

  public AddDocValuesMergePolicyFactory(SolrResourceLoader resourceLoader, MergePolicyFactoryArgs args, IndexSchema schema) {
    super(resourceLoader, args, schema);
    // parse any arguments here if there are any.
    if (!args.keys().isEmpty()) {
      throw new IllegalArgumentException("Arguments were " + args + " but " + getClass().getSimpleName() + " takes no arguments.");
    }
  }

  @Override
  public MergePolicy getMergePolicy() {
    return new AddDocValuesMergePolicy(schema);
  }
}

/**
 * The actual implementation that drives rewriting all segments and adding docValues when necessary. When
 * optimize/forceMerge is called using this policy, every segment in which the schema has docValues defined for some
 * field but the segment does _not_ have docvalues currently will be rewritten with the docValues structures added to
 * the index on disk.
 *
 * Segments are not merged. Every segment that is rewritten is rewritten into a single segment (singleton merges)
 *
 * To be effective, the field(s) in question must have their values indexed. If the values are _not_ indexed, then
 * the docValues for that document/field combination will be empty.
 */
class AddDocValuesMergePolicy extends TieredMergePolicy implements RewriteSegments {

  private IndexSchema schema;
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private String rewriteInfo;

  AddDocValuesMergePolicy(IndexSchema schema) {
    this.schema = schema;
  }


  /**
   *
   * @param info The segment to be examined
   * @return true - the schema has docValues=true for at least one field and the segment does _not_ have docValues
   *                information for that field, therefore it should be rewritten
   *         false - the schema and segment information agree, i.e. all fields with docValues=true in the schema
   *                 have the corresponding information already in the segment.
   */

  @Override
  public boolean shouldRewrite(SegmentCommitInfo info) {
    // Need to get a reader for this segment
    try (SegmentReader reader = new SegmentReader(info, IOContext.DEFAULT)) {
      StringBuilder fieldsToRewrite = new StringBuilder();
      boolean shouldRewrite = false;
      // nocommit should this iterate over the reader's fields instead?
      // maybe not - iterating over schema fields makes it easier in the future
      // to synthesize other missing reader's fields
      for (Map.Entry<String, SchemaField> ent : schema.getFields().entrySet()) {
        SchemaField sf = ent.getValue();
        FieldInfo fi = reader.getFieldInfos().fieldInfo(ent.getKey());
        if (null != sf && fi != null &&
            sf.hasDocValues() &&
            fi.getDocValuesType() == DocValuesType.NONE &&
            fi.getIndexOptions() != IndexOptions.NONE) {
          shouldRewrite = true;
          if (fieldsToRewrite.length() > 0) {
            fieldsToRewrite.append(' ');
          }
          fieldsToRewrite.append(fi.name);
        }
      }
      if (shouldRewrite) {
        // nocommit not sure about the concurrency here
        rewriteInfo = fieldsToRewrite.toString();
      } else {
        rewriteInfo = null;
      }
      return shouldRewrite;
    } catch (IOException e) {
      // It's safer to rewrite the segment if there's an error, although it may lead to a lot of work.
      log.warn("Could not get a reader for field {}, will rewrite segment", info.toString());

      return true;
    }
  }

  // See superclass javadocs.
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
          spec.add(new AddDocValuesOneMerge(Collections.singletonList(info), rewriteInfo));
        }
      }
    }
    return (spec.merges.size() == 0) ? null : spec;
  }
}

class AddDocValuesOneMerge extends MergePolicy.OneMerge {

  private final String rewriteInfo;
  /**
   * Sole constructor.
   *
   * @param segments List of {@link SegmentCommitInfo}s
   *                 to be merged.
   * @param rewriteInfo diagnostic information about the reason for rewrite
   */
  public AddDocValuesOneMerge(List<SegmentCommitInfo> segments, String rewriteInfo) {
    super(segments);
    this.rewriteInfo = rewriteInfo;
  }

  @Override
  public void setMergeInfo(SegmentCommitInfo info) {
    info.info.getDiagnostics().put(AddDocValuesMergePolicyFactory.REWRITE_INFO_PROP, rewriteInfo);
    super.setMergeInfo(info);
  }
}

