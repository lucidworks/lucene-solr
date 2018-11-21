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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.uninverting.UninvertingReader;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A merge policy that can detect schema changes and  write docvalues into merging segments when a field has docvalues enabled
 * Using UninvertingReader.
 *
 * This merge policy uses TieredMergePolicy for selecting regular merge segments
 *
 */
public class AddDocValuesMergePolicyFactory extends SimpleMergePolicyFactory {

  final private boolean skipIntegrityCheck;
  final private String marker;

  public static final String MARKER_PROP = "__addDVMarker__";

  public AddDocValuesMergePolicyFactory(SolrResourceLoader resourceLoader, MergePolicyFactoryArgs args, IndexSchema schema) {
    super(resourceLoader, args, schema);
    final Boolean sic = (Boolean)args.remove("skipIntegrityCheck");
    if (sic != null) {
      this.skipIntegrityCheck = sic.booleanValue();
    } else {
      this.skipIntegrityCheck = false;
    }
    Object m = args.remove("marker");
    if (m != null) {
      this.marker = String.valueOf(m);
    } else {
      this.marker = null;
    }
    if (!args.keys().isEmpty()) {
      throw new IllegalArgumentException("Arguments were "+args+" but "+getClass().getSimpleName()+" takes no arguments.");
    }
  }

  /**
   * Whether or not the wrapped docValues producer should check consistency
   */
  public boolean getSkipIntegrityCheck() {
    return skipIntegrityCheck;
  }

  /**
   * Marker to use for marking already converted segments.
   * If not null then only segments that don't contain this marker value will be rewritten.
   * If null then only segments without any marker value will be rewritten.
   */
  public String getMarker() {
    return marker;
  }

  @Override
  public MergePolicy getMergePolicyInstance() {
    return new AddDVMergePolicy(schema, marker, skipIntegrityCheck);
  }

  private static UninvertingReader.Type getUninversionType(IndexSchema schema, FieldInfo fi) {
    SchemaField sf = schema.getFieldOrNull(fi.name);

    if (sf != null &&
        sf.hasDocValues() &&
        fi.getIndexOptions() != IndexOptions.NONE) {
      return sf.getType().getUninversionType(sf);
    } else {
      return null;
    }
  }


  public static class AddDVMergePolicy extends TieredMergePolicy {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final IndexSchema schema;
    private final String marker;
    private final boolean skipIntegrityCheck;

    AddDVMergePolicy(IndexSchema schema, String marker, boolean skipIntegrityCheck) {
      this.schema = schema;
      this.marker = marker;
      this.skipIntegrityCheck = skipIntegrityCheck;
    }

    @Override
    public MergeSpecification findMerges(MergeTrigger mergeTrigger, SegmentInfos segmentInfos, IndexWriter writer) throws IOException {
      MergeSpecification spec = super.findMerges(mergeTrigger, segmentInfos, writer);
      if (spec == null || spec.merges.isEmpty()) {
        return spec;
      }
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < spec.merges.size(); i++) {
        OneMerge oneMerge = spec.merges.get(i);
        int needWrapping = 0;
        sb.setLength(0);
        for (SegmentCommitInfo info : oneMerge.segments) {
          String segmentMarker = info.info.getDiagnostics().get(MARKER_PROP);
          String source = info.info.getDiagnostics().get("source");
          if (segmentMarker == null || (marker != null && !marker.equals(segmentMarker))) {
            needWrapping++;
            if (sb.length() > 0) {
              sb.append(' ');
            }
            sb.append(info.toString() + "(" + source + ")");
          }
        }
        if (needWrapping > 0) {
          log.info("-- OneMerge needs wrapping (" + needWrapping + "/" + oneMerge.segments.size() +
              "): " + sb.toString());
          OneMerge wrappedOneMerge = new AddDVOneMerge(oneMerge.segments, schema, marker, skipIntegrityCheck);
          spec.merges.set(i, wrappedOneMerge);
        }
      }
      return spec;
    }

    /**
     *
     * @param info The segment to be examined
     * @return true - the schema has docValues=true for at least one field and the segment does _not_ have docValues
     *                information for that field, therefore it should be rewritten
     *         false - the schema and segment information agree, i.e. all fields with docValues=true in the schema
     *                 have the corresponding information already in the segment.
     */

    public boolean shouldRewrite(SegmentCommitInfo info) {
      // Need to get a reader for this segment
      try (SegmentReader reader = new SegmentReader(info, IOContext.DEFAULT)) {
        // check the marker, if defined
        String existingMarker = info.info.getDiagnostics().get(MARKER_PROP);
        if (existingMarker != null) {
          if (marker == null || marker.equals(existingMarker)) {
            return false;
          }
        }
        // iterating over schema fields makes it easier in the future
        // to synthesize other missing reader's fields
        boolean shouldRewrite = false;
        for (Map.Entry<String, SchemaField> ent : schema.getFields().entrySet()) {
          FieldInfo fi = reader.getFieldInfos().fieldInfo(ent.getKey());
          if (fi != null && getUninversionType(schema, fi) != null) {
            shouldRewrite = true;
          }
        }
        return shouldRewrite;
      } catch (IOException e) {
        // It's safer to rewrite the segment if there's an error, although it may lead to a lot of work.
        log.warn("Could not get a reader for field {}, will rewrite segment", info.toString());

        return true;
      }
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
            spec.add(new AddDVOneMerge(Collections.singletonList(info), schema, marker, skipIntegrityCheck));
          }
        }
      }
      return (spec.merges.size() == 0) ? null : spec;
    }
  }

  private static class AddDVOneMerge extends MergePolicy.OneMerge {

    private final String marker;
    private final IndexSchema schema;
    private final boolean skipIntegrityCheck;

    public AddDVOneMerge(List<SegmentCommitInfo> segments, IndexSchema schema, String marker,
                         final boolean skipIntegrityCheck) {
      super(segments);
      this.schema = schema;
      this.marker = marker != null ? marker : this.getClass().getSimpleName();
      this.skipIntegrityCheck = skipIntegrityCheck;
    }

    @Override
    public CodecReader wrapForMerge(CodecReader reader) throws IOException {
      // Wrap the reader with an uninverting reader if
      // Schema says there should be
      // NOTE: this converts also fields that already have docValues to
      // update their values to the current schema type


      Map<String, UninvertingReader.Type> uninversionMap = null;

      for (FieldInfo fi : reader.getFieldInfos()) {
        final UninvertingReader.Type type = getUninversionType(schema, fi);
        if (type != null) {
          if (uninversionMap == null) {
            uninversionMap = new HashMap<>();
          }
          uninversionMap.put(fi.name, type);
        }

      }

      if (uninversionMap == null) {
        // should not happen - we already check that we need to uninvert some fields
        return reader; // Default to normal reader if nothing to uninvert
      } else {
        return new UninvertingFilterCodecReader(reader, uninversionMap, skipIntegrityCheck);
      }
    }

    @Override
    public List<CodecReader> getMergeReaders() throws IOException {
      List<CodecReader> mergeReaders = super.getMergeReaders();
      List<CodecReader> newMergeReaders = new ArrayList<>(mergeReaders.size());
      for (CodecReader r : mergeReaders) {
        newMergeReaders.add(wrapForMerge(r));
      }
      return newMergeReaders;
    }

    @Override
    public void setMergeInfo(SegmentCommitInfo info) {
      info.info.getDiagnostics().put(MARKER_PROP, marker);
      super.setMergeInfo(info);
    }
  }
}
