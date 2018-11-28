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
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FilterMergePolicy;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SegmentReader;
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
 * This merge policy uses wrapped MergePolicy (default is TieredMergePolicy) for selecting regular merge segments
 *
 */
public class AddDocValuesMergePolicyFactory extends WrapperMergePolicyFactory {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final boolean skipIntegrityCheck;
  private final boolean noMerge;
  private final String marker;

  public static final String SKIP_INTEGRITY_CKECK_PROP = "skipIntegrityCheck";
  public static final String MARKER_PROP = "marker";
  public static final String NO_MERGE_PROP = "noMerge";

  public static final String DIAGNOSTICS_MARKER_PROP = "__addDVMarker__";
  public static final String DEFAULT_MARKER = AddDVOneMerge.class.getSimpleName();


  public AddDocValuesMergePolicyFactory(SolrResourceLoader resourceLoader, MergePolicyFactoryArgs args, IndexSchema schema) {
    super(resourceLoader, args, schema);
    final Boolean sic = (Boolean)args.remove(SKIP_INTEGRITY_CKECK_PROP);
    if (sic != null) {
      this.skipIntegrityCheck = sic.booleanValue();
    } else {
      this.skipIntegrityCheck = false;
    }
    Object m = args.remove(MARKER_PROP);
    if (m != null) {
      this.marker = String.valueOf(m);
    } else {
      this.marker = null;
    }
    final Boolean nm = (Boolean)args.remove(NO_MERGE_PROP);
    if (nm != null) {
      this.noMerge = nm.booleanValue();
    } else {
      this.noMerge = false;
    }
    if (!args.keys().isEmpty()) {
      throw new IllegalArgumentException("Arguments were "+args+" but "+getClass().getSimpleName()+" takes no such arguments.");
    }

    log.info("Using args: marker={}, noMerge={}, skipIntegrityCheck={}", marker, noMerge, skipIntegrityCheck);
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
  public MergePolicy getMergePolicyInstance(MergePolicy wrappedMP) {
    return new AddDVMergePolicy(wrappedMP, schema, marker, noMerge, skipIntegrityCheck);
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


  public static class AddDVMergePolicy extends FilterMergePolicy {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final IndexSchema schema;
    private final String marker;
    private final boolean skipIntegrityCheck;
    private final boolean noMerge;
    private final Map<String, Object> stats = new ConcurrentHashMap<>();

    AddDVMergePolicy(MergePolicy in, IndexSchema schema, String marker, boolean noMerge, boolean skipIntegrityCheck) {
      super(in);
      this.schema = schema;
      this.marker = marker;
      this.noMerge = noMerge;
      this.skipIntegrityCheck = skipIntegrityCheck;
    }

    public Map<String, Object> getStatistics() {
      return stats;
    }

    private void count(String name) {
      count(name, 1);
    }

    private void count(String name, int delta) {
      AtomicInteger counter = (AtomicInteger)stats.computeIfAbsent(name, n -> new AtomicInteger());
      counter.addAndGet(delta);
    }

    private MergeSpecification wrapMergeSpecification(String mergeType, MergeSpecification spec) throws IOException {
      if (spec == null || spec.merges.isEmpty()) {
        count("emptyMerge");
        return spec;
      }
      StringBuilder sb = new StringBuilder();
      count("mergesTotal", spec.merges.size());
      count(mergeType);
      for (int i = 0; i < spec.merges.size(); i++) {
        OneMerge oneMerge = spec.merges.get(i);
        if (oneMerge instanceof AddDVOneMerge) { // already wrapping
          continue;
        }
        int needWrapping = 0;
        sb.setLength(0);
        for (SegmentCommitInfo info : oneMerge.segments) {
          String segmentMarker = info.info.getDiagnostics().get(DIAGNOSTICS_MARKER_PROP);
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
          log.debug("-- OneMerge needs wrapping ({}/{}): {}", needWrapping, oneMerge.segments.size(), sb.toString());
          OneMerge wrappedOneMerge = new AddDVOneMerge(oneMerge.segments, schema, marker, skipIntegrityCheck);
          spec.merges.set(i, wrappedOneMerge);
          count("mergesWrapped", needWrapping);
        } else {
          if (log.isDebugEnabled()) {
            OneMerge nonWrappedOneMerge = new OneMerge(oneMerge.segments) {
              @Override
              public void setMergeInfo(SegmentCommitInfo info) {
                super.setMergeInfo(info);
                info.info.getDiagnostics().put("class", "AddDocValuesMergePolicy(nonWrapped)");
              }
            };
            spec.merges.set(i, nonWrappedOneMerge);
          }
        }
      }
      return spec;
    }

    @Override
    public MergeSpecification findMerges(MergeTrigger mergeTrigger, SegmentInfos segmentInfos, IndexWriter writer) throws IOException {
      if (noMerge) {
        count("noMerge");
        log.debug("findMerges: skipping, noMerge set");
        return null;
      }
      MergeSpecification spec = super.findMerges(mergeTrigger, segmentInfos, writer);
      return wrapMergeSpecification("findMerges", spec);
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
        String existingMarker = info.info.getDiagnostics().get(DIAGNOSTICS_MARKER_PROP);
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
            break;
          }
        }
        return shouldRewrite;
      } catch (IOException e) {
        // It's safer to rewrite the segment if there's an error, although it may lead to a lot of work.
        log.warn("Error opening a reader for segment {}, will rewrite segment", info.toString());
        count("shouldRewriteError");
        return true;
      }
    }

    @Override
    public MergeSpecification findForcedMerges(SegmentInfos infos, int maxSegmentCount, Map<SegmentCommitInfo, Boolean> segmentsToMerge, IndexWriter writer) throws IOException {
      if (noMerge) {
        log.debug("findForcedMerges: skipping, noMerge set");
        return null;
      }
      MergeSpecification spec = super.findForcedMerges(infos, maxSegmentCount, segmentsToMerge, writer);
      if (spec == null) {
        spec = new MergeSpecification();
      }

      // now find the stragglers and add them individually to the merge

      // don't take into account segments that are already being merged
      final Set<SegmentCommitInfo> merging = new HashSet<>(writer.getMergingSegments());
      // nor the ones already to be wrapped
      for (OneMerge om : spec.merges) {
        merging.addAll(om.segments);
      }

      Iterator<SegmentCommitInfo> iter = infos.iterator();
      while (iter.hasNext()) {
        SegmentCommitInfo info = iter.next();
        final Boolean isOriginal = segmentsToMerge.get(info);
        if (isOriginal == null || isOriginal == Boolean.FALSE || merging.contains(info)) {
          continue;
        } else {
          if (shouldRewrite(info)) {
            count("forcedMergeWrapped");
            log.info("--forceMerging {}", info.toString());
            spec.add(new AddDVOneMerge(Collections.singletonList(info), schema, marker, skipIntegrityCheck));
          }
        }
      }
      if (spec.merges.isEmpty()) {
        spec = null;
      }

      return wrapMergeSpecification("findForcedMerges", spec);
    }

    @Override
    public MergeSpecification findForcedDeletesMerges(SegmentInfos infos, IndexWriter writer) throws IOException {
      if (noMerge) {
        log.debug("findForcedDeletesMerges: skipping, noMerge set");
        return null;
      }
      MergeSpecification spec = super.findForcedDeletesMerges(infos, writer);
      return wrapMergeSpecification("findForcedDeletesMerges", spec);
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
      this.marker = marker != null ? marker : DEFAULT_MARKER;
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
      info.info.getDiagnostics().put(DIAGNOSTICS_MARKER_PROP, marker);
      super.setMergeInfo(info);
    }
  }
}
