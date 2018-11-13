package org.apache.solr.handler.admin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergePolicy.MergeSpecification;
import org.apache.lucene.index.MergePolicy.OneMerge;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.util.Version;
import org.apache.solr.common.luke.FieldFlag;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.RefCounted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.lucene.index.IndexOptions.DOCS;
import static org.apache.lucene.index.IndexOptions.DOCS_AND_FREQS;
import static org.apache.lucene.index.IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS;
import static org.apache.solr.common.params.CommonParams.NAME;

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

/**
 * This handler exposes information about last commit generation segments
 */
public class SegmentsInfoRequestHandler extends RequestHandlerBase {
  private static Logger log = LoggerFactory.getLogger(SegmentsInfoRequestHandler.class);

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp)
      throws Exception {
    getSegmentsInfo(req, rsp);
    rsp.setHttpCaching(false);
  }

  private void getSegmentsInfo(SolrQueryRequest req, SolrQueryResponse rsp)
      throws Exception {
    SolrIndexSearcher searcher = req.getSearcher();
    IndexSchema schema = req.getSchema();

    SegmentInfos infos =
        SegmentInfos.readLatestCommit(searcher.getIndexReader().directory());
    SimpleOrderedMap<Object> infosInfo = new SimpleOrderedMap<>();
    Version minVersion = infos.getMinSegmentLuceneVersion();
    if (minVersion != null) {
      infosInfo.add("minSegmentLuceneVersion", minVersion.toString());
    }
    Version commitVersion = infos.getCommitLuceneVersion();
    if (commitVersion != null) {
      infosInfo.add("commitLuceneVersion", commitVersion.toString());
    }
    infosInfo.add("numSegments", infos.size());
    infosInfo.add("segmentsFileName", infos.getSegmentsFileName());
    infosInfo.add("userData", infos.userData);

    List<String> mergeCandidates = getMergeCandidatesNames(req, infos);

    SimpleOrderedMap<Object> segmentInfos = new SimpleOrderedMap<>();
    SimpleOrderedMap<Object> segmentInfo = null;
    boolean withFieldInfos = req.getParams().getBool("fieldInfos", false);

    List<LeafReaderContext> leafContexts = searcher.getIndexReader().leaves();
    for (SegmentCommitInfo segmentCommitInfo : infos) {
      segmentInfo = getSegmentInfo(segmentCommitInfo, withFieldInfos, leafContexts, schema);
      if (mergeCandidates.contains(segmentCommitInfo.info.name)) {
        segmentInfo.add("mergeCandidate", true);
      }
      segmentInfos.add((String) segmentInfo.get(NAME), segmentInfo);
    }

    rsp.add("info", infosInfo);
    rsp.add("segments", segmentInfos);
  }

  private SimpleOrderedMap<Object> getSegmentInfo(
      SegmentCommitInfo segmentCommitInfo, boolean withFieldInfos, List<LeafReaderContext> leafContexts, IndexSchema schema) throws IOException {
    SimpleOrderedMap<Object> segmentInfoMap = new SimpleOrderedMap<>();

    segmentInfoMap.add(NAME, segmentCommitInfo.info.name);
    segmentInfoMap.add("delCount", segmentCommitInfo.getDelCount());
    segmentInfoMap.add("sizeInBytes", segmentCommitInfo.sizeInBytes());
    segmentInfoMap.add("size", segmentCommitInfo.info.maxDoc());
    Long timestamp = Long.parseLong(segmentCommitInfo.info.getDiagnostics()
        .get("timestamp"));
    segmentInfoMap.add("age", new Date(timestamp));
    segmentInfoMap.add("source",
        segmentCommitInfo.info.getDiagnostics().get("source"));
    if (!segmentCommitInfo.info.getDiagnostics().isEmpty()) {
      segmentInfoMap.add("diagnostics", segmentCommitInfo.info.getDiagnostics());
    }
    if (!segmentCommitInfo.info.getAttributes().isEmpty()) {
      segmentInfoMap.add("attributes", segmentCommitInfo.info.getAttributes());
    }
    segmentInfoMap.add("version", segmentCommitInfo.info.getVersion().toString());
    if (withFieldInfos) {
      SegmentReader seg = null;
      for (LeafReaderContext lrc : leafContexts) {
        LeafReader leafReader = lrc.reader();
        // unwrap
        while (leafReader instanceof FilterLeafReader) {
          leafReader = ((FilterLeafReader)leafReader).getDelegate();
        }
        if (leafReader instanceof SegmentReader) {
          SegmentReader sr = (SegmentReader)leafReader;
          if (sr.getSegmentInfo().info.equals(segmentCommitInfo.info)) {
            seg = sr;
            break;
          }
        }
      }
      if (seg == null) {
        log.debug("Skipping segment info - not available as a SegmentReader: " + segmentCommitInfo);
      } else {
        FieldInfos fis = seg.getFieldInfos();
        SimpleOrderedMap<Object> fields = new SimpleOrderedMap<>();
        for (FieldInfo fi : fis) {
          fields.add(fi.name, getFieldFlags(fi, schema));
        }
        segmentInfoMap.add("fields", fields);
      }
    }

    return segmentInfoMap;
  }

  private SimpleOrderedMap<Object> getFieldFlags(FieldInfo fi, IndexSchema schema) {

    SimpleOrderedMap<Object> fieldFlags = new SimpleOrderedMap<>();
    StringBuilder flags = new StringBuilder();

    IndexOptions opts = fi.getIndexOptions();
    flags.append( (opts != IndexOptions.NONE) ? FieldFlag.INDEXED.getAbbreviation() : '-' );
    DocValuesType dvt = fi.getDocValuesType();
    if (dvt != DocValuesType.NONE) {
      flags.append(FieldFlag.DOC_VALUES.getAbbreviation());
      switch (dvt) {
        case NUMERIC:
          flags.append("num");
          break;
        case BINARY:
          flags.append("bin");
          break;
        case SORTED:
          flags.append("srt");
          break;
        case SORTED_NUMERIC:
          flags.append("srn");
          break;
        case SORTED_SET:
          flags.append("srs");
          break;
      }
    } else {
      flags.append("----");
    }
    flags.append( (fi.hasVectors()) ? FieldFlag.TERM_VECTOR_STORED.getAbbreviation() : '-' );
    flags.append( (fi.omitsNorms()) ? FieldFlag.OMIT_NORMS.getAbbreviation() : '-' );

    flags.append( (DOCS == opts ) ?
        FieldFlag.OMIT_TF.getAbbreviation() : '-' );

    flags.append((DOCS_AND_FREQS == opts) ?
        FieldFlag.OMIT_POSITIONS.getAbbreviation() : '-');

    flags.append((DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS == opts) ?
        FieldFlag.STORE_OFFSETS_WITH_POSITIONS.getAbbreviation() : '-');

    fieldFlags.add("flags", flags.toString());

    // probably too much detail?
//    Map<String, String> attributes = fi.attributes();
//    if (!attributes.isEmpty()) {
//      fieldFlags.add("attributes", attributes);
//    }
    // check compliance with the current schema
    SchemaField sf = schema.getFieldOrNull(fi.name);

    if (sf != null) {
      SimpleOrderedMap<Object> nonCompliant = new SimpleOrderedMap<>();
      if (sf.hasDocValues() &&
          fi.getDocValuesType() == DocValuesType.NONE &&
          fi.getIndexOptions() != IndexOptions.NONE) {
        nonCompliant.add("docValues", "schema=" + sf.getType().getUninversionType(sf) + ", segment=false");
      }
      if (!sf.hasDocValues() &&
          fi.getDocValuesType() != DocValuesType.NONE &&
          fi.getIndexOptions() != IndexOptions.NONE) {
        nonCompliant.add("docValues", "schema=false, segment=" + fi.getDocValuesType().toString());
      }
      if (sf.indexed() != (fi.getIndexOptions() != IndexOptions.NONE)) {
        nonCompliant.add("indexed", "schema=" + sf.indexed() + ", segment=" + fi.getIndexOptions());
      }
      if (sf.omitNorms() != fi.omitsNorms()) {
        nonCompliant.add("omitNorms", "schema=" + sf.omitNorms() + ", segment=" + fi.omitsNorms());
      }
      if (sf.storeTermVector() != fi.hasVectors()) {
        nonCompliant.add("termVectors", "schema=" + sf.storeTermVector() + ", segment=" + fi.hasVectors());
      }
      if (sf.storeOffsetsWithPositions() != (fi.getIndexOptions() == IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS)) {
        nonCompliant.add("storeOffsetsWithPositions", "schema=" + sf.storeOffsetsWithPositions() + ", segment=" + fi.getIndexOptions());
      }

      if (nonCompliant.size() > 0) {
        fieldFlags.add("nonCompliant", nonCompliant);
      }
    }
    return fieldFlags;
  }

  private List<String> getMergeCandidatesNames(SolrQueryRequest req, SegmentInfos infos) throws IOException {
    List<String> result = new ArrayList<String>();
    RefCounted<IndexWriter> refCounted = req.getCore().getSolrCoreState().getIndexWriter(req.getCore());
    try {
      IndexWriter indexWriter = refCounted.get();
      //get chosen merge policy
      MergePolicy mp = indexWriter.getConfig().getMergePolicy();
      //Find merges
      MergeSpecification findMerges = mp.findMerges(MergeTrigger.EXPLICIT, infos, indexWriter);
      if (findMerges != null && findMerges.merges != null && findMerges.merges.size() > 0) {
        for (OneMerge merge : findMerges.merges) {
          //TODO: add merge grouping
          for (SegmentCommitInfo mergeSegmentInfo : merge.segments) {
            result.add(mergeSegmentInfo.info.name);
          }
        }
      }

      return result;
    } finally {
      refCounted.decref();
    }
  }

  @Override
  public String getDescription() {
    return "Lucene segments info.";
  }
}
