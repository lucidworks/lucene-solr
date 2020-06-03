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

package org.apache.solr.search;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.LongBitSet;
import org.apache.lucene.util.LongValues;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostFilterLocalJoinQuery extends JoinQuery implements PostFilter {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private boolean cache;
  private int cost;
  private boolean cacheSep;

  private String joinField;

  public PostFilterLocalJoinQuery(String joinField, String coreName, Query subQuery) {
    super(joinField, joinField, coreName, subQuery);
    this.joinField = joinField;
  }

  @Override
  public DelegatingCollector getFilterCollector(IndexSearcher searcher) {
    log.debug("Running local join query using postfilter");
    final SolrIndexSearcher solrSearcher = (SolrIndexSearcher) searcher;
    try {

      final SortedDocValues fromValues = DocValues.getSorted(solrSearcher.getSlowAtomicReader(), joinField);
      final SortedDocValues toValues = DocValues.getSorted(solrSearcher.getSlowAtomicReader(), joinField);

      final LongBitSet ordBitSet = new LongBitSet(fromValues.getValueCount());

      final TermOrdinalCollector collector = new TermOrdinalCollector(fromValues, ordBitSet);
      solrSearcher.search(q, collector);

      if (true) {
        return new LocalJoinQueryCollector(toValues, ordBitSet);
      } else {
        return new NoMatchesCollector();
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean getCache() {
    return false;
  }

  public int hashCode() {
    return 1;
  }

  @Override
  public void setCache(boolean cache) {
    this.cache = cache;
  }

  @Override
  public int getCost() {
    return 101;
  }

  @Override
  public void setCost(int cost) {
    this.cost = cost;
  }

  @Override
  public boolean getCacheSep() {
    return cacheSep;
  }

  @Override
  public void setCacheSep(boolean cacheSep) {
    this.cacheSep = cacheSep;
  }


  public static QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    return new QParser(qstr, localParams, params, req) {
      @Override
      public Query parse() throws SyntaxError {
        final String fromField = localParams.get("from");
        final String v = localParams.get(CommonParams.VALUE);

        LocalSolrQueryRequest otherReq = new LocalSolrQueryRequest(req.getCore(), params);
        QParser fromQueryParser = QParser.getParser(v, otherReq);
        Query fromQuery = fromQueryParser.getQuery();
        return new PostFilterLocalJoinQuery(fromField, null, fromQuery);

      }
    };
  }



  private static class TermOrdinalCollector extends DelegatingCollector {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final LongBitSet topLevelDocValuesBitSet;
    private OrdinalMap ordinalMap;
    private SortedDocValues[] sortedDocValues;
    private SortedDocValues leafDocValues;
    private LongValues longValues;


    public TermOrdinalCollector(SortedDocValues topLevelDocValues, LongBitSet topLevelDocValuesBitSet) {
      this.topLevelDocValuesBitSet = topLevelDocValuesBitSet;
      this.ordinalMap = ((MultiDocValues.MultiSortedDocValues)topLevelDocValues).mapping;
      this.sortedDocValues = ((MultiDocValues.MultiSortedDocValues)topLevelDocValues).values;
    }


    public ScoreMode scoreMode() {
      return ScoreMode.COMPLETE_NO_SCORES;
    }

    public boolean needsScores(){
      return false;
    }

    @Override
    public void doSetNextReader(LeafReaderContext context) throws IOException {
      this.docBase = context.docBase;
      this.leafDocValues = sortedDocValues[context.ord];
      this.longValues = ordinalMap.getGlobalOrds(context.ord);
    }

    @Override
    public void collect(int doc) throws IOException {
      if (leafDocValues.advanceExact(doc)) { // TODO The use of advanceExact assumes collect() is called in increasing docId order.  Is that true?
        topLevelDocValuesBitSet.set(longValues.get(leafDocValues.ordValue()));
      }
    }
  }

  private static class LocalJoinQueryCollector extends DelegatingCollector {
    private LeafCollector leafCollector;
    private LongBitSet topLevelDocValuesBitSet;
    private OrdinalMap ordinalMap;
    private SortedDocValues[] sortedDocValues;
    private SortedDocValues leafDocValues;
    private LongValues longValues;

    public LocalJoinQueryCollector(SortedDocValues topLevelDocValues, LongBitSet topLevelDocValuesBitSet) {
      this.topLevelDocValuesBitSet = topLevelDocValuesBitSet;
      this.ordinalMap = ((MultiDocValues.MultiSortedDocValues)topLevelDocValues).mapping;
      this.sortedDocValues = ((MultiDocValues.MultiSortedDocValues)topLevelDocValues).values;
    }

    @Override
    public void setScorer(Scorable scorer) throws IOException {
      leafCollector.setScorer(scorer);
    }

    @Override
    public void doSetNextReader(LeafReaderContext context) throws IOException {
      this.leafCollector = delegate.getLeafCollector(context);
      this.docBase = context.docBase;
      this.leafDocValues = sortedDocValues[context.ord];
      this.longValues = ordinalMap.getGlobalOrds(context.ord);
    }

    @Override
    public void collect(int doc) throws IOException {
      if (this.leafDocValues.advanceExact(doc)) {
        long ord = leafDocValues.ordValue();
        long globalOrd = longValues.get(ord);
        if (topLevelDocValuesBitSet.get(globalOrd)) {
            leafCollector.collect(doc);
        }
      }
    }
  }

  private static class NoMatchesCollector extends DelegatingCollector {
    @Override
    public void collect(int doc) throws IOException {}
  }
}
