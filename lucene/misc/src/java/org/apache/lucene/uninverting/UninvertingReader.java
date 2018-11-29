package org.apache.lucene.uninverting;

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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;

import org.apache.lucene.document.BinaryDocValuesField; // javadocs
import org.apache.lucene.document.DoubleField; // javadocs
import org.apache.lucene.document.FloatField; // javadocs
import org.apache.lucene.document.IntField; // javadocs
import org.apache.lucene.document.LongField; // javadocs
import org.apache.lucene.document.NumericDocValuesField; // javadocs
import org.apache.lucene.document.SortedDocValuesField; // javadocs
import org.apache.lucene.document.SortedSetDocValuesField; // javadocs
import org.apache.lucene.document.StringField; // javadocs
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.uninverting.FieldCache.CacheEntry;
import org.apache.lucene.util.Bits;

/**
 * A FilterReader that exposes <i>indexed</i> values as if they also had
 * docvalues.
 * <p>
 * This is accomplished by "inverting the inverted index" or "uninversion".
 * <p>
 * The uninversion process happens lazily: upon the first request for the 
 * field's docvalues (e.g. via {@link org.apache.lucene.index.LeafReader#getNumericDocValues(String)} 
 * or similar), it will create the docvalues on-the-fly if needed and cache it,
 * based on the core cache key of the wrapped LeafReader.
 *
 * <p><b>NOTE: this version differs from stock Solr implementation by preferring the
 * field cache when a mapping exists, even if doc values already exist for that field.</b></p>
 */
public class UninvertingReader extends FilterLeafReader {
  
  /**
   * Specifies the type of uninversion to apply for the field. 
   */
  public static enum Type {
    /** 
     * Single-valued Integer, (e.g. indexed with {@link IntField})
     * <p>
     * Fields with this type act as if they were indexed with
     * {@link NumericDocValuesField}.
     */
    INTEGER,
    /** 
     * Single-valued Long, (e.g. indexed with {@link LongField}) 
     * <p>
     * Fields with this type act as if they were indexed with
     * {@link NumericDocValuesField}.
     */
    LONG,
    /** 
     * Single-valued Float, (e.g. indexed with {@link FloatField}) 
     * <p>
     * Fields with this type act as if they were indexed with
     * {@link NumericDocValuesField}.
     */
    FLOAT,
    /** 
     * Single-valued Double, (e.g. indexed with {@link DoubleField}) 
     * <p>
     * Fields with this type act as if they were indexed with
     * {@link NumericDocValuesField}.
     */
    DOUBLE,
    /** 
     * Single-valued Binary, (e.g. indexed with {@link StringField}) 
     * <p>
     * Fields with this type act as if they were indexed with
     * {@link BinaryDocValuesField}.
     */
    BINARY,
    /** 
     * Single-valued Binary, (e.g. indexed with {@link StringField}) 
     * <p>
     * Fields with this type act as if they were indexed with
     * {@link SortedDocValuesField}.
     */
    SORTED,
    /** 
     * Multi-valued Binary, (e.g. indexed with {@link StringField}) 
     * <p>
     * Fields with this type act as if they were indexed with
     * {@link SortedSetDocValuesField}.
     */
    SORTED_SET_BINARY,
    /** 
     * Multi-valued Integer, (e.g. indexed with {@link IntField}) 
     * <p>
     * Fields with this type act as if they were indexed with
     * {@link SortedSetDocValuesField}.
     */
    SORTED_SET_INTEGER,
    /** 
     * Multi-valued Float, (e.g. indexed with {@link FloatField}) 
     * <p>
     * Fields with this type act as if they were indexed with
     * {@link SortedSetDocValuesField}.
     */
    SORTED_SET_FLOAT,
    /** 
     * Multi-valued Long, (e.g. indexed with {@link LongField}) 
     * <p>
     * Fields with this type act as if they were indexed with
     * {@link SortedSetDocValuesField}.
     */
    SORTED_SET_LONG,
    /** 
     * Multi-valued Double, (e.g. indexed with {@link DoubleField}) 
     * <p>
     * Fields with this type act as if they were indexed with
     * {@link SortedSetDocValuesField}.
     */
    SORTED_SET_DOUBLE
  }
  
  /**
   * Wraps a provided DirectoryReader. Note that for convenience, the returned reader
   * can be used normally (e.g. passed to {@link DirectoryReader#openIfChanged(DirectoryReader)})
   * and so on. 
   */
  public static DirectoryReader wrap(DirectoryReader in, Function<String, Type> mapper) throws IOException {
    return new UninvertingDirectoryReader(in, mapper);
  }

  static class UninvertingDirectoryReader extends FilterDirectoryReader {
    final Function<String, Type> mapper;

    public UninvertingDirectoryReader(DirectoryReader in, final Function<String, Type> mapper) throws IOException {
      super(in, new FilterDirectoryReader.SubReaderWrapper() {
        @Override
        public LeafReader wrap(LeafReader reader) {
          return UninvertingReader.wrap(reader, mapper);
        }
      });
      this.mapper = mapper;
    }

    @Override
    protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
      return new UninvertingDirectoryReader(in, mapper);
    }
  }

  final Function<String, Type> mapping;
  final FieldInfos fieldInfos;
  
  public UninvertingReader(LeafReader in, Function<String, Type> mapping, FieldInfos fieldInfos) {
    super(in);
    this.mapping = mapping;
    this.fieldInfos = fieldInfos;
  }


  public static LeafReader wrap(LeafReader in, Function<String, Type> mapping) {
    boolean wrap = false;

    ArrayList<FieldInfo> filteredInfos = new ArrayList<>(in.getFieldInfos().size());
    for (FieldInfo fi : in.getFieldInfos()) {
      // unlike the version in Solr 7x we check the type even if docValues already exist
      DocValuesType type = null;
      if (fi.getIndexOptions() != IndexOptions.NONE) {
        Type t = mapping.apply(fi.name);
        if (t != null) {
          switch(t) {
            case INTEGER:
            case LONG:
            case FLOAT:
            case DOUBLE:
              type = DocValuesType.NUMERIC;
              break;
            case BINARY:
              type = DocValuesType.BINARY;
              break;
            case SORTED:
              type = DocValuesType.SORTED;
              break;
            case SORTED_SET_BINARY:
            case SORTED_SET_INTEGER:
            case SORTED_SET_FLOAT:
            case SORTED_SET_LONG:
            case SORTED_SET_DOUBLE:
              type = DocValuesType.SORTED_SET;
              break;
            default:
              throw new AssertionError();
          }
        }
      }
      if (type != null) { // we changed it
        wrap = true;
        filteredInfos.add(new FieldInfo(fi.name, fi.number, fi.hasVectors(), fi.omitsNorms(),
            fi.hasPayloads(), fi.getIndexOptions(), type, -1, fi.attributes()));
      } else {
        filteredInfos.add(fi);
      }
    }
    if (!wrap) {
      return in;
    } else {
      FieldInfos fieldInfos = new FieldInfos(filteredInfos.toArray(new FieldInfo[filteredInfos.size()]));
      return new UninvertingReader(in, mapping, fieldInfos);
    }
  }

  @Override
  public FieldInfos getFieldInfos() {
    return fieldInfos;
  }

  // unlike the version in Solr 7x we always first return uninverted values if available in mapping
  // also, if the mapping if Type.NONE it drops existing doc values
  @Override
  public NumericDocValues getNumericDocValues(String field) throws IOException {
    Type v = getType(field);
    if (v != null) {
      switch (v) {
        case INTEGER: return FieldCache.DEFAULT.getNumerics(in, field, FieldCache.NUMERIC_UTILS_INT_PARSER, true);
        case FLOAT: return FieldCache.DEFAULT.getNumerics(in, field, FieldCache.NUMERIC_UTILS_FLOAT_PARSER, true);
        case LONG: return FieldCache.DEFAULT.getNumerics(in, field, FieldCache.NUMERIC_UTILS_LONG_PARSER, true);
        case DOUBLE: return FieldCache.DEFAULT.getNumerics(in, field, FieldCache.NUMERIC_UTILS_DOUBLE_PARSER, true);
      }
      return null;
    } else {
      return in.getNumericDocValues(field);
    }
  }

  @Override
  public BinaryDocValues getBinaryDocValues(String field) throws IOException {
    Type v = getType(field);
    if (v != null) {
      if (v == Type.BINARY) {
        return FieldCache.DEFAULT.getTerms(in, field, true);
      } else {
        return null;
      }
    } else {
      return in.getBinaryDocValues(field);
    }
  }

  @Override
  public SortedDocValues getSortedDocValues(String field) throws IOException {
    Type v = getType(field);
    if (v != null) {
      if (v == Type.SORTED) {
        return FieldCache.DEFAULT.getTermsIndex(in, field);
      } else {
        return null;
      }
    } else {
      return in.getSortedDocValues(field);
    }
  }
  
  @Override
  public SortedSetDocValues getSortedSetDocValues(String field) throws IOException {
    Type v = getType(field);
    if (v != null) {
      switch (v) {
        case SORTED_SET_INTEGER:
        case SORTED_SET_FLOAT: 
          return FieldCache.DEFAULT.getDocTermOrds(in, field, FieldCache.INT32_TERM_PREFIX);
        case SORTED_SET_LONG:
        case SORTED_SET_DOUBLE:
          return FieldCache.DEFAULT.getDocTermOrds(in, field, FieldCache.INT64_TERM_PREFIX);
        case SORTED_SET_BINARY:
          return FieldCache.DEFAULT.getDocTermOrds(in, field, null);
      }
      return null;
    } else {
      return in.getSortedSetDocValues(field);
    }
  }

  @Override
  public Bits getDocsWithField(String field) throws IOException {
    if (getType(field) != null) {
      return FieldCache.DEFAULT.getDocsWithField(in, field);
    } else {
      return in.getDocsWithField(field);
    }
  }
  
  /** 
   * Returns the field's uninversion type, or null 
   * if the field doesn't exist or doesn't have a mapping.
   */
  /**
   * Returns the field's uninversion type, or null
   * if the field doesn't exist or doesn't have a mapping.
   */
  private Type getType(String field) {
    return mapping.apply(field);
  }

  @Override
  public Object getCoreCacheKey() {
    return in.getCoreCacheKey();
  }

  @Override
  public Object getCombinedCoreAndDeletesKey() {
    return in.getCombinedCoreAndDeletesKey();
  }

  @Override
  public String toString() {
    return "Uninverting(" + in.toString() + ")";
  }
  
  /** 
   * Return information about the backing cache
   * @lucene.internal 
   */
  public static String[] getUninvertedStats() {
    CacheEntry[] entries = FieldCache.DEFAULT.getCacheEntries();
    String[] info = new String[entries.length];
    for (int i = 0; i < entries.length; i++) {
      info[i] = entries[i].toString();
    }
    return info;
  }
}
