/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;
import io.druid.common.utils.JodaUtils;
import io.druid.guice.GuiceInjectors;
import io.druid.guice.annotations.Json;
import io.druid.initialization.Initialization;
import io.druid.java.util.common.RE;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.NotDimFilter;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.segment.Cursor;
import io.druid.segment.IndexIO;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexStorageAdapter;
import io.druid.segment.VirtualColumns;
import io.druid.segment.column.Column;
import io.druid.segment.column.GenericColumn;
import io.druid.segment.column.IndexedLongsGenericColumn;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public class OrderAnalysisTest
{
  private static final Logger LOG = new Logger(OrderAnalysisTest.class);
  private static final List<Path> PATHS = Arrays.asList(
      Paths.get("/Users/charlesallen/bin/wrk/hash/usage/data"),
      Paths.get("/Users/charlesallen/bin/wrk/hash/lex/data")/*,
      Paths.get("/Users/charlesallen/bin/wrk/hash/lex/data"),
      Paths.get("/Users/charlesallen/bin/wrk/hash/cardinality/data")
      */
  );
  private static final Path QUERY_JSONS = Paths.get(
      "/Users/charlesallen/bin/wrk/hash/requests/data_reduced/only_filter_latency.json"
  );
  private static final Path OUTPUT_JSONS = Paths.get(
      "/Users/charlesallen/bin/wrk/hash/requests/bytype_results.json"
  );
  private static final Injector INJECTOR = Initialization.makeInjectorWithModules(
      GuiceInjectors.makeStartupInjector(),
      ImmutableList.of(binder -> {
        binder.bind(Key.get(String.class, Names.named("serviceName"))).toInstance("test-service");
        binder.bind(Key.get(Integer.class, Names.named("servicePort"))).toInstance(0);
        binder.bind(Key.get(Integer.class, Names.named("tlsServicePort"))).toInstance(0);
      })
  );

  @Test
  public void testOrderSkips() throws Exception
  {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final PrintStream printStream = new PrintStream(baos, true);
    final IndexIO indexIO = INJECTOR.getInstance(IndexIO.class);
    final ObjectMapper mapper = INJECTOR.getInstance(Key.get(ObjectMapper.class, Json.class));
    final Map<File, QueryableIndex> indexCache = new HashMap<>();
    final Map<File, QueryableIndexStorageAdapter> storageAdapterCache = new HashMap<>();
    final AtomicLong lineCounter = new AtomicLong(0L);
    final TypeReference<Map<String, Object>> typeReference = new TypeReference<Map<String, Object>>()
    {
    };
    PATHS
        .stream()
        .map(Path::toFile)
        .flatMap(file -> {
          try {
            return Files.lines(QUERY_JSONS).map(s -> {
              final long lineNo = lineCounter.incrementAndGet();
              final Map<String, Object> map;
              final ScanData scanData = new ScanData();
              scanData.file = file;
              try {
                map = mapper.readValue(s, typeReference);
                if (map.get("filter") == null) {
                  scanData.queryData = new QueryData(
                      new NotDimFilter(
                          new SelectorDimFilter("foo", "foo", null)
                      ),
                      (Integer) map.get("latency")
                  );
                } else {
                  scanData.queryData = mapper.convertValue(map, QueryData.class);
                }
              }
              catch (IllegalArgumentException | IOException e) {
                throw new RE(e, "error parsing [%s] at %d", s, lineNo);
              }
              return scanData;
            });
          }
          catch (IOException ioe) {
            throw new RE("oops");
          }
        })
        .filter(Objects::nonNull)
        .map((sd) -> {
          final File file = sd.file;
          final QueryableIndex queryableIndex = indexCache.computeIfAbsent(file, (f) -> {
            try {
              return indexIO.loadIndex(f);
            }
            catch (IOException e) {
              throw new RE(e, "oops!");
            }
          });
          final QueryableIndexStorageAdapter storageAdapter = storageAdapterCache
              .computeIfAbsent(file, (f) -> new QueryableIndexStorageAdapter(indexCache.get(f)));
          final String type = file.getParentFile().getName();
          final Sequence<Cursor> cursorSequence = storageAdapter.makeCursors(
              sd.queryData.getFilter().toFilter(),
              JodaUtils.ETERNITY,
              VirtualColumns.EMPTY,
              Granularities.ALL,
              false,
              null
          );
          return Sequences.toList(Sequences.map(cursorSequence, (c) -> {
            // Metric doesn't matter, only qty of blocks matters
            final LongColumnSelector selector = c.makeLongColumnSelector("accepted_cnt");
            long sum = 0L;
            while (!c.isDone()) {
              sum += selector.get();
              c.advance();
            }
            final Column column = queryableIndex.getColumn("accepted_cnt");
            Field field;
            Object obj;
            final AtomicLong counter;
            try {
              field = IndexedLongsGenericColumn.class.getDeclaredField("column");
              field.setAccessible(true);
              final GenericColumn genericColumn = column.getGenericColumn();
              final Object innerCol = field.get(genericColumn);
              field = innerCol.getClass().getDeclaredField("this$0");
              field.setAccessible(true);
              obj = field.get(innerCol);
              field = obj.getClass().getDeclaredField("blockLoads");
              field.setAccessible(true);
              counter = (AtomicLong) field.get(obj);
            }
            catch (Exception e) {
              throw new RE(e, "error");
            }
            final long blocks = counter.get();
            counter.set(0);
            if (blocks == 0) {
              return null;
            }
            return StringUtils.safeFormat("%s\t%d", type, blocks);
          }), Lists.newArrayList()).get(0); // Only head
        })
        .filter(Objects::nonNull)
        .forEach(printStream::println);
    try (FileOutputStream fileOutputStream = new FileOutputStream(OUTPUT_JSONS.toFile())) {
      baos.writeTo(fileOutputStream);
    }
  }
}

class QueryData
{
  private final DimFilter filter;
  private final long latency;

  @JsonCreator
  public QueryData(@JsonProperty("filter") DimFilter filter, @JsonProperty("latency") long latency)
  {
    this.filter = filter;
    this.latency = latency;
  }

  public DimFilter getFilter()
  {
    return filter;
  }

  public long getLatency()
  {
    return latency;
  }
}

class ScanData
{
  QueryData queryData;
  File file;
}
