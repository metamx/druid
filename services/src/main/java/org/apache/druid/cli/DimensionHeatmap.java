/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.cli;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.name.Names;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.druid.guice.DruidProcessingModule;
import org.apache.druid.guice.QueryRunnerFactoryModule;
import org.apache.druid.guice.QueryableModule;
import org.apache.druid.hll.HyperLogLogCollector;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.filter.Filters;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.chrono.ISOChronology;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

@Command(
    name = "dimension-heatmap"
)
public class DimensionHeatmap extends GuiceRunnable {

//  private static FileOutputStream fileOutputStream;
  private static PrintWriter printWriter;

  public DimensionHeatmap()
  {
    super(log);
  }

  private static final Logger log = new Logger(DumpSegment.class);

  @Option(
      name = {"-d", "--directory"},
      title = "directory",
      description = "Directory containing segment data.",
      required = true)
  public String directory;

  @Option(
      name = {"-o", "--out"},
      title = "file",
      description = "File to write to, or omit to write to stdout.",
      required = false)
  public String outputFileName;

  @Option(
      name = {"-m"},
      title = "count metric",
      description = "Count metric",
      required = true)
  public String metric;

  @Option(
      name = {"--dim"},
      title = "count dimension",
      description = "Count dimension",
      required = true)
  public String dimension;

  @Override
  public void run()
  {
    final Injector injector = makeInjector();
    final IndexIO indexIO = injector.getInstance(IndexIO.class);
    try (final QueryableIndex index = indexIO.loadIndex(new File(directory))) {
      List<String> metricNames = getDimNames(index);
//      fileOutputStream = new FileOutputStream(outputFileName);
      printWriter = new PrintWriter(new File(outputFileName));
      output("Dims " + metricNames);
      long l = System.currentTimeMillis();
      summarize(injector, index, metricNames);
      printWriter.println("time " + ((System.currentTimeMillis() - l) / 1000.0/ 60));
      printWriter.close();
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
 class OverflowException extends Exception {

 }
  private void summarize(Injector injector, QueryableIndex index, List<String> dimNames)
  {
    final QueryableIndexStorageAdapter adapter = new QueryableIndexStorageAdapter(index);
    final List<String> columnNames = Collections.singletonList(dimension);

    final Sequence<Cursor> cursors = adapter.makeCursors(
        Filters.toFilter(null),
        index.getDataInterval().withChronology(ISOChronology.getInstanceUTC()),
        VirtualColumns.EMPTY,
        Granularities.ALL,
        false,
        null
    );
      class Summary
      {
        int totalCount;

      }
    Summary summary = new Summary();
    Sequence<Object> map = Sequences.map(
        cursors,
        cursor -> {
          ColumnSelectorFactory columnSelectorFactory = cursor.getColumnSelectorFactory();
          final List<BaseObjectColumnValueSelector> selectors = columnNames
              .stream()
              .map(columnSelectorFactory::makeColumnValueSelector)
              .collect(Collectors.toList());
          ColumnValueSelector metricValueSelector = columnSelectorFactory.makeColumnValueSelector(metric);
          ColumnValueSelector timeValueSelector = columnSelectorFactory.makeColumnValueSelector(ColumnHolder.TIME_COLUMN_NAME);

          // for each dimesion value create Dimension -> value -> count

          LinkedHashSet<Long> timestamps = new LinkedHashSet<>();
          class Dim {
            private final String name;
            double sum;
            int count;

            public Dim(String key)
            {
              name = key;
            }
          }
            TreeMap<String, Dim> dimSum = new TreeMap<>();
            while (!cursor.isDone()) {
              summary.totalCount++;

              timestamps.add(timeValueSelector.getLong());
              for (int i = 0; i < columnNames.size(); i++) {
                BaseObjectColumnValueSelector baseObjectColumnValueSelector = selectors.get(i);
                Object dimValue = baseObjectColumnValueSelector.getObject();
                String key = (String)dimValue;
                Dim dim = dimSum.computeIfAbsent(key, k -> new Dim(key));
                Object metricValue =  metricValueSelector.getObject();
                if (metricValue instanceof HyperLogLogCollector) {
                  dim.sum += ((HyperLogLogCollector) metricValue).estimateCardinality();
                  dim.count++;
                } else if (metricValue instanceof Number) {
                  dim.sum += ((Number)metricValue).doubleValue();
                  dim.count++;
                } else {
                  throw new IllegalStateException();
                }
              }
              cursor.advance();
            }
          output("Timestamps");
          for (Long timestamp : timestamps) {
            output("  " + new DateTime(timestamp, DateTimeZone.UTC));
          }
          TreeMap<Integer, List<Dim>> byCount = new TreeMap<>();
          for (Map.Entry<String, Dim> e : dimSum.entrySet()) {
            List<Dim> dims = byCount.computeIfAbsent(e.getValue().count, k -> new ArrayList<>());
            dims.add(e.getValue());
          }
          for (Map.Entry<Integer, List<Dim>> e : byCount.entrySet()) {
            List<Dim> value = e.getValue();
//            for (Dim dim : value) {
//              output(String.format("%s %.3f %.3f", dim.name, dim.sum, ((double)dim.count) / summary.totalCount));
//            }
          }
          TreeMap<Double, List<Dim>> bySum = new TreeMap<>();
          for (Map.Entry<String, Dim> e : dimSum.entrySet()) {
            List<Dim> dims = bySum.computeIfAbsent(e.getValue().sum, k -> new ArrayList<>());
            dims.add(e.getValue());
          }
          for (Map.Entry<Double, List<Dim>> e : bySum.entrySet()) {
            List<Dim> value = e.getValue();
            for (Dim dim : value) {
              output(String.format("%s %.3f %.3f", dim.name, dim.sum, ((double)dim.count) / summary.totalCount));
            }
          }

          return null;
        }
    );
    map.accumulate(null, (accumulated, in) -> null);


    output(String.format("Rows %s", summary.totalCount));
  }

  private void output(String s)
  {
    printWriter.println(s);
  }

  private List<String> getDimNames(QueryableIndex index)
  {
    return new ArrayList<>(Sets.newLinkedHashSet(index.getAvailableDimensions()));
  }

  @Override
  protected List<? extends Module> getModules()
  {
    return ImmutableList.of(
        new DruidProcessingModule(),
        new QueryableModule(),
        new QueryRunnerFactoryModule(),
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            binder.bindConstant().annotatedWith(Names.named("serviceName")).to("druid/tool");
            binder.bindConstant().annotatedWith(Names.named("servicePort")).to(9999);
            binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(-1);
            binder.bind(DruidProcessingConfig.class).toInstance(
                new DruidProcessingConfig()
                {
                  @Override
                  public String getFormatString()
                  {
                    return "processing-%s";
                  }

                  @Override
                  public int intermediateComputeSizeBytes()
                  {
                    return 100 * 1024 * 1024;
                  }

                  @Override
                  public int getNumThreads()
                  {
                    return 1;
                  }

                  @Override
                  public int columnCacheSizeBytes()
                  {
                    return 25 * 1024 * 1024;
                  }
                }
            );
            binder.bind(ColumnConfig.class).to(DruidProcessingConfig.class);
          }
        }
    );
  }
}
