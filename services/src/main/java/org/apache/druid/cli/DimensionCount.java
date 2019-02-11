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
import org.mozilla.javascript.tools.debugger.Dim;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Command(
    name = "summarize-segment-dimensions"
)
public class DimensionCount extends GuiceRunnable {

//  private static FileOutputStream fileOutputStream;
  private static PrintWriter printWriter;

  public DimensionCount()
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

  @Override
  public void run()
  {
    final Injector injector = makeInjector();
    final IndexIO indexIO = injector.getInstance(IndexIO.class);
    try (final QueryableIndex index = indexIO.loadIndex(new File(directory))) {
      List<String> metricNames = getDimNames(index);
//      fileOutputStream = new FileOutputStream(outputFileName);
      printWriter = new PrintWriter(new File(outputFileName));
      log.info("Dims " + metricNames);
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
    final List<String> columnNames = new ArrayList<>(dimNames);

    final Sequence<Cursor> cursors = adapter.makeCursors(
        Filters.toFilter(null),
        index.getDataInterval().withChronology(ISOChronology.getInstanceUTC()),
        VirtualColumns.EMPTY,
        Granularities.ALL,
        false,
        null
    );
    class MetricStat {

    }
      class Summary
      {

        private final List<String> columnNames;
        public int count;
        private LinkedHashMap<String, Dim> dims;

        public Summary(List<String> columnNames)
        {
          this.columnNames = columnNames;
          dims = new LinkedHashMap<>();
          for (String columnName : columnNames) {
            dims.put(columnName, new Dim());
          }
        }

        private int previous = count;
        private boolean totalOverflow;
        public void add(String dimName, Object v) throws OverflowException
        {
            dims.get(dimName).add(v);
            if (previous -count > 100000) {
              int sum = 0;
              for (Dim dim : dims.values()) {
                sum += dim.count();
              }
              if (sum >= 100000) {
                totalOverflow  = true;
                throw new OverflowException();
              }
              previous = count;
            }
        }

        public void reportDims()
        {
          for (String columnName : columnNames) {
            output(columnName);
            Dim dim = dims.get(columnName);
            dim.report(count);
          }
        }

        class Dim {
          private HashMap<Object, Integer> valueCounts;

          {
            valueCounts = new HashMap<>();
          }
          boolean overflow = false;
          int dimRowCount;
          public void add(Object v)
          {
            if (!valueCounts.containsKey(v)&& valueCounts.size() > 10000) {
              overflow = true;
              return;
            }

            dimRowCount++;
            Integer count = valueCounts.computeIfAbsent(v, k -> 0);
            valueCounts.put(v, count + 1);
          }

          public int count()
          {
            return valueCounts.size();
          }

          public void report(int count)
          {
            int dropped = 0;
            if (overflow)
              output("  overflow");
            for (Map.Entry<Object, Integer> e : valueCounts.entrySet()) {
              double ratio = ((double)e.getValue())/count;
              if (ratio > 0.0099) {
                output(String.format("    %s %.3f", e.getKey(), ratio));
              } else {
                dropped += e.getValue();
              }
            }
            output(String.format("  dropped %.3f",  ((double)dropped) / count));
          }
        }
      }
    Summary summary = new Summary(columnNames);
    Sequence<Object> map = Sequences.map(
        cursors,
        cursor -> {
          ColumnSelectorFactory columnSelectorFactory = cursor.getColumnSelectorFactory();
          final List<BaseObjectColumnValueSelector> selectors = columnNames
              .stream()
              .map(columnSelectorFactory::makeColumnValueSelector)
              .collect(Collectors.toList());
          ColumnValueSelector timeValueSelector = columnSelectorFactory.makeColumnValueSelector(ColumnHolder.TIME_COLUMN_NAME);

          // for each dimesion value create Dimension -> value -> count

          LinkedHashSet<Long> timestamps = new LinkedHashSet<Long>();
          try {
            while (!cursor.isDone()) {
              summary.count++;

              timestamps.add(timeValueSelector.getLong());
              for (int i = 0; i < columnNames.size(); i++) {
                BaseObjectColumnValueSelector baseObjectColumnValueSelector = selectors.get(i);
                Object object = baseObjectColumnValueSelector.getObject();
                if (object instanceof HyperLogLogCollector) {
                  summary.add(columnNames.get(i), ((HyperLogLogCollector) object).estimateCardinality());
                } else {
                  summary.add(columnNames.get(i), object);
                }
              }
              cursor.advance();
            }
          } catch (OverflowException e) {
            output("total overflow");
          }
          output("Timestamps");
          for (Long timestamp : timestamps) {
            output("  " + new DateTime(timestamp, DateTimeZone.UTC));
          }
          return null;
        }
    );
    map.accumulate(null, (accumulated, in) -> null);


    output(String.format("Rows %s", summary.count));
    summary.reportDims();
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
