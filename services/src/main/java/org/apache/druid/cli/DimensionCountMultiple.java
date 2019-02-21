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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Command(
    name = "summarize-multiple-segment-dimensions"
)
public class DimensionCountMultiple extends GuiceRunnable
{
  //  private static FileOutputStream fileOutputStream;
  private static PrintWriter printWriter;

  public DimensionCountMultiple()
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
      title = "count merged",
      description = "Count merged",
      required = false)
  public Boolean countTotalDropped = false;

  @Option(
      name = {"-p"},
      title = "percentage limit",
      required = false)
  public Double dimPercentageLimit = 0.0099;

  @Option(
      name = {"--metric-limit"},
      title = "metric percentage limit",
      required = false)
  public Double metricPercentageLimit = 0.0099;

  @Option(
      name = {"--metric"},
      title = "percentage limit",
      required = true)
  public String metric;

  List<String> dimNames = null;


  Summary summary = null;

  @Override
  public void run()
  {
    final Injector injector = makeInjector();
    final IndexIO indexIO = injector.getInstance(IndexIO.class);
    boolean first = true;
    try {
      printWriter = new PrintWriter(new File(outputFileName));
      long l = System.currentTimeMillis();
      ArrayList<Sequence<Cursor>> cursors = new ArrayList<>();
      for (String dir : directory.split(",")) {
        final QueryableIndex index = indexIO.loadIndex(new File(dir));
        if (first) {
          dimNames = getDimNames(index);
          summary = new Summary(dimNames);
          output("Dims " + dimNames);
          output(String.format("Metric limit %.8f", metricPercentageLimit));
          output(String.format("Dim limit %.8f", dimPercentageLimit));
          first = false;
        }
        final QueryableIndexStorageAdapter adapter = new QueryableIndexStorageAdapter(index);

        final Sequence<Cursor> cursors0 = adapter.makeCursors(
            Filters.toFilter(null),
            index.getDataInterval().withChronology(ISOChronology.getInstanceUTC()),
            VirtualColumns.EMPTY,
            Granularities.ALL,
            false,
            null
        );
        cursors.add(cursors0);
      }

      for (Sequence<Cursor> cursor : cursors) {
        Sequence<Object> map = Sequences.map(
            cursor,
            cursor0 -> {
              cursor0.reset();
              countDims2(cursor0, summary, dimNames);
              return null;
            }
        );

        map.accumulate(null, (accumulated, in) -> null);
      }
      BloomFilter<Integer> filter = BloomFilter.create(
          Funnels.integerFunnel(),
          1500000 * 3,
          0.001
      );
      for (Sequence<Cursor> cursor : cursors) {
        Sequence<Object> map = Sequences.map(
            cursor,
            cursor0 -> {
              cursor0.reset();
              filterOut(cursor0, summary, dimNames, filter);
              return null;
            }
        );

        map.accumulate(null, (accumulated, in) -> null);
      }
      output("Total rows " + summary.totalCount);
      output("Merged " + ((double) summary.merged) / summary.totalCount);
      output("Retained " + ((double) summary.retained) / summary.totalCount);
      output("Total string size " + summary.totalSize / 1024 / 1024 + " MiB");
      output(String.format(
          "Dropped string size %d MiB ratio %.3f",
          summary.totalDroppedSize / 1024 / 1024,
          ((double) summary.totalDroppedSize) / summary.totalSize
      ));
      printWriter.println("time " + ((System.currentTimeMillis() - l) / 1000.0 / 60));
    }
    catch (FileNotFoundException e) {
      e.printStackTrace();
    }
    catch (IOException e) {
      e.printStackTrace();
    }
    finally {
      printWriter.close();
    }

  }

  private void filterOut(Cursor cursor, Summary summary, List<String> columnNames, BloomFilter<Integer> filter)
  {
    ColumnSelectorFactory columnSelectorFactory = cursor.getColumnSelectorFactory();
    final List<BaseObjectColumnValueSelector> selectors = columnNames
        .stream()
        .map(columnSelectorFactory::makeColumnValueSelector)
        .collect(Collectors.toList());
    ColumnValueSelector metricValueSelector = columnSelectorFactory.makeColumnValueSelector(metric);

    Object[] values = new Object[columnNames.size()];
    int previousMerged = 0;
    output("Merged examples");
    while (!cursor.isDone()) {

      double metricValue = metricValueSelector.getDouble();
      boolean ret = false;
      for (int i = 0; i < columnNames.size(); i++) {
        BaseObjectColumnValueSelector baseObjectColumnValueSelector = selectors.get(i);
        values[i] = baseObjectColumnValueSelector.getObject();
        if (values[i] != null && values[i].getClass() != String.class) {
          values[i] = null;
          continue;
        }
        if (summary.getRatio(columnNames.get(i), values[i]) <= dimPercentageLimit) {
          String value = null;
          if (values[i] instanceof String) {
            value = ((String) values[i]);
          }
          if (metricValue / summary.maxMetricValue <= metricPercentageLimit) {
            if (values[i] instanceof String) {
              summary.totalDroppedSize += value.length() * 2;
            }
            values[i] = null;
          } else {
            ret = true;
          }
          if (value != null) {
            summary.totalSize += value.length() * 2;
          }
        }
      }
      if (ret) {
        summary.retained++;
      }
      if (!filter.put(Arrays.hashCode(values))) {
        summary.merged++;
        if (summary.merged - previousMerged > 150000 * 3) {
          output("  merged " + Arrays.asList(values));
          previousMerged = summary.merged;
        }
      }
      cursor.advance();
    }

  }

  public void countDims2(Cursor cursor, Summary summary, List<String> columnNames)
  {
    ColumnSelectorFactory columnSelectorFactory = cursor.getColumnSelectorFactory();
    final List<BaseObjectColumnValueSelector> selectors = columnNames
        .stream()
        .map(columnSelectorFactory::makeColumnValueSelector)
        .collect(Collectors.toList());
    ColumnValueSelector timeValueSelector = columnSelectorFactory.makeColumnValueSelector(ColumnHolder.TIME_COLUMN_NAME);

    // for each dimesion value create Dimension -> value -> count

    ColumnValueSelector metricValueSelector = columnSelectorFactory.makeColumnValueSelector(metric);

    double maxMetricValue = 0;
    LinkedHashSet<Long> timestamps = new LinkedHashSet<>();
    try {
      while (!cursor.isDone()) {
        summary.totalCount++;

        timestamps.add(timeValueSelector.getLong());
        for (int i = 0; i < columnNames.size(); i++) {
          double metricValue = metricValueSelector.getDouble();
          BaseObjectColumnValueSelector baseObjectColumnValueSelector = selectors.get(i);
          Object dimValue = baseObjectColumnValueSelector.getObject();
          String dimName = columnNames.get(i);
          if (dimValue instanceof HyperLogLogCollector) {
            summary.add(dimName, ((HyperLogLogCollector) dimValue).estimateCardinality());
          } else {
            summary.add(dimName, dimValue);
          }
          summary.maxMetricValue = Math.max(metricValue, summary.maxMetricValue);
        }
        cursor.advance();
      }
    }
    catch (OverflowException e) {
      output("total overflow");
    }
    output("Timestamps");
    for (Long timestamp : timestamps) {
      output("  " + new DateTime(timestamp, DateTimeZone.UTC));
    }
  }

  class OverflowException extends Exception
  {

  }

  class Summary
  {
    private final List<String> columnNames;
    public int totalCount;
    private LinkedHashMap<String, Dim> dims;
    public double maxMetricValue;
    public int retained;
    public int merged;
    public long totalSize;
    public long totalDroppedSize;

    public Summary(List<String> columnNames)
    {
      this.columnNames = columnNames;
      dims = new LinkedHashMap<>();
      for (String columnName : columnNames) {
        dims.put(columnName, new Dim());
      }
    }

    private int previous = totalCount;
    private boolean totalOverflow;

    public void add(String dimName, Object v) throws OverflowException
    {
      dims.get(dimName).add(v);
      if (previous - totalCount > 100000) {
        int sum = 0;
        for (Dim dim : dims.values()) {
          sum += dim.count();
        }
        if (sum >= 100000) {
          totalOverflow = true;
          throw new OverflowException();
        }
        previous = totalCount;
      }
    }

    public void reportDims()
    {
      for (String columnName : columnNames) {
        output(columnName);
        Dim dim = dims.get(columnName);
        dim.report(totalCount);
      }
    }

    public double getRatio(String column, Object dimValue)
    {
      Counts counts = dims.get(column).valueCounts.get(dimValue);
      if (counts != null) {
        return ((double) counts.count) / totalCount;
      }
      return Double.MAX_VALUE;
    }

    class Counts
    {
      int count;
    }

    class Dim
    {
      private HashMap<Object, Counts> valueCounts;

      {
        valueCounts = new HashMap<>();
      }

      boolean overflow = false;
      int dimRowCount;

      public void add(Object v)
      {
        if (!valueCounts.containsKey(v) && valueCounts.size() > 10000) {
          overflow = true;
        }

        dimRowCount++;
        Counts counts = valueCounts.computeIfAbsent(v, k -> new Counts());
        counts.count += 1;
      }

      public int count()
      {
        return valueCounts.size();
      }

      public void report(int count)
      {
        int dropped = 0;
        if (overflow) {
          output("  overflow");
        }
        for (Map.Entry<Object, Counts> e : valueCounts.entrySet()) {
          double ratio = ((double) e.getValue().count) / count;
          if (ratio > dimPercentageLimit) {
            output(String.format("    %s %.3f", e.getKey(), ratio));
          } else {
            dropped += e.getValue().count;
          }
        }
        output(String.format("  dropped %.3f", ((double) dropped) / count));
      }
    }
  }

  private void countDims(Injector injector, QueryableIndex index, List<String> dimNames, Summary summary)
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

//    Summary summary = new Summary(columnNames);
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

          ColumnValueSelector metricValueSelector = columnSelectorFactory.makeColumnValueSelector(metric);

          double maxMetricValue = 0;
          LinkedHashSet<Long> timestamps = new LinkedHashSet<>();
          try {
            while (!cursor.isDone()) {
              summary.totalCount++;

              timestamps.add(timeValueSelector.getLong());
              for (int i = 0; i < columnNames.size(); i++) {
                double metricValue = metricValueSelector.getDouble();
                BaseObjectColumnValueSelector baseObjectColumnValueSelector = selectors.get(i);
                Object dimValue = baseObjectColumnValueSelector.getObject();
                String dimName = columnNames.get(i);
                if (dimValue instanceof HyperLogLogCollector) {
                  summary.add(dimName, ((HyperLogLogCollector) dimValue).estimateCardinality());
                } else {
                  summary.add(dimName, dimValue);
                }
                maxMetricValue = Math.max(metricValue, maxMetricValue);
              }
              cursor.advance();
            }
          }
          catch (OverflowException e) {
            output("total overflow");
          }
          output("Timestamps");
          for (Long timestamp : timestamps) {
            output("  " + new DateTime(timestamp, DateTimeZone.UTC));
          }
          if (countTotalDropped) {
            cursor.reset();

            // metric > 1% then count retained

            BloomFilter<Integer> filter = BloomFilter.create(
                Funnels.integerFunnel(),
                1500000,
                0.001
            );
            Object[] values = new Object[columnNames.size()];
            int merged = 0;
            int previousMerged = 0;
            int retained = 0;
            output("Merged examples");
            long totalDroppedSize = 0;
            long totalSize = 0;
            while (!cursor.isDone()) {

              double metricValue = metricValueSelector.getDouble();
              boolean ret = false;
              for (int i = 0; i < columnNames.size(); i++) {
                BaseObjectColumnValueSelector baseObjectColumnValueSelector = selectors.get(i);
                values[i] = baseObjectColumnValueSelector.getObject();
                if (summary.getRatio(columnNames.get(i), values[i]) <= dimPercentageLimit) {
                  String value = null;
                  if (values[i] instanceof String) {
                    value = ((String) values[i]);
                  }
                  if (metricValue / maxMetricValue <= metricPercentageLimit) {
                    if (values[i] instanceof String) {
                      totalDroppedSize += value.length() * 2;
                    }
                    values[i] = null;
                  } else {
                    ret = true;
                  }
                  if (value != null) {
                    totalSize += value.length() * 2;
                  }
                }
              }
              if (ret) {
                retained++;
              }
              if (!filter.put(Arrays.hashCode(values))) {
                merged++;
                if (merged - previousMerged > 150000) {
                  output("  merged " + Arrays.asList(values));
                  previousMerged = merged;
                }
              }
              cursor.advance();
            }
            output("Merged " + ((double) merged) / summary.totalCount);
            output("Retained " + ((double) retained) / summary.totalCount);
            output("Total string size " + totalSize / 1024 / 1024 + " MiB");
            output(String.format(
                "Dropped string size %d MiB ratio %.3f",
                totalDroppedSize / 1024 / 1024,
                ((double) totalDroppedSize) / totalSize
            ));
          }

          return null;
        }
    );
    map.accumulate(null, (accumulated, in) -> null);


    output(String.format("Rows %s", summary.totalCount));
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
