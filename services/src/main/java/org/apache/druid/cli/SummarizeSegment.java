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
import org.joda.time.chrono.ISOChronology;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

@Command(
    name = "summarize-by-value-segment"
)
public class SummarizeSegment extends GuiceRunnable {
  public SummarizeSegment()
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

  @Override
  public void run()
  {
    final Injector injector = makeInjector();
    final IndexIO indexIO = injector.getInstance(IndexIO.class);
    try (final QueryableIndex index = indexIO.loadIndex(new File(directory))) {
      List<String> metricNames = getMetricNames(index);
      log.info("Metrics " + metricNames);
      summarize(injector, index, metricNames);
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private void summarize(Injector injector, QueryableIndex index, List<String> metricNames)
  {
    final QueryableIndexStorageAdapter adapter = new QueryableIndexStorageAdapter(index);
    final List<String> columnNames = new ArrayList<>(metricNames);

    final Sequence<Cursor> cursors = adapter.makeCursors(
        Filters.toFilter(null),
        index.getDataInterval().withChronology(ISOChronology.getInstanceUTC()),
        VirtualColumns.EMPTY,
        Granularities.ALL,
        false,
        null
    );
    class MetricStat {
      double max;
      List<Double> values = new ArrayList<Double>();
      int retainedCount;

      public void updateMax(double aDouble)
      {
        max = Math.max(aDouble, max);
      }

      Map<String, Integer> blockerCounts = new TreeMap<>();
      public void countBlockers(ArrayList<String> blockers)
      {
        for (String blocker : blockers) {
          Integer orDefault = blockerCounts.getOrDefault(blocker, 0);
          blockerCounts.put(blocker, orDefault + 1);
        }
        retainedCount++;
      }

      public void clearMax()
      {
        max = 0;
      }
    }
      class Summary
      {
        boolean notAnumber = false;
        Map<String, String> notNumbers = new HashMap<>();
        private final List<String> columnNames;
        int count;
        private Map<String, MetricStat> metricStatsByName = new LinkedHashMap<String, MetricStat>();
        private List<MetricStat> metricStatsByIndex = new ArrayList<>();
        int dropCount;
        int retainedCount;
        private boolean[] negatives;

        public double getDropped() {
          return ((double)dropCount) / count;
        }

        public double getRetained() {
          return ((double)retainedCount) / count;
        }

        public Summary(List<String> columnNames) {
          this.columnNames = columnNames;
          for (String columnName : columnNames) {
            MetricStat metricStat = new MetricStat();
            metricStatsByIndex.add(metricStat);
            metricStatsByName.put(columnName, metricStat);
          }
        }

        private MetricStat getStat(int i)
        {
          return metricStatsByIndex.get(i);
        }
        private MetricStat getStat(String columnName)
        {
          return metricStatsByName.get(columnName);
        }

        List<double[]> collected = new ArrayList<double[]>();
        public void collect(double[] doubles)
        {
            collected.add(doubles);
          for (int i = 0; i < doubles.length; i++) {
            double aDouble = doubles[i];
            getStat(i).updateMax(aDouble);
          }
        }

        public void summarizeCollected()
        {
          double[] ratios = new double[columnNames.size()];
          ArrayList<String> blockers = new ArrayList<>();
          ArrayList<String> dropped = new ArrayList<>();
          for (int rowIndex = 0; rowIndex < collected.size(); rowIndex++) {
            double[] doubles = collected.get(rowIndex);
            for (int columnIndex = 0; columnIndex < doubles.length; columnIndex++) {
              double aDouble = doubles[columnIndex];
              if (Math.abs(getStat(columnIndex).max) < 0.000000001) {
                ratios[columnIndex] = 0.0;
              } else{
                ratios[columnIndex] = aDouble / getStat(columnIndex).max;
              }
            }
            for (int j = 0; j < ratios.length; j++) {
              String columnName = columnNames.get(j);
              double ratio = ratios[j];
              if (ratio < 0.01) {
                dropped.add(columnName);
              } else {
                blockers.add(columnName);
              }
            }
            if (!dropped.isEmpty()) {
              if (blockers.isEmpty())
                dropCount++;
              else if (blockers.size() != dropped.size()) {
                retainedCount++;
                countBlockers(dropped, blockers);
              }
            }
            blockers.clear();
            dropped.clear();
          }
        }

        private void countBlockers(ArrayList<String> dropped, ArrayList<String> blockers)
        {
          for (int i = 0; i < dropped.size(); i++) {
            String s = dropped.get(i);
            getStat(s).countBlockers(blockers);
          }
        }

        public void resetCollected()
        {
          collected.clear();
          for (int i = 0; i < metricStatsByIndex.size(); i++) {
            metricStatsByIndex.get(i).clearMax();
          }
        }

        public void negatives(boolean[] negatives)
        {
          this.negatives = negatives;
        }

        public void printNegatives()
        {
          output("Negatives:");
          for (int i = 0; i < columnNames.size(); i++) {
            String s = columnNames.get(i);
            if (negatives[i])
              output("  " + s);
          }
        }
      }
      Set<String> hll = new LinkedHashSet<>();
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


          // foreach column find max (create array of ColumnStat objects)
          // foreach row find metric value ratio to max
          //    if < 1% for all metrics drop row and count dropped rows
          //    else < 1% at least one metric, create/update MetricStat for each metric < 1% (count metric > 1% - blocking metrics)
          // print percentage of dropped rows
          // foreach MetricStat print retained ratio to all rows, with percentage for each blocking metric
          long previous = timeValueSelector.getLong();
          boolean[] negatives = new boolean[columnNames.size()];
          while (!cursor.isDone()) {
            summary.count++;

              // iterate and find max
              // iterate and count dropped
            long time = timeValueSelector.getLong();
            if (previous != time) {
              summary.summarizeCollected();
              summary.resetCollected();
              previous = time;
            }
            double[] doubles = new double[columnNames.size()];
            for (int i = 0; i < columnNames.size(); i++) {
              BaseObjectColumnValueSelector baseObjectColumnValueSelector = selectors.get(i);
              Object object = baseObjectColumnValueSelector.getObject();
              if (object instanceof Number) {
                Number value = (Number) object;
                doubles[i] = value.doubleValue();
                if (doubles[i] < 0)
                  negatives[i] = true;
              } else if (object instanceof HyperLogLogCollector) {
                doubles[i] = ((HyperLogLogCollector)object).estimateCardinality();
                hll.add(columnNames.get(i));
              } else {
                summary.notAnumber = true;
                summary.notNumbers.putIfAbsent(columnNames.get(i) ,baseObjectColumnValueSelector.classOfObject().getSimpleName());
                doubles[i] = 0;
              }
            }
            summary.collect(doubles);
            summary.negatives(negatives);
            cursor.advance();
          }
          summary.summarizeCollected();
          summary.resetCollected();
          return null;
        }
    );
    map.accumulate(null, (accumulated, in) -> null);


    output(String.format("Rows %s", summary.count));
    output(String.format("Dropped %.3f Retained %.3f", summary.getDropped(), summary.getRetained()));
    for (String columnName : columnNames) {
      MetricStat metricStat = summary.metricStatsByName.get(columnName);

      if (!metricStat.blockerCounts.isEmpty()) {
        Set<Map.Entry<String, Integer>> entries = metricStat.blockerCounts.entrySet();
        TreeMap<Integer, List<String>> sorted = new TreeMap<>((o1, o2) -> -1 * Integer.compare(o1, o2));
        for (Map.Entry<String, Integer> entry : entries) {
          List<String> strings = sorted.get(entry.getValue());
          if (strings == null) {
            sorted.put(entry.getValue() , strings = new ArrayList<>());
          }
          strings.add(entry.getKey());
        }
        String s = sorted.entrySet().stream().map(e -> String.format("%s %.5f", e.getValue(), ((double) e.getKey()) / metricStat.retainedCount)).collect(Collectors.joining(" "));
        output(String.format("%s retained due to %s", columnName, s));
      }

    }
    if (summary.notAnumber) {
      output("Found not a number " + summary.notNumbers);
    }
    summary.printNegatives();

    output("HLL columns " + hll);
  }

  private void output(String s)
  {
    System.out.println(s);
  }

  private List<String> getMetricNames(QueryableIndex index)
  {
    Set<String> columnNames = new LinkedHashSet<>(index.getColumnNames());
    Set<String> availableDimensions = Sets.newLinkedHashSet(index.getAvailableDimensions());
    return new ArrayList<>(Sets.difference(columnNames, availableDimensions));
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
