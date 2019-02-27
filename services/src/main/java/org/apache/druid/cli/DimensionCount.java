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
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import org.apache.druid.collections.bitmap.WrappedImmutableConciseBitmap;
import org.apache.druid.extendedset.intset.ConciseSet;
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
import org.apache.druid.segment.column.BitmapIndex;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.filter.Filters;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.chrono.ISOChronology;

import java.io.File;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Command(
    name = "summarize-segment-dimensions"
)
public class DimensionCount extends GuiceRunnable
{
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

  @Override
  public void run()
  {
    final Injector injector = makeInjector();
    final IndexIO indexIO = injector.getInstance(IndexIO.class);
    try (final QueryableIndex index = indexIO.loadIndex(new File(directory))) {
      List<String> dimNames = getDimNames(index);
      printWriter = new PrintWriter(new File(outputFileName));
      output("Dims " + dimNames);
      output("Metric limit " + metricPercentageLimit);
      output("Dim limit " + dimPercentageLimit);
      long l = System.currentTimeMillis();
      summarize(injector, index, dimNames);
      printWriter.println("time " + ((System.currentTimeMillis() - l) / 1000.0 / 60));
      printWriter.close();
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  class OverflowException extends Exception
  {

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

    class Summary
    {
      private final List<String> columnNames;
      private final LinkedHashMap<String, Dim> dims;
      public int totalCount;
      public int droppedBitmapSize;
      public int droppedBitmapCompressedSize;

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

            // metric value > metricLimit then count as retained

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
            Map<String, ConciseSet> sets = new HashMap<>();
            int rowCount = 0;

            LZ4Factory factory = LZ4Factory.fastestInstance();
            LZ4Compressor compressor = factory.fastCompressor();
            byte[] compressed = new byte[1024*1024*20];
            BloomFilter<CharSequence> valueFilter = BloomFilter.create(
                Funnels.stringFunnel(Charset.forName("UTF8")),
                1500000,
                0.001
            );

            while (!cursor.isDone()) {
              double metricValue = metricValueSelector.getDouble();
              boolean ret = false;

              for (int i = 0; i < columnNames.size(); i++) {
                BaseObjectColumnValueSelector baseObjectColumnValueSelector = selectors.get(i);
                values[i] = baseObjectColumnValueSelector.getObject();
                String columnName = columnNames.get(i);
                if (summary.getRatio(columnName, values[i]) <= dimPercentageLimit) {
                  BitmapIndex bitmapIndex = index.getColumnHolder(columnName).getBitmapIndex();

                  if (metricValue / maxMetricValue <= metricPercentageLimit) {
                    String value;// = null;
                    if (values[i] instanceof String) {
                      value = ((String) values[i]);
                      if (valueFilter.put(value)) {
                        WrappedImmutableConciseBitmap bitmap = (WrappedImmutableConciseBitmap) bitmapIndex.getBitmap(
                            bitmapIndex.getIndex(value));
                        byte[] bytes = bitmap.getBitmap().toBytes();
                        summary.droppedBitmapSize += bytes.length;
                        int compressedLength = compressor.compress(
                            bytes,
                            0,
                            bytes.length,
                            compressed,
                            0,
                            compressed.length
                        );
                        summary.droppedBitmapCompressedSize += compressedLength;
                      }
                      ConciseSet conciseSet = sets.computeIfAbsent(columnName, k -> new ConciseSet());
                      conciseSet.add(rowCount);
                      values[i] = null;
                    }
                  } else {
                    ret = true;
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
              rowCount++;
            }
            output("Merged " + ((double) merged) / summary.totalCount);
            output("Retained " + ((double) retained) / summary.totalCount);
            output("Dropped bitmap size " + summary.droppedBitmapSize);
            output("Dropped bitmap compressed size " + summary.droppedBitmapCompressedSize);
            int createdBitmapSize = 0;
            int createdBitmapCompressedSize = 0;
            for (String s : sets.keySet()) {
              int[] words = sets.get(s).getWords();
              createdBitmapSize += words.length * 4;
              ByteBuffer buf = ByteBuffer.allocate(words.length * Integer.BYTES);
              buf.asIntBuffer().put(words);
              byte[] array = buf.array();
              int compressedLength = compressor.compress(array, 0, array.length, compressed, 0, compressed.length);
              createdBitmapCompressedSize += compressedLength;
            }
            output("Created bitmap size " + createdBitmapSize);
            output("Created bitmap compressed size " + createdBitmapCompressedSize);
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
