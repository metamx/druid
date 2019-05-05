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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.name.Names;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.guice.DruidProcessingModule;
import org.apache.druid.guice.QueryRunnerFactoryModule;
import org.apache.druid.guice.QueryableModule;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMergerV9;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.data.ConciseBitmapSerdeFactory;
import org.apache.druid.segment.data.RoaringBitmapSerdeFactory;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.incremental.IndexSizeExceededException;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.joda.time.chrono.ISOChronology;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Command(
    name = "make-segment"
)
public class MakeSegment extends GuiceRunnable
{
  public MakeSegment()
  {
    super(log);
  }

  private static final Logger log = new Logger(DumpSegment.class);

  @Option(
      name = {"-i", "--in"},
      title = "directory",
      required = true)
  public String directory;

  @Option(
      name = {"-o", "--out"},
      title = "out",
      required = true)
  public String out;

  @Option(
      name = {"--order"},
      title = "order",
      required = false)
  public String order;

  @Option(
      name = {"--bitmap"},
      title = "bitmap",
      required = false)
  public String bitmap = "concise";

  @Override
  public void run()
  {
    IncrementalIndex index = readData();
    persist(index);
  }

  static class Column
  {
    String name;

    public ColumnValueSelector getSelector()
    {
      return selector;
    }

    private ColumnValueSelector selector;

    public Column(String name)
    {
      this.name = name;
    }

    public void setSelector(ColumnValueSelector selector)
    {
      this.selector = selector;
    }

  }

  static class IndexBuilder {
    public IncrementalIndex getIncrementalIndex()
    {
      return incrementalIndex;
    }

    private IncrementalIndex incrementalIndex;
    private final List<String> dimensions;

    IndexBuilder(AggregatorFactory[] aggregatorFactories, List<String> dimensions) {
      incrementalIndex = IncrementalIndexes.createIncremental(Arrays.asList(aggregatorFactories));
      this.dimensions = dimensions;
    }
    class RowBuilder {
      Map<String, Object> event = new HashMap<>();
      private long time;

      RowBuilder addTime(long time) {
        this.time = time;
        return this;
      }
      RowBuilder addPair(String name, Object value) {
         event.put(name, value);
         return this;
      }
      void buildAndInsert() {
        MapBasedInputRow row = new MapBasedInputRow(
            time,
            dimensions,
            event
        );
        IncrementalIndexes.fillIndex(incrementalIndex, row);
      }
    }
    RowBuilder createRow() {
      return new RowBuilder();
    }
  }

  private IncrementalIndex readData()
  {
    IndexBuilder[] indexBuilder = new IndexBuilder[1];
    final Injector injector = makeInjector();
    final IndexIO indexIO = injector.getInstance(IndexIO.class);
    try (final QueryableIndex index = indexIO.loadIndex(new File(directory))) {
      final QueryableIndexStorageAdapter adapter = new QueryableIndexStorageAdapter(index);
      final Sequence<Cursor> cursors = adapter.makeCursors(
          null,
          index.getDataInterval().withChronology(ISOChronology.getInstanceUTC()),
          VirtualColumns.EMPTY,
          Granularities.ALL,
          false,
          null
      );
      List<String> metricNames = SummarizeSegment2.getMetricNames(index);
      AggregatorFactory[] aggregators = index.getMetadata().getAggregators();
      new LinkedHashSet<>(index.getColumnNames()).removeAll(metricNames);
      ArrayList<String> dimensionsList = new ArrayList<>(new LinkedHashSet<>(index.getColumnNames()));
      indexBuilder[0] = new IndexBuilder(aggregators, dimensionsList);

      if (order != null) {
        List<String> orderList = Arrays.asList(order.split(","));
        Collections.reverse(orderList);
        for (String s : orderList) {
          dimensionsList.remove(s);
          dimensionsList.add(0, s);
        }
      }
      List<Column> metrics = metricNames.stream()
                                        .map(s -> new Column(s))
                                        .collect(Collectors.toList());
      List<Column> dimensions = new LinkedHashSet<>(index.getColumnNames()).stream()
                                                                           .map(s -> new Column(s))
                                                                           .collect(Collectors.toList());
      int[] count = new int[1];
      final Sequence<Object> sequence = Sequences.map(
          cursors,
          cursor -> {
            ColumnSelectorFactory columnSelectorFactory = cursor.getColumnSelectorFactory();
            dimensions.forEach(c -> c.setSelector(columnSelectorFactory.makeColumnValueSelector(c.name)));
            metrics.forEach(c -> c.setSelector(columnSelectorFactory.makeColumnValueSelector(c.name)));
            ColumnValueSelector timeSelector = columnSelectorFactory.makeColumnValueSelector(ColumnHolder.TIME_COLUMN_NAME);

            while (!cursor.isDone()) {
              IndexBuilder.RowBuilder rowBuilder = indexBuilder[0].createRow().addTime(timeSelector.getLong());
              for (Column c : dimensions) {
                final Object value = c.selector.getObject();
                rowBuilder.addPair(c.name, value);
              }
              for (Column c : metrics) {
                final Object value = c.selector.getObject();
                rowBuilder.addPair(c.name, value);
              }
              rowBuilder.buildAndInsert();
              count[0]++;
              if (count[0] % 25000 == 0) {
                log.info("Processing " + count[0]);
              }
              cursor.advance();
            }
            return null;
          }
      );
      sequence.accumulate(null, (accumulated, in) -> null);

    }
    catch (IOException e) {
      e.printStackTrace();
    }

    return indexBuilder[0].incrementalIndex;
  }

  static class IncrementalIndexes
  {
    private static IndexMergerV9 INDEX_MERGER_V9;
    private static IndexIO INDEX_IO;
    public static ObjectMapper JSON_MAPPER;

    private static IncrementalIndex createIncremental(List<AggregatorFactory> aggs)
    {
      JSON_MAPPER = new DefaultObjectMapper();
      INDEX_IO = new IndexIO(
          JSON_MAPPER,
          new ColumnConfig()
          {
            @Override
            public int columnCacheSizeBytes()
            {
              return 0;
            }
          }
      );
      INDEX_MERGER_V9 = new IndexMergerV9(JSON_MAPPER, INDEX_IO, OffHeapMemorySegmentWriteOutMediumFactory.instance());

      return new IncrementalIndex.Builder()
          .setIndexSchema(
              new IncrementalIndexSchema.Builder()
                  .withMetrics(aggs.toArray(new AggregatorFactory[aggs.size()]))
                  .withRollup(true)
                  .build()
          )
          .setReportParseExceptions(false)
          .setMaxRowCount(2_000_000)
          .buildOnheap();
    }

    private static void fillIndex(IncrementalIndex incrementalIndex, MapBasedInputRow row)
    {
      try {
        incrementalIndex.add(row);
      }
      catch (IndexSizeExceededException e) {
        e.printStackTrace();
      }
    }

    static void persist0(IncrementalIndex incrementalIndex, String outDir, String bitmapType)
    {
      try {
        File indexFile = INDEX_MERGER_V9.persist(
            incrementalIndex,
            new File(outDir),
            new IndexSpec(bitmapType.equals("concise")
                          ? new ConciseBitmapSerdeFactory()
                          : new RoaringBitmapSerdeFactory(null), null, null, null),
            null
        );
      }
      catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  private void persist(IncrementalIndex incrementalIndex)
  {

    String outDir = this.out;
    String bitmapType = this.bitmap;
    IncrementalIndexes.persist0(incrementalIndex, outDir, bitmapType);
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
