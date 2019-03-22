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
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.name.Names;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.DimensionsSpec;
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
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMergerV9;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.incremental.IndexSizeExceededException;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.joda.time.chrono.ISOChronology;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Command(
    name = "segment-correlation"
)
public class ColumnCorrelation extends GuiceRunnable
{
  public ColumnCorrelation()
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

  private static IndexMergerV9 INDEX_MERGER_V9;
  private static IndexIO INDEX_IO;
  public static ObjectMapper JSON_MAPPER;

  @Override
  public void run()
  {
    List<AggregatorFactory> aggs = new ArrayList<>();
    readData();
  }

  class Column
  {
    String name;
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

  private void readData()
  {
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
//      List<AggregatorFactory> collect = metricNames.stream()
//                                                     .map(s -> new LongSumAggregatorFactory(s, s))
//                                                     .collect(Collectors.toList());
//      index.
      LinkedHashSet<String> dims0 = new LinkedHashSet<>(index.getColumnNames());
      dims0.removeAll(metricNames);
      ArrayList<String> dimensionsList = new ArrayList<>(dims0);
      Collections.sort(dimensionsList, new Comparator<String>() {

        @Override
        public int compare(String o1, String o2)
        {
          int cardinality1 = index.getColumnHolder(o1).getBitmapIndex().getCardinality();
          int cardinality2 = index.getColumnHolder(o2).getBitmapIndex().getCardinality();
          return Integer.compare(cardinality1, cardinality2);
        }

      });
      log.info("Sorted dims: " + dimensionsList);
//      dimensionsList.remove("has_loc");
//      dimensionsList.add(0, "has_loc");
//      dimensionsList.remove("site_id");
//      dimensionsList.add(0, "site_id");
      List<Column> metrics =
                  metricNames.stream()
//                             .filter(s -> index.getColumnHolder(s).getCapabilities().getType().isNumeric())
                             .map(s -> new Column(s))
                             .collect(Collectors.toList());
List<Column> dimensions = dims0.stream()
                  .map(s -> new Column(s))
                  .collect(Collectors.toList());
int [] count = new int[1];
      final Sequence<Object> sequence = Sequences.map(
          cursors,
          new Function<Cursor, Object>()
          {
            @Override
            public Object apply(Cursor cursor)
            {

              ColumnSelectorFactory columnSelectorFactory = cursor.getColumnSelectorFactory();
              dimensions.forEach(c -> c.setSelector(columnSelectorFactory.makeColumnValueSelector(c.name)));
              metrics.forEach(c -> c.setSelector(columnSelectorFactory.makeColumnValueSelector(c.name)));
              double[][] doubles = new double[index.getNumRows()][dimensions.size()];
              List<DimensionSelector> dimensionSelectors = dimensions.stream()
                                                          .map(d -> DefaultDimensionSpec.of(d.name))
                                                          .map(d -> {
                                                            return cursor.getColumnSelectorFactory()
                                                                         .makeDimensionSelector(d);
                                                          })
                                                          .collect(Collectors.toList());

              while (!cursor.isDone()) {
                int columnCount= 0;
                for (int i = 0; i < dimensionSelectors.size(); i++) {
                  DimensionSelector selector = dimensionSelectors.get(i);
                  IndexedInts row = selector.getRow();
                  if (row.size() == 0) {
//                    dimensions.get(i).selector.getObject();
//                    log.info("a");
                    doubles[count[0]][columnCount++] = 0;
                  } else {
                    doubles[count[0]][columnCount++] = row.get(0);
                  }
                }
                count[0]++;
                if (count[0]% 25000 == 0)
                  log.info("Processing " + count[0]);
                cursor.advance();
              }
              PearsonsCorrelation pearsonsCorrelation = new PearsonsCorrelation(doubles);
              RealMatrix correlationMatrix = pearsonsCorrelation.getCorrelationMatrix();
              BufferedWriter br = null;
              log.info("Starting file");
              try {
              br = new BufferedWriter(new FileWriter(out));
              StringBuilder sb = new StringBuilder();
              sb.append("dims,");
                for (int i = 0; i < dimensionsList.size(); i++) {
                  String s = dimensionsList.get(i);
                  sb.append(s);
                  if (i != dimensionsList.size() -1) {
                    sb.append(",");
                  }
                }
                br.write(sb.toString());
                br.write("\n");
                doubles = correlationMatrix.getData();
              for (int i = 0; i < doubles.length; i++) {
                double[] row = doubles[i];
                sb.delete(0, sb.length());
                sb.append(dimensionsList.get(i)).append(",");
                for (int j = 0; j < row.length; j++) {
                  double v = row[j];
                  sb.append(String.format("%.3f", v));
                  if (j != row.length -1)
                    sb.append(",");
                }

                sb.append("\n");
                br.write(sb.toString());
              }

              } catch (Exception e) {
                e.printStackTrace();
              } finally {
                try {
                  br.close();
                }
                catch (IOException e) {
                  e.printStackTrace();
                }
              }

              return null;
            }
          }
      );
      sequence.accumulate(null, (accumulated, in) -> null);

    }
    catch (IOException e) {
      e.printStackTrace();
    }

  }

  private IncrementalIndex createIncremental(List<AggregatorFactory> aggs)
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

  private void fillIndex(IncrementalIndex incrementalIndex, MapBasedInputRow row)
  {
    try {
      incrementalIndex.add(row);
    }
    catch (IndexSizeExceededException e) {
      e.printStackTrace();
    }
  }

  private void persist(IncrementalIndex incrementalIndex)
  {
    try {
      File indexFile = INDEX_MERGER_V9.persist(
          incrementalIndex,
          new File(out),
          new IndexSpec(),
          null
      );
    }
    catch (IOException e) {
      e.printStackTrace();
    }
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
