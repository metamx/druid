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
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.aggregation.AggregatorFactory;
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
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Command(
    name = "truncate-segment"
)
public class TruncateSegment extends GuiceRunnable
{
  private static PrintWriter printWriter;

  public TruncateSegment()
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
      name = {"--outputDir"},
      title = "out",
      required = true)
  public String outputDir;

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
      required = false)
  public String metric = null;

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


  private void summarize(Injector injector, QueryableIndex index, List<String> dimNames)
  {
    final QueryableIndexStorageAdapter adapter = new QueryableIndexStorageAdapter(index);
    final List<String> dimensionNames = new ArrayList<>(dimNames);

    final Sequence<Cursor> cursors = adapter.makeCursors(
        Filters.toFilter(null),
        index.getDataInterval().withChronology(ISOChronology.getInstanceUTC()),
        VirtualColumns.EMPTY,
        Granularities.ALL,
        false,
        null
    );

    List<String> metricNames = SummarizeSegment2.getMetricNames(index);
          List<MakeSegment.Column> metrics = metricNames.stream()
                                                        .map(s -> new MakeSegment.Column(s))
                                                        .collect(Collectors.toList());
    MakeSegment.IndexBuilder[] indexBuilder = new MakeSegment.IndexBuilder[1];
    DimensionCount.Summary summary = new DimensionCount.Summary(dimensionNames, (String o) -> output(o), dimPercentageLimit);
    Sequence<Object> map = Sequences.map(
        cursors,
        cursor -> {
          ColumnSelectorFactory columnSelectorFactory = cursor.getColumnSelectorFactory();
          final List<BaseObjectColumnValueSelector> dimensionSelectors = dimensionNames
              .stream()
              .map(columnSelectorFactory::makeColumnValueSelector)
              .collect(Collectors.toList());

          metrics.forEach(c -> c.setSelector(columnSelectorFactory.makeColumnValueSelector(c.name)));

          ColumnValueSelector metricValueSelector = metric != null ? columnSelectorFactory.makeColumnValueSelector(
              metric) : null;

          ColumnValueSelector timeValueSelector = columnSelectorFactory.makeColumnValueSelector(ColumnHolder.TIME_COLUMN_NAME);

          summary.countDimsAndMetricMax(
              dimensionNames,
              cursor,
              dimensionSelectors,
              metricValueSelector,
              timeValueSelector
          );
            cursor.reset();

            // metric value > metricLimit then count as retained

            AggregatorFactory[] aggregators = index.getMetadata().getAggregators();
            indexBuilder[0] = new MakeSegment.IndexBuilder(aggregators, dimensionNames, outputDir, "concise", 750_000);
            String unknown = "unknown";
            while (!cursor.isDone()) {
              double metricValue = 0.0;
              if (metricValueSelector != null) {
                metricValue = metricValueSelector.getDouble();
              }
              MakeSegment.IndexBuilder.RowBuilder rowBuilder = indexBuilder[0].createRow().addTime(timeValueSelector.getLong());

              for (int i = 0; i < dimensionNames.size(); i++) {
                BaseObjectColumnValueSelector baseObjectColumnValueSelector = dimensionSelectors.get(i);
                Object value = baseObjectColumnValueSelector.getObject();
                String columnName = dimensionNames.get(i);
                if (summary.getRatio(columnName, value) <= dimPercentageLimit) {

                  if (metricValueSelector == null || metricValue / summary.maxMetricValue <= metricPercentageLimit) {
                    if (value instanceof String) {
                      value = unknown;
                    }
                  }
                }

                rowBuilder.addPair(columnName, value);
              }
              for (MakeSegment.Column c : metrics) {
                rowBuilder.addPair(c.name, c.getSelector().getObject());
              }
              rowBuilder.buildAndInsert();
              cursor.advance();
            }

          return null;
        }
    );
    map.accumulate(null, (accumulated, in) -> null);

    indexBuilder[0].finish();

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
