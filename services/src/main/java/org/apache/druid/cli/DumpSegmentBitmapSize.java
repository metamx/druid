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
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.name.Names;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.netty.util.SuppressForbidden;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.collections.bitmap.WrappedRoaringBitmap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.guice.DruidProcessingModule;
import org.apache.druid.guice.QueryRunnerFactoryModule;
import org.apache.druid.guice.QueryableModule;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.column.BitmapIndex;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.timeline.SegmentId;
import org.roaringbitmap.IntIterator;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

@Command(
    name = "dump-segment-bitmap-size"
)
public class DumpSegmentBitmapSize extends GuiceRunnable
{
  private static final Logger log = new Logger(DumpSegmentBitmapSize.class);

  public DumpSegmentBitmapSize()
  {
    super(log);
  }

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
      name = {"-c", "--column"},
      title = "column",
      description = "Column to include, specify multiple times for multiple columns, or omit to include all columns.",
      required = false)
  public List<String> columnNamesFromCli = new ArrayList<>();

  @Override
  public void run()
  {
    final Injector injector = makeInjector();
    final IndexIO indexIO = injector.getInstance(IndexIO.class);


    try (final QueryableIndex index = indexIO.loadIndex(new File(directory))) {
      runBitmaps(injector, index);
    }
    catch (IOException e) {
      e.printStackTrace();
    }
  }


  private void runBitmaps(final Injector injector, final QueryableIndex index) throws IOException
  {
    final List<String> columnNames = getColumnsToInclude(index);

    withOutputStream(
        out -> {
          long totalSum = 0;
          int totalCount = 0;
          long totalRoaringSum = 0;
          for (final String columnName : columnNames) {
            if (!columnNamesFromCli.isEmpty() && !columnNamesFromCli.contains(columnName)) {
              continue;
            }
            final ColumnHolder columnHolder = index.getColumnHolder(columnName);
            final BitmapIndex bitmapIndex = columnHolder.getBitmapIndex();

            if (bitmapIndex != null) {
              assert out != null;
              out.println(columnName + "\n");
              String format = String.format("  cardinality %s\n", bitmapIndex.getCardinality());
              out.println(format);
              int sumSize = 0;
              int maxSize = 0;
              int perColumnCount = 0;
              int sumRoaringSize = 0;
              int maxRoaringSize = 0;
              for (int i = 0; i < bitmapIndex.getCardinality(); i++) {
                WrappedRoaringBitmap wrappedRoaringBitmap = new WrappedRoaringBitmap();
                String val = NullHandling.nullToEmptyIfNeeded(bitmapIndex.getValue(i));
                if (val != null) {
                  final ImmutableBitmap bitmap = bitmapIndex.getBitmap(i);
                  int length = bitmap.toBytes().length;
                  IntIterator iterator = bitmap.iterator();
                  while (iterator.hasNext()) {
                    int next = iterator.next();
                    wrappedRoaringBitmap.add(next);
                  }
                  int sizeInBytes = wrappedRoaringBitmap.getSizeInBytes();
                  sumRoaringSize += sizeInBytes;
                  sumSize += length;
                  totalSum += length;
                  totalRoaringSum += sizeInBytes;
                  perColumnCount++;
                  totalCount++;
                  maxSize = Math.max(maxSize, length);
                  maxRoaringSize = Math.max(sizeInBytes, maxRoaringSize);
                  wrappedRoaringBitmap.clear();
                }
              }
              out.println(String.format("  %s", columnName));
              out.println(String.format("    Concise Sum %s Max %s Avg %.3f\n", sumSize, maxSize, ((double)sumSize)/perColumnCount));
              out.println(String.format("    Roaring Sum %s Max %s Avg %.3f\n", sumRoaringSize, maxRoaringSize, ((double)sumRoaringSize)/perColumnCount));
            }
          }
          out.println(String.format("====================================="));
          out.println(String.format("Total"));
          out.println(String.format("  Concise sum %s avg %.3f\n", totalSum, ((double) totalSum)/totalCount));
          out.println(String.format("  Roaring sum %s avg %.3f\n", totalRoaringSum, ((double) totalRoaringSum)/totalCount));

          out.println(String.format("Ratio %.3f", ((double)totalRoaringSum)/totalSum));
          return null;
        }
    );
  }

  private List<String> getColumnsToInclude(final QueryableIndex index)
  {
    final Set<String> columnNames = Sets.newLinkedHashSet(columnNamesFromCli);

    // Empty columnNames => include all columns.
    if (columnNames.isEmpty()) {
      columnNames.add(ColumnHolder.TIME_COLUMN_NAME);
      Iterables.addAll(columnNames, index.getColumnNames());
    } else {
      // Remove any provided columns that do not exist in this segment.
      for (String columnName : ImmutableList.copyOf(columnNames)) {
        if (index.getColumnHolder(columnName) == null) {
          columnNames.remove(columnName);
        }
      }
    }

    return ImmutableList.copyOf(columnNames);
  }

  @SuppressForbidden(reason = "System#out")
  private <T> T withOutputStream(Function<PrintStream, T> f) throws IOException
  {
    if (outputFileName == null) {
      return f.apply(System.out);
    } else {
      try (final PrintStream out = new PrintStream(new FileOutputStream(outputFileName))) {
        return f.apply(out);
      }
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

  private static <T> Sequence<T> executeQuery(final Injector injector, final QueryableIndex index, final Query<T> query)
  {
    final QueryRunnerFactoryConglomerate conglomerate = injector.getInstance(QueryRunnerFactoryConglomerate.class);
    final QueryRunnerFactory<T, Query<T>> factory = conglomerate.findFactory(query);
    final QueryRunner<T> runner = factory.createRunner(new QueryableIndexSegment(index, SegmentId.dummy("segment")));
    return factory
        .getToolchest()
        .mergeResults(factory.mergeRunners(Execs.directExecutor(), ImmutableList.of(runner)))
        .run(QueryPlus.wrap(query), new HashMap<>());
  }

  private static <T> void evaluateSequenceForSideEffects(final Sequence<T> sequence)
  {
    sequence.accumulate(null, (accumulated, in) -> null);
  }

}
