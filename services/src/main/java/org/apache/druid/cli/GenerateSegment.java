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
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.name.Names;
import com.opencsv.CSVReader;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.druid.guice.DruidProcessingModule;
import org.apache.druid.guice.QueryRunnerFactoryModule;
import org.apache.druid.guice.QueryableModule;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.segment.column.ColumnConfig;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Command(
    name = "generate-segment"
)
public class GenerateSegment extends GuiceRunnable
{
  public GenerateSegment()
  {
    super(log);
  }

  private static final Logger log = new Logger(DumpSegment.class);

  @Option(
      name = {"-i", "--in"},
      title = "csv",
      required = true)
  public String csv;

  @Option(
      name = {"-o", "--out"},
      title = "out",
      required = true)
  public String out;

  @Option(
      name = {"--bitmap"},
      title = "bitmap",
      required = false)
  public String bitmap = "concise";

  @Override
  public void run()
  {
    File file = new File(out);
    if (!file.exists()) {
      if (!file.mkdirs()) {
        System.out.println("Cannot create " + out);
        return;
      }
    }
    try {
      readData();
    }
    catch (IOException e) {
      e.printStackTrace();
    }
  }


  private void readData() throws IOException
  {
    Reader reader = Files.newBufferedReader(Paths.get(csv));
    CSVReader csvReader = new CSVReader(reader);
    String[] strings = csvReader.readNext();
    List<String> dims = Arrays.stream(strings).skip(1).collect(Collectors.toList());

    MakeSegment.IndexBuilder[] indexBuilder = new MakeSegment.IndexBuilder[1];
    DoubleSumAggregatorFactory doubleSumAggregatorFactory = new DoubleSumAggregatorFactory("a", "a");
    indexBuilder[0] = new MakeSegment.IndexBuilder(
        new AggregatorFactory[]{doubleSumAggregatorFactory},
        dims,
        out,
        bitmap,
        750_000
    );

    int[] count = new int[1];
    long time = 0;
    String[] nextRecord;
    while ((nextRecord = csvReader.readNext()) != null) {

      MakeSegment.IndexBuilder.RowBuilder rowBuilder = indexBuilder[0].createRow().addTime(time++);
      for (int i = 1; i < nextRecord.length; i++) {
        String s = nextRecord[i];
        rowBuilder.addPair(dims.get(i - 1), s);
      }
      rowBuilder.addPair("a", nextRecord[0]);
      rowBuilder.buildAndInsert();
      count[0]++;
      if (count[0] % 25000 == 0) {
        log.info("Processing " + count[0]);
      }
    }
    indexBuilder[0].finish();
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
