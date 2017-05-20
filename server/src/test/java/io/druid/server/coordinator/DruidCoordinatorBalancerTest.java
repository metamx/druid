/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.server.coordinator;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.druid.client.ImmutableDruidServer;
import io.druid.concurrent.Execs;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 */
public class DruidCoordinatorBalancerTest
{
  private static final int MAX_SEGMENTS_TO_MOVE = 5;
  private DruidCoordinator coordinator;
  private ImmutableDruidServer druidServer1;
  private ImmutableDruidServer druidServer2;
  private ImmutableDruidServer druidServer3;
  private ImmutableDruidServer druidServer4;
  private DataSegment segment1;
  private DataSegment segment2;
  private DataSegment segment3;
  private DataSegment segment4;
  Map<String, DataSegment> segments;

  @Before
  public void setUp() throws Exception
  {
    coordinator = EasyMock.createMock(DruidCoordinator.class);
    druidServer1 = EasyMock.createMock(ImmutableDruidServer.class);
    druidServer2 = EasyMock.createMock(ImmutableDruidServer.class);
    druidServer3 = EasyMock.createMock(ImmutableDruidServer.class);
    druidServer4 = EasyMock.createMock(ImmutableDruidServer.class);
    segment1 = EasyMock.createMock(DataSegment.class);
    segment2 = EasyMock.createMock(DataSegment.class);
    segment3 = EasyMock.createMock(DataSegment.class);
    segment4 = EasyMock.createMock(DataSegment.class);

    // Mock stuff that the coordinator needs
    coordinator.moveSegment(
        EasyMock.<ImmutableDruidServer>anyObject(),
        EasyMock.<ImmutableDruidServer>anyObject(),
        EasyMock.<String>anyObject(),
        EasyMock.<LoadPeonCallback>anyObject()
    );
    EasyMock.expectLastCall().anyTimes();
    EasyMock.replay(coordinator);

    DateTime start1 = new DateTime("2012-01-01");
    DateTime start2 = new DateTime("2012-02-01");
    DateTime version = new DateTime("2012-03-01");
    segment1 = new DataSegment(
        "datasource1",
        new Interval(start1, start1.plusHours(1)),
        version.toString(),
        Maps.<String, Object>newHashMap(),
        Lists.<String>newArrayList(),
        Lists.<String>newArrayList(),
        NoneShardSpec.instance(),
        0,
        11L
    );
    segment2 = new DataSegment(
        "datasource1",
        new Interval(start2, start2.plusHours(1)),
        version.toString(),
        Maps.<String, Object>newHashMap(),
        Lists.<String>newArrayList(),
        Lists.<String>newArrayList(),
        NoneShardSpec.instance(),
        0,
        7L
    );
    segment3 = new DataSegment(
        "datasource2",
        new Interval(start1, start1.plusHours(1)),
        version.toString(),
        Maps.<String, Object>newHashMap(),
        Lists.<String>newArrayList(),
        Lists.<String>newArrayList(),
        NoneShardSpec.instance(),
        0,
        4L
    );
    segment4 = new DataSegment(
        "datasource2",
        new Interval(start2, start2.plusHours(1)),
        version.toString(),
        Maps.<String, Object>newHashMap(),
        Lists.<String>newArrayList(),
        Lists.<String>newArrayList(),
        NoneShardSpec.instance(),
        0,
        8L
    );

    segments = new HashMap<String, DataSegment>();
    segments.put("datasource1_2012-01-01T00:00:00.000Z_2012-01-01T01:00:00.000Z_2012-03-01T00:00:00.000Z", segment1);
    segments.put("datasource1_2012-02-01T00:00:00.000Z_2012-02-01T01:00:00.000Z_2012-03-01T00:00:00.000Z", segment2);
    segments.put("datasource2_2012-01-01T00:00:00.000Z_2012-01-01T01:00:00.000Z_2012-03-01T00:00:00.000Z", segment3);
    segments.put("datasource2_2012-02-01T00:00:00.000Z_2012-02-01T01:00:00.000Z_2012-03-01T00:00:00.000Z", segment4);
  }

  @After
  public void tearDown() throws Exception
  {
    EasyMock.verify(coordinator);
    EasyMock.verify(druidServer1);
    EasyMock.verify(druidServer2);
    EasyMock.verify(druidServer3);
    EasyMock.verify(druidServer4);
  }


  @Test
  public void testMoveToEmptyServerBalancer() throws IOException
  {
    mockDruidServer(druidServer1, "from", "normal", 30L, 100L, segments);
    mockDruidServer(druidServer2, "to", "normal", 0L, 100L, new HashMap<>());

    EasyMock.replay(druidServer3);
    EasyMock.replay(druidServer4);


    LoadQueuePeonTester fromPeon = new LoadQueuePeonTester();
    LoadQueuePeonTester toPeon = new LoadQueuePeonTester();

    ListeningExecutorService exec = MoreExecutors.listeningDecorator(
        Execs.singleThreaded("balancer-strategy-exec"));
    BalancerStrategy balancerStrategy =
        new CostBalancerStrategyFactory().createBalancerStrategy(exec);

    DruidCoordinatorRuntimeParams params =
        DruidCoordinatorRuntimeParams
            .newBuilder()
            .withDruidCluster(
                new DruidCluster(
                    ImmutableMap.<String, MinMaxPriorityQueue<ServerHolder>>of(
                        "normal",
                        MinMaxPriorityQueue
                            .orderedBy(DruidCoordinatorBalancerTester.percentUsedComparator)
                            .create(
                                Arrays.asList(
                                    new ServerHolder(druidServer1, fromPeon),
                                    new ServerHolder(druidServer2, toPeon)
                                )
                            )
                    )
                )
            )
            .withLoadManagementPeons(
                ImmutableMap.<String, LoadQueuePeon>of(
                    "from", fromPeon,
                    "to", toPeon
                )
            )
            .withAvailableSegments(segments.values())
            .withDynamicConfigs(
                new CoordinatorDynamicConfig.Builder().withMaxSegmentsToMove(
                    MAX_SEGMENTS_TO_MOVE
                ).build()
            )
            .withBalancerStrategy(balancerStrategy)
            .withBalancerReferenceTimestamp(new DateTime("2013-01-01"))
            .build();

    params = new DruidCoordinatorBalancerTester(coordinator).run(params);
    Assert.assertTrue(params.getCoordinatorStats().getTieredStat("movedCount", "normal") > 0);
    Assert.assertTrue(params.getCoordinatorStats().getTieredStat("movedCount", "normal") < segments.size());
    exec.shutdown();
  }


  @Test
  public void testRun1() throws IOException
  {
    // Mock some servers of different usages

    mockDruidServer(druidServer1, "from", "normal", 30L, 100L, segments);
    mockDruidServer(druidServer2, "to", "normal", 0L, 100L, new HashMap<>());

    EasyMock.replay(druidServer3);
    EasyMock.replay(druidServer4);

    ListeningExecutorService exec = MoreExecutors.listeningDecorator(
        Execs.singleThreaded("balancer-strategy-exec"));
    BalancerStrategy balancerStrategy =
        new CostBalancerStrategyFactory().createBalancerStrategy(exec);

    LoadQueuePeonTester fromPeon = new LoadQueuePeonTester();
    LoadQueuePeonTester toPeon = new LoadQueuePeonTester();
    DruidCoordinatorRuntimeParams params =
        DruidCoordinatorRuntimeParams
            .newBuilder()
            .withDruidCluster(
                new DruidCluster(
                    ImmutableMap.<String, MinMaxPriorityQueue<ServerHolder>>of(
                        "normal",
                        MinMaxPriorityQueue
                            .orderedBy(DruidCoordinatorBalancerTester.percentUsedComparator)
                            .create(
                                Arrays.asList(
                                    new ServerHolder(druidServer1, fromPeon),
                                    new ServerHolder(druidServer2, toPeon)
                                )
                            )
                    )
                )
            )
            .withLoadManagementPeons(
                ImmutableMap.<String, LoadQueuePeon>of(
                    "from", fromPeon,
                    "to", toPeon
                )
            )
            .withAvailableSegments(segments.values())
            .withDynamicConfigs(
                new CoordinatorDynamicConfig.Builder().withMaxSegmentsToMove(
                    MAX_SEGMENTS_TO_MOVE)
                                                      .build()
            )
            .withBalancerStrategy(balancerStrategy)
            .withBalancerReferenceTimestamp(new DateTime("2013-01-01"))
            .build();

    params = new DruidCoordinatorBalancerTester(coordinator).run(params);
    Assert.assertTrue(params.getCoordinatorStats().getTieredStat("movedCount", "normal") > 0);
    exec.shutdown();
  }


  @Test
  public void testRun2() throws IOException
  {
    // Mock some servers of different usages
    mockDruidServer(druidServer1, "1", "normal", 30L, 100L, segments);
    mockDruidServer(druidServer2, "2", "normal", 0L, 100L, new HashMap<>());
    mockDruidServer(druidServer3, "3", "normal", 0L, 100L, new HashMap<>());
    mockDruidServer(druidServer4, "4", "normal", 0L, 100L, new HashMap<>());

    LoadQueuePeonTester peon1 = new LoadQueuePeonTester();
    LoadQueuePeonTester peon2 = new LoadQueuePeonTester();
    LoadQueuePeonTester peon3 = new LoadQueuePeonTester();
    LoadQueuePeonTester peon4 = new LoadQueuePeonTester();

    ListeningExecutorService exec = MoreExecutors.listeningDecorator(
        Execs.singleThreaded("balancer-strategy-exec"));
    BalancerStrategy balancerStrategy =
        new CostBalancerStrategyFactory().createBalancerStrategy(exec);

    DruidCoordinatorRuntimeParams params =
        DruidCoordinatorRuntimeParams
            .newBuilder()
            .withDruidCluster(
                new DruidCluster(
                    ImmutableMap.<String, MinMaxPriorityQueue<ServerHolder>>of(
                        "normal",
                        MinMaxPriorityQueue
                            .orderedBy(DruidCoordinatorBalancerTester.percentUsedComparator)
                            .create(
                                Arrays.asList(
                                    new ServerHolder(druidServer1, peon1),
                                    new ServerHolder(druidServer2, peon2),
                                    new ServerHolder(druidServer3, peon3),
                                    new ServerHolder(druidServer4, peon4)
                                )
                            )
                    )
                )
            )
            .withLoadManagementPeons(
                ImmutableMap.<String, LoadQueuePeon>of(
                    "1", peon1,
                    "2", peon2,
                    "3", peon3,
                    "4", peon4
                )
            )
            .withAvailableSegments(segments.values())
            .withDynamicConfigs(
                new CoordinatorDynamicConfig.Builder().withMaxSegmentsToMove(
                    MAX_SEGMENTS_TO_MOVE
                ).build()
            )
            .withBalancerStrategy(balancerStrategy)
            .withBalancerReferenceTimestamp(new DateTime("2013-01-01"))
            .build();

    params = new DruidCoordinatorBalancerTester(coordinator).run(params);
    Assert.assertTrue(params.getCoordinatorStats().getTieredStat("movedCount", "normal") > 0);
    exec.shutdown();
  }

  @Test
  public void testMaxSegmentsToMovePerTier() throws IOException
  {
    // Mock some servers of different usages
    mockDruidServer(druidServer1, "1", "normal", 30L, 100L, segments);
    mockDruidServer(druidServer2, "2", "normal", 0L, 100L, new HashMap<>());
    mockDruidServer(druidServer3, "3", "extra", 30L, 100L, segments);
    mockDruidServer(druidServer4, "4", "extra", 0L, 100L, new HashMap<>());

    LoadQueuePeonTester peon1 = new LoadQueuePeonTester();
    LoadQueuePeonTester peon2 = new LoadQueuePeonTester();
    LoadQueuePeonTester peon3 = new LoadQueuePeonTester();
    LoadQueuePeonTester peon4 = new LoadQueuePeonTester();

    ListeningExecutorService exec = MoreExecutors.listeningDecorator(
        Execs.singleThreaded("balancer-strategy-exec"));

    BalancerStrategy balancerStrategy = createBalancerStrategy(exec);

    Map<String, MinMaxPriorityQueue<ServerHolder>> cluster = new HashMap<>();
    cluster.put("normal", MinMaxPriorityQueue
        .orderedBy(DruidCoordinatorBalancerTester.percentUsedComparator)
        .create(
            Arrays.asList(
                new ServerHolder(druidServer1, peon1),
                new ServerHolder(druidServer2, peon2)
            )
        ));

    cluster.put("extra", MinMaxPriorityQueue
        .orderedBy(DruidCoordinatorBalancerTester.percentUsedComparator)
        .create(
            Arrays.asList(
                new ServerHolder(druidServer3, peon3),
                new ServerHolder(druidServer4, peon4)
            )
        ));

    DruidCoordinatorRuntimeParams params =
        DruidCoordinatorRuntimeParams
            .newBuilder()
            .withDruidCluster(
                new DruidCluster(cluster)
            )
            .withLoadManagementPeons(
                ImmutableMap.<String, LoadQueuePeon>of(
                    "1", peon1,
                    "2", peon2,
                    "3", peon3,
                    "4", peon4
                )
            )
            .withAvailableSegments(segments.values())
            .withDynamicConfigs(
                new CoordinatorDynamicConfig.Builder().withMaxSegmentsToMove(
                    1
                ).build()
            )
            .withBalancerStrategy(balancerStrategy)
            .withBalancerReferenceTimestamp(new DateTime("2013-01-01"))
            .build();

    DruidCoordinatorBalancerTester balancerTester = new DruidCoordinatorBalancerTester(coordinator);
    params = balancerTester.run(params);
    Assert.assertEquals(1L, params.getCoordinatorStats().getTieredStat("movedCount", "normal"));
    Assert.assertEquals(1L, params.getCoordinatorStats().getTieredStat("movedCount", "extra"));

    exec.shutdown();
  }

  @Test
  public void testMaxSegmentsToMove() throws IOException
  {
    // Mock some servers of different usages
    mockDruidServer(druidServer1, "1", "normal", 30L, 100L, segments);
    mockDruidServer(druidServer2, "2", "normal", 0L, 100L, new HashMap<>());
    mockDruidServer(druidServer3, "3", "normal", 0L, 100L, new HashMap<>());
    mockDruidServer(druidServer4, "4", "normal", 0L, 100L, new HashMap<>());

    LoadQueuePeonTester peon1 = new LoadQueuePeonTester();
    LoadQueuePeonTester peon2 = new LoadQueuePeonTester();
    LoadQueuePeonTester peon3 = new LoadQueuePeonTester();
    LoadQueuePeonTester peon4 = new LoadQueuePeonTester();

    ListeningExecutorService exec = MoreExecutors.listeningDecorator(
        Execs.singleThreaded("balancer-strategy-exec"));
    BalancerStrategy balancerStrategy = createBalancerStrategy(exec);

    Map<String, MinMaxPriorityQueue<ServerHolder>> cluster = new HashMap<>();
    cluster.put("normal", MinMaxPriorityQueue
        .orderedBy(DruidCoordinatorBalancerTester.percentUsedComparator)
        .create(
            Arrays.asList(
                new ServerHolder(druidServer1, peon1),
                new ServerHolder(druidServer2, peon2),
                new ServerHolder(druidServer3, peon3),
                new ServerHolder(druidServer4, peon4)
            )
        ));
    DruidCoordinatorRuntimeParams params =
        DruidCoordinatorRuntimeParams
            .newBuilder()
            .withDruidCluster(
                new DruidCluster(cluster)
            )
            .withLoadManagementPeons(
                ImmutableMap.<String, LoadQueuePeon>of(
                    "1", peon1,
                    "2", peon2,
                    "3", peon3,
                    "4", peon4
                )
            )
            .withAvailableSegments(segments.values())
            .withDynamicConfigs(
                new CoordinatorDynamicConfig.Builder().withMaxSegmentsToMove(
                    1
                ).build()
            )
            .withBalancerStrategy(balancerStrategy)
            .withBalancerReferenceTimestamp(new DateTime("2013-01-01"))
            .build();

    DruidCoordinatorBalancerTester balancerTester = new DruidCoordinatorBalancerTester(coordinator);
    params = balancerTester.run(params);
    Assert.assertEquals(1L, params.getCoordinatorStats().getTieredStat("movedCount", "normal"));

    params = params.buildFromExisting()
                   .withDynamicConfigs(
                       new CoordinatorDynamicConfig.Builder().withMaxSegmentsToMove(
                           2
                       ).build()
                   )
                   .build();

    params = balancerTester.run(params);
    Assert.assertEquals(2L, params.getCoordinatorStats().getTieredStat("movedCount", "normal"));
    exec.shutdown();
  }

  private void mockDruidServer(
      ImmutableDruidServer druidServer,
      String name,
      String tier,
      long currSize,
      long maxSize,
      Map<String, DataSegment> segments
  )
  {
    EasyMock.expect(druidServer.getName()).andReturn(name).atLeastOnce();
    EasyMock.expect(druidServer.getTier()).andReturn(tier).anyTimes();
    EasyMock.expect(druidServer.getCurrSize()).andReturn(currSize).atLeastOnce();
    EasyMock.expect(druidServer.getMaxSize()).andReturn(maxSize).atLeastOnce();
    EasyMock.expect(druidServer.getSegments()).andReturn(segments).anyTimes();
    EasyMock.expect(druidServer.getSegment(EasyMock.<String>anyObject())).andReturn(null).anyTimes();
    EasyMock.replay(druidServer);
  }

  private BalancerStrategy createBalancerStrategy(ListeningExecutorService exec)
  {
    final BalancerStrategy costBalancerStrategy =
        new CostBalancerStrategyFactory().createBalancerStrategy(exec);

    final AtomicInteger segmentIndex = new AtomicInteger(0);
    final List<DataSegment> segmentList = Arrays.asList(segment1, segment2, segment3, segment4);

    BalancerStrategy balancerStrategy = new BalancerStrategy()
    {
      @Override
      public ServerHolder findNewSegmentHomeBalancer(
          DataSegment proposalSegment, List<ServerHolder> serverHolders
      )
      {
        return costBalancerStrategy.findNewSegmentHomeBalancer(proposalSegment, serverHolders);
      }

      @Override
      public ServerHolder findNewSegmentHomeReplicator(
          DataSegment proposalSegment, List<ServerHolder> serverHolders
      )
      {
        return costBalancerStrategy.findNewSegmentHomeReplicator(proposalSegment, serverHolders);
      }

      @Override
      public BalancerSegmentHolder pickSegmentToMove(List<ServerHolder> serverHolders)
      {
        DataSegment segmentToMove = segmentList.get(segmentIndex.getAndIncrement());
        for (ServerHolder serverHolder : serverHolders) {
          if (serverHolder.getServer().getSegments().containsKey(segmentToMove.getIdentifier())) {
            return new BalancerSegmentHolder(serverHolder.getServer(), segmentToMove);
          }
        }
        return null;
      }

      @Override
      public void emitStats(
          String tier, CoordinatorStats stats, List<ServerHolder> serverHolderList
      )
      {

      }
    };
    return balancerStrategy;
  }

}
