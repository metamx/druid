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

package io.druid.grpc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.RE;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.java.util.common.io.Closer;
import io.druid.query.Query;
import io.druid.query.QueryPlus;
import io.druid.query.QueryRunner;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.Result;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.topn.TopNQuery;
import io.druid.query.topn.TopNQueryBuilder;
import io.druid.query.topn.TopNResultValue;
import io.druid.server.QueryManager;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.KnownLength;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.ClientCalls;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.Future;

public class QueryServerTest
{
  private static final ObjectMapper MAPPER = new DefaultObjectMapper();

  @Test
  public void TestBAIS()
  {
    final byte[] buff = new byte[]{1, 2, 3, 4};
    final QueryServer.ByteArrayInputStream bais = new QueryServer.ByteArrayInputStream(buff);
    Assert.assertEquals(buff.length, bais.available());
    Assert.assertEquals(buff[0], bais.read());
    Assert.assertEquals(buff.length - 1, bais.available());
    Assert.assertTrue(bais instanceof KnownLength);
  }

  @Test
  public void testQueryMarshall()
  {
    final QueryServer qs = new QueryServer(MAPPER, null, null);
    final TopNQuery topNQuery = new TopNQueryBuilder()
        .dataSource("Test datasource")
        .intervals("2017-01-01/2018-01-01")
        .dimension("some dimension")
        .metric("some metric")
        .threshold(1)
        .aggregators(ImmutableList.of(new CountAggregatorFactory("some metric")))
        .build();
    Assert.assertEquals(topNQuery, qs.queryImpl.QUERY_MARSHALL.parse(qs.queryImpl.QUERY_MARSHALL.stream(topNQuery)));
  }

  @Test
  public void testResultMarshall()
  {
    final QueryServer qs = new QueryServer(MAPPER, null, null);
    final Result result = new Result(DateTime.now(), ImmutableList.of(ImmutableMap.of("dim", 1)));
    Assert.assertEquals(result, qs.queryImpl.RESULT_MARSHALL.parse(qs.queryImpl.RESULT_MARSHALL.stream(result)));
  }

  @Test
  public void testCancel()
  {
    final String id = "some id";
    final QueryManager qm = EasyMock.createStrictMock(QueryManager.class);
    EasyMock.expect(qm.cancelQuery(EasyMock.eq(id))).andReturn(true).once();
    EasyMock.replay(qm);
    final QueryServer qs = new QueryServer(MAPPER, qm, null);
    final ServerCall<Query, Result> call = EasyMock.createStrictMock(ServerCall.class);
    final Metadata metadata = new Metadata();
    metadata.put(qs.queryImpl.QUERY_ID_KEY, id);
    final ServerCall.Listener<Query> listener = qs.queryImpl.SERVICE_CALL_HANDLER.startCall(call, metadata);
    listener.onCancel();
    EasyMock.verify(qm);
  }

  @Test
  public void testQuery()
  {
    final Result<TopNResultValue> result = new Result<TopNResultValue>(
        DateTime.now(),
        new TopNResultValue(ImmutableList.of(ImmutableMap.of(
            "dim",
            1
        )))
    );
    final TopNQuery query = new TopNQueryBuilder()
        .dataSource("Test datasource")
        .intervals("2017-01-01/2018-01-01")
        .dimension("some dimension")
        .metric("some metric")
        .threshold(1)
        .aggregators(ImmutableList.of(new CountAggregatorFactory("some metric")))
        .build();
    final String id = "some id";
    final QuerySegmentWalker walker = EasyMock.createStrictMock(QuerySegmentWalker.class);
    EasyMock.expect(walker.getQueryRunnerForIntervals(query, query.getIntervals()))
            .andReturn(new QueryRunner<Result<TopNResultValue>>()
            {
              public Sequence<Result<TopNResultValue>> run(
                  QueryPlus<Result<TopNResultValue>> queryPlus,
                  Map<String, Object> responseContext
              )
              {
                return Sequences.simple(ImmutableList.of(result));
              }
            })
            .once();
    final QueryServer qs = new QueryServer(MAPPER, null, walker);
    final ServerCall<Query, Result> call = EasyMock.createNiceMock(ServerCall.class);
    call.request(EasyMock.eq(1));
    EasyMock.expectLastCall().once();
    EasyMock.expect(call.isCancelled()).andReturn(false).once();
    call.sendMessage(EasyMock.eq(result));
    EasyMock.expectLastCall().once();
    final Metadata metadata = new Metadata();
    metadata.put(qs.queryImpl.QUERY_ID_KEY, id);

    EasyMock.replay(walker, call);
    final ServerCall.Listener<Query> listener = qs.queryImpl.SERVICE_CALL_HANDLER.startCall(call, metadata);
    listener.onMessage(query);
    EasyMock.verify(walker, call);
  }

  @Test
  public void testStartStop()
  {
    final QueryServer qs = new QueryServer(MAPPER, null, null);
    qs.start();
    qs.stop();
  }

  @Test
  public void testStartStopStop()
  {
    final QueryServer qs = new QueryServer(MAPPER, null, null);
    qs.start();
    qs.stop();
    qs.stop();
  }

  @Test
  public void testStartStopStartStop()
  {
    final QueryServer qs = new QueryServer(MAPPER, null, null);
    qs.start();
    qs.stop();
    qs.start();
    qs.stop();
  }

  @Test(expected = ISE.class)
  public void testStartStart()
  {
    final QueryServer qs = new QueryServer(MAPPER, null, null);
    qs.start();
    try {
      qs.start();
    }
    finally {
      qs.stop();
    }
  }

  @Test
  public void testServiceIT() throws Exception
  {
    final Result<TopNResultValue> result = new Result<TopNResultValue>(
        DateTime.now(),
        new TopNResultValue(ImmutableList.of(ImmutableMap.of(
            "dim",
            1
        )))
    );
    final TopNQuery query = new TopNQueryBuilder()
        .dataSource("Test datasource")
        .intervals("2017-01-01/2018-01-01")
        .dimension("some dimension")
        .metric("some metric")
        .threshold(1)
        .aggregators(ImmutableList.of(new CountAggregatorFactory("some metric")))
        .build();
    final String id = "some id";
    final QuerySegmentWalker walker = EasyMock.createStrictMock(QuerySegmentWalker.class);
    EasyMock.expect(walker.getQueryRunnerForIntervals(query, query.getIntervals()))
            .andReturn(new QueryRunner<Result<TopNResultValue>>()
            {
              public Sequence<Result<TopNResultValue>> run(
                  QueryPlus<Result<TopNResultValue>> queryPlus,
                  Map<String, Object> responseContext
              )
              {
                return Sequences.simple(ImmutableList.of(result));
              }
            })
            .once();
    final QueryServer qs = new QueryServer(MAPPER, null, walker);
    final Metadata metadata = new Metadata();
    metadata.put(qs.queryImpl.QUERY_ID_KEY, id);

    final Result expected = MAPPER.readValue(MAPPER.writeValueAsBytes(result), Result.class);

    EasyMock.replay(walker);

    qs.start();
    try (final Closer closer = Closer.create()) {
      closer.register(qs::stop);
      final ManagedChannel channel = ManagedChannelBuilder
          .forAddress("localhost", 9987)
          .usePlaintext(true)
          .build();
      closer.register(channel::shutdownNow);
      final QueryStub queryStub = new QueryStub(channel, qs);
      Assert.assertEquals(expected, queryStub.issueQuery(query).get());
    }
    catch (Exception e) {
      throw new RE(e, "test failed");
    }
    EasyMock.verify(walker);
  }

  final class QueryStub extends AbstractStub<QueryStub>
  {
    private final QueryServer qs;

    protected QueryStub(Channel channel, QueryServer qs)
    {
      super(channel);
      this.qs = qs;
    }

    protected QueryStub(Channel channel, CallOptions callOptions, QueryServer qs)
    {
      super(channel, callOptions);
      this.qs = qs;
    }

    @Override
    protected QueryStub build(Channel channel, CallOptions callOptions)
    {
      return new QueryStub(channel, callOptions, qs);
    }

    Future<Result> issueQuery(Query q)
    {
      return ClientCalls.futureUnaryCall(getChannel().newCall(qs.queryImpl.methodDescriptor, getCallOptions()), q);
    }
  }
}
