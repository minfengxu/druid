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

package io.druid.query.aggregation.atomcube;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import io.druid.client.cache.Cache;
import io.druid.query.ChainedExecutionQueryRunner;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryWatcher;
import io.druid.query.Result;
import io.druid.segment.Segment;
import io.druid.server.initialization.ServerConfig;

import javax.annotation.Nullable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class AtomCubeQueryRunnerFactory implements QueryRunnerFactory<Result<AtomCubeResultValue>, AtomCubeQuery>
{

  private final AtomCubeQueryQueryToolChest toolChest;
  private final QueryWatcher queryWatcher;
  private final QuerySegmentWalker texasRanger;
  private final ServerConfig config;
  @Inject
  private Cache cache;
  @Inject
  private ObjectMapper objectMapper;

  //use java 8 api, but main build disabled.
//  private final static ListeningExecutorService exec = MoreExecutors.listeningDecorator(Executors.newWorkStealingPool(
//      1000));

  private final static ListeningExecutorService exec = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(
      1000));
  @Inject
  public AtomCubeQueryRunnerFactory(
      AtomCubeQueryQueryToolChest toolChest,
      QueryWatcher queryWatcher,
      ServerConfig config,
      @Nullable QuerySegmentWalker texasRanger
  )
  {
    this.toolChest = toolChest;
    this.queryWatcher = queryWatcher;
    this.texasRanger = texasRanger;
    this.config = config;
  }

  @Override
  public QueryRunner<Result<AtomCubeResultValue>> createRunner(Segment segment)
  {
    return new AtomCubeQueryRunner(config, texasRanger, queryWatcher, exec, cache, objectMapper, toolChest);
  }

  @Override
  public QueryRunner<Result<AtomCubeResultValue>> mergeRunners(
      ExecutorService queryExecutor,
      Iterable<QueryRunner<Result<AtomCubeResultValue>>> queryRunners
  )
  {
    return new ChainedExecutionQueryRunner<Result<AtomCubeResultValue>>(queryExecutor, queryWatcher, queryRunners);
  }

  @Override
  public QueryToolChest<Result<AtomCubeResultValue>, AtomCubeQuery> getToolchest()
  {
    return toolChest;
  }
}
