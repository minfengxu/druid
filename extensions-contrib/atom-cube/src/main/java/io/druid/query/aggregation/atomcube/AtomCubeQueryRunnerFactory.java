package io.druid.query.aggregation.atomcube;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import io.druid.client.cache.Cache;
import io.druid.query.*;
import io.druid.segment.Segment;
import io.druid.server.initialization.ServerConfig;

import javax.annotation.Nullable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by minfengxu on 2016/5/30 0030.
 */
public class AtomCubeQueryRunnerFactory implements QueryRunnerFactory<Result<AtomCubeResultValue>, AtomCubeQuery> {

  private final AtomCubeQueryQueryToolChest toolChest;
  private final QueryWatcher queryWatcher;
  private final QuerySegmentWalker texasRanger;
  private final ServerConfig config;
  @Inject
  private Cache cache;
  @Inject
  private ObjectMapper objectMapper;
  private final static ListeningExecutorService exec = MoreExecutors.listeningDecorator(Executors.newWorkStealingPool(1000));

  @Inject
  public AtomCubeQueryRunnerFactory(
    AtomCubeQueryQueryToolChest toolChest,
    QueryWatcher queryWatcher,
    ServerConfig config,
    @Nullable QuerySegmentWalker texasRanger
  ) {
    this.toolChest = toolChest;
    this.queryWatcher = queryWatcher;
    this.texasRanger = texasRanger;
    this.config = config;
  }

  @Override
  public QueryRunner<Result<AtomCubeResultValue>> createRunner(Segment segment) {
    return new AtomCubeQueryRunner(config, texasRanger, queryWatcher, exec, cache, objectMapper, toolChest);
  }

  @Override
  public QueryRunner<Result<AtomCubeResultValue>> mergeRunners(ExecutorService queryExecutor, Iterable<QueryRunner<Result<AtomCubeResultValue>>> queryRunners) {
    return new ChainedExecutionQueryRunner<Result<AtomCubeResultValue>>(queryExecutor, queryWatcher, queryRunners);
  }

  @Override
  public QueryToolChest<Result<AtomCubeResultValue>, AtomCubeQuery> getToolchest() {
    return toolChest;
  }
}
