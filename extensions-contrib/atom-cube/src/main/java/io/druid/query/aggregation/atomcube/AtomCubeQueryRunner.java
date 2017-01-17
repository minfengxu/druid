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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.metamx.common.ISE;
import com.metamx.common.Pair;
import com.metamx.emitter.EmittingLogger;
import io.druid.client.CacheUtil;
import io.druid.client.cache.Cache;
import io.druid.collections.bitmap.ImmutableBitmap;
import io.druid.collections.bitmap.WrappedImmutableRoaringBitmap;
import io.druid.data.input.MapBasedRow;
import io.druid.java.util.common.guava.BaseSequence;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.java.util.common.guava.Yielder;
import io.druid.java.util.common.guava.YieldingAccumulator;
import io.druid.query.AbstractPrioritizedCallable;
import io.druid.query.BaseQuery;
import io.druid.query.CacheStrategy;
import io.druid.query.Query;
import io.druid.query.QueryContextKeys;
import io.druid.query.QueryInterruptedException;
import io.druid.query.QueryRunner;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryWatcher;
import io.druid.query.Result;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.timeseries.TimeseriesResultValue;
import io.druid.query.topn.DimensionAndMetricValueExtractor;
import io.druid.query.topn.TopNQuery;
import io.druid.query.topn.TopNResultValue;
import io.druid.server.initialization.ServerConfig;
import org.joda.time.DateTime;
import org.roaringbitmap.RoaringBitmap;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class AtomCubeQueryRunner implements QueryRunner
{

  private static final EmittingLogger log = new EmittingLogger(AtomCubeQueryRunner.class);

  private final QuerySegmentWalker texasRanger;
  private final ServerConfig config;
  private final QueryWatcher queryWatcher;
  private final ExecutorService executorService;
  private final Cache cache;
  private final ObjectMapper mapper;
  private final AtomCubeQueryQueryToolChest toolChest;

  private static final Map<String, FetchBitmapFunc<Query, Yielder, ImmutableBitmap>> mm = new HashMap<>();

  static {
    mm.put(Query.TOPN, new FetchBitmapFunc<Query, Yielder, ImmutableBitmap>()
    {
      @Override
      public ImmutableBitmap apply(Query query, Yielder yielder)
      {
        ImmutableBitmap ret = null;
        String metricName = getAtomCubeMetricName(query);
        if (metricName != null && yielder != null) {
          ret = goThroughYielder(yielder, metricName, new Fetcher<String, Object, ImmutableBitmap>()
          {
            @Override
            public ImmutableBitmap apply(String metricName, Object input)
            {
              ImmutableBitmap ret = null;
              if (null != input && input instanceof Result) {
                Result result = (Result) input;
                DateTime timeStamp = result.getTimestamp();
                Object value = result.getValue();
                if (value instanceof TopNResultValue) {
                  TopNResultValue resultValue = (TopNResultValue) value;
                  List<DimensionAndMetricValueExtractor> values = resultValue.getValue();
                  for (DimensionAndMetricValueExtractor valueExtractor : values) {
                    ret = ret == null ? toImmutable(valueExtractor.getMetric(metricName)) :
                          ret.union(toImmutable(valueExtractor.getMetric(metricName)));
                  }
                }
              }
              return ret;
            }
          });
        }
        return ret;
      }
    });

    mm.put(Query.TIMESERIES, new FetchBitmapFunc<Query, Yielder, ImmutableBitmap>()
    {
      @Override
      public ImmutableBitmap apply(Query query, Yielder yielder)
      {
        ImmutableBitmap ret = null;
        String metricName = getAtomCubeMetricName(query);
        if (metricName != null && yielder != null) {
          ret = goThroughYielder(yielder, metricName, new Fetcher<String, Object, ImmutableBitmap>()
          {
            @Override
            public ImmutableBitmap apply(String metricName, Object input)
            {
              Object ret = null;
              if (null != input && input instanceof Result) {
                Result result = (Result) input;
                DateTime timeStamp = result.getTimestamp();
                Object value = result.getValue();
                if (value instanceof TimeseriesResultValue) {
                  TimeseriesResultValue resultValue = (TimeseriesResultValue) value;
                  ret = resultValue.getMetric(metricName);
                }
              }
              return toImmutable(ret);
            }
          });
        }
        return ret;
      }
    });

    mm.put(Query.GROUP_BY, new FetchBitmapFunc<Query, Yielder, ImmutableBitmap>()
    {
      @Override
      public ImmutableBitmap apply(Query query, Yielder yielder)
      {
        ImmutableBitmap ret = null;
        String metricName = getAtomCubeMetricName(query);
        if (metricName != null && yielder != null) {
          ret = goThroughYielder(yielder, metricName, new Fetcher<String, Object, ImmutableBitmap>()
          {
            @Override
            public ImmutableBitmap apply(String metricName, Object input)
            {
              Object ret = null;
              if (null != input && input instanceof MapBasedRow) {
                MapBasedRow result = (MapBasedRow) input;
                DateTime timeStamp = result.getTimestamp();
                Map<String, Object> value = result.getEvent();
                ret = value.get(metricName);
              }
              return toImmutable(ret);
            }
          });
        }
        return ret;
      }
    });
  }

  public AtomCubeQueryRunner(
      ServerConfig config,
      QuerySegmentWalker texasRanger,
      QueryWatcher queryWatcher,
      ExecutorService exec,
      Cache cache,
      ObjectMapper objectMapper,
      AtomCubeQueryQueryToolChest toolChest
  )
  {
    this.config = config;
    this.texasRanger = texasRanger;
    this.queryWatcher = queryWatcher;
    this.executorService = exec;
    this.cache = cache;
    this.done = false;
    this.mapper = objectMapper;
    this.toolChest = toolChest;
  }

  final BlockingDeque<Pair<String, ImmutableBitmap>> queue = new LinkedBlockingDeque();
  boolean done = false;

  final Map<String, StringBuilder> queryStack = Maps.newConcurrentMap();
  //  final StringBuffer debugsb1 = new StringBuffer();
  final StringBuffer debugsb2 = new StringBuffer();

  private boolean satisfyPostAgg(Map results, PostAggregator postagg)
  {
    boolean ret = false;
    List<String> fields = Lists.newArrayList();
    String field = null;
    if (postagg instanceof AtomCubeSetPostAggregator) {
      fields = ((AtomCubeSetPostAggregator) postagg).getFields();
    } else if (postagg instanceof AtomCubeSizePostAggregator) {
      field = ((AtomCubeSizePostAggregator) postagg).getField();
    } else if (postagg instanceof AtomCubeRawPostAggregator) {
      field = ((AtomCubeRawPostAggregator) postagg).getField();
    }
    if (field != null) {
      if (results.get(field) != null) {
        debugsb2.append("-- check postagg:").append(postagg.getName()).append(" passed\n");
        ret = true;
      }
    } else if (!fields.isEmpty()) {
      int i = 0;
      for (String s : fields) {
        if (results.get(s) != null) {
          i++;
        }
      }
      if (i == fields.size()) {
        debugsb2.append("-- check postagg:").append(postagg.getName()).append(" passed\n");
        ret = true;
      }
    }
    return ret;
  }

  private void doCompute(Map results, List<PostAggregator> postaggs)
  {
    if (!postaggs.isEmpty()) {
      List<PostAggregator> indexes = Lists.newArrayList();
      for (PostAggregator postagg : postaggs) {
        if (satisfyPostAgg(results, postagg)) {
          Object o = postagg.compute(results);
          results.put(postagg.getName(), o);
          indexes.add(postagg);
          debugsb2.append("-- compute postagg:").append(postagg.getName()).append(" done\n");
        }
      }
      if (!indexes.isEmpty()) {
        for (PostAggregator postagg : indexes) {
          postaggs.remove(postagg);
          debugsb2.append("-- postagg:").append(postagg.getName()).append(" removed\n");
        }
      }
    }
  }

  private Map<String, Object> computeSet(AtomCubeQuery atomQ) throws InterruptedException
  {
    List<PostAggregator> postaggs = atomQ.getPostAggregations() == null ? null :
                                    Lists.newCopyOnWriteArrayList(atomQ.getPostAggregations());
    final Map<String, Object> results = new HashMap<String, Object>();
    if (postaggs != null) {

      while (!done || !queue.isEmpty()) {
        Pair<String, ImmutableBitmap> pair = queue.poll(100, TimeUnit.MILLISECONDS);
        if (pair != null) {
          results.put(pair.lhs, pair.rhs);
          debugsb2.append("-- put ").append(pair.lhs).append(" to results, and doCompute\n");
          doCompute(results, postaggs);
        }
      }
      doCompute(results, postaggs);
      if (!postaggs.isEmpty()) {
        log.warn("AtomCube Query's postaggregators did not compute complete.");
        log.warn("results contains:");
        for (String key : results.keySet()) {
          log.warn("\t" + key);
        }
        log.warn("PostAggregators didn't compute:");
        for (PostAggregator postagg : postaggs) {
          log.warn("postagg name:" + postagg.getName());
          if (postagg instanceof AtomCubeSetPostAggregator) {
            for (String field : ((AtomCubeSetPostAggregator) postagg).getFields()) {
              log.warn("\t field:" + field);
            }
          } else if (postagg instanceof AtomCubeSizePostAggregator) {
            log.warn("\t field:" + ((AtomCubeSizePostAggregator) postagg).getField());
          } else if (postagg instanceof AtomCubeRawPostAggregator) {
            log.warn("\t field:" + ((AtomCubeRawPostAggregator) postagg).getField());
          }
        }
        log.warn(debugsb2.toString());
      }
    } else {
      log.warn("AtomCube Query has not been assigned postaggregators.");
    }
    log.debug("compute ... done");
    return results;
  }

  private String getQueryName(final Map<String, Query> queries, final Query query)
  {
    String ret = null;
    for (Map.Entry<String, Query> entry : queries.entrySet()) {
      if (entry.getValue() == query) {
        ret = entry.getKey();
        break;
      }
    }
    return ret;
  }

  private Sequence fetchResultFromCache(final CacheStrategy strategy, final byte[] value)
  {
    final Function cacheFn = strategy.pullFromCache();
    final TypeReference cacheObjectClazz = strategy.getCacheObjectClazz();
    try {
      final MappingIterator iterator = mapper.readValues(
          mapper.getFactory().createParser(value),
          cacheObjectClazz
      );
      Object o = iterator.next();
      Result<AtomCubeResultValue> result = (Result<AtomCubeResultValue>) cacheFn.apply(o);
      if (result != null) {
        return BaseSequence.simple(result.getValue());
      } else {
        log.debug("cache expired!!!");
        return null;
      }
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  private ListenableFuture<List<ImmutableBitmap>> prepareFutures(final AtomCubeQuery atomQ)
  {
    final Map<String, Query> queries = atomQ.getQueries();
    Iterable<Query> queryIterables =
        Iterables.unmodifiableIterable(Iterables.filter(queries.values(), Predicates.notNull()));
    return Futures.allAsList(
        Iterables.transform(
            queryIterables,
            new Function<Query, ListenableFuture<ImmutableBitmap>>()
            {

              @Nullable
              @Override
              public ListenableFuture<ImmutableBitmap> apply(@Nullable final Query input)
              {
                if (input == null) {
                  throw new ISE("Null query!??");
                }
                final String queryName = getQueryName(queries, input);
                final StringBuilder debugsb = new StringBuilder();
                debugsb.append("** add query to list, query:").append(queryName).append("\n");
                final Query query = input;
                int priority = BaseQuery.getContextPriority(query, 0);
                return ((ListeningExecutorService) executorService).submit(
                    new AbstractPrioritizedCallable<ImmutableBitmap>(priority)
                    {
                      @Override
                      public ImmutableBitmap call()
                      {
                        debugsb.append("** running query:").append(queryName).append("\n");
                        long begin = System.currentTimeMillis();
                        Yielder yielder = null;
                        try {
                          yielder = Query(query, debugsb);
                        }
                        catch (IOException e) {
                          e.printStackTrace();
                          log.error(e.getMessage());
                          debugsb.append("** subquery failed:").append(e.getMessage()).append("\n");
                        }
                        catch (Throwable throwable) {
                          debugsb.append("** * subquery failed:").append(throwable.getMessage()).append("\n");
                        }
                        debugsb.append("** query done:")
                               .append(queryName)
                               .append(" time:")
                               .append(System.currentTimeMillis() - begin)
                               .append("\n");
                        log.debug("--- inner query use " + (System.currentTimeMillis() - begin) + "millis");
                        ImmutableBitmap immutableBitmap = mm.get(query.getType()).apply(query, yielder);
                        if (immutableBitmap == null) {
                          immutableBitmap = AtomCubeAggregatorFactory.BITMAP_FACTORY.makeEmptyImmutableBitmap();
                        }
                        debugsb.append("** done query:")
                               .append(queryName)
                               .append(" time:")
                               .append(System.currentTimeMillis() - begin)
                               .append("\n");
                        queue.offer(Pair.of(queryName, immutableBitmap));
                        queryStack.put(queryName, debugsb);
                        if (immutableBitmap.isEmpty()) {
                          log.warn("returned bitmap is empty:\n" + debugsb.toString());
                        }
                        return immutableBitmap;
                      }
                    }
                );
              }
            }
        )
    );
  }

  private boolean subQuerySucceed(
      final ListenableFuture<List<ImmutableBitmap>> futures,
      final AtomCubeQuery atomQ
  )
  {
    boolean ret = false;
    final Number timeout = atomQ.getContextValue(QueryContextKeys.TIMEOUT, (Number) null);
    List<ImmutableBitmap> bitmaps = null;
    try {
      if (timeout == null) {
        bitmaps = futures.get();
      } else {
        bitmaps = futures.get(timeout.longValue(), TimeUnit.MILLISECONDS);
      }
    }
    catch (ExecutionException e) {
      throw Throwables.propagate(e.getCause());
    }
    catch (InterruptedException e) {
      log.warn(e, "Query interrupted, cancelling pending results, query id [%s]", atomQ.getId());
      futures.cancel(true);
      throw new QueryInterruptedException(e);
    }
    catch (TimeoutException e) {
      log.info("Query timeout, cancelling pending results for query id [%s]", atomQ.getId());
      futures.cancel(true);
      throw new QueryInterruptedException(e);
    }
    finally {
      if (bitmaps != null && atomQ.getQueries().size() == bitmaps.size()) {
        log.debug("sub queries done!");
        ret = true;
      } else {
        log.error("not all sub queries response correctly, but set \"done\" be true to let the progress continue!!!");
        logQueryStack();
      }
      queryStack.clear();
    }
    return ret;
  }

  private void logQueryStack()
  {
    for (Map.Entry<String, StringBuilder> entry : queryStack.entrySet()) {
      log.error(entry.getKey());
      log.error(entry.getValue().toString());
    }
  }

  @Override
  public Sequence run(Query _query, Map responseContext)
  {
    if (!(_query instanceof AtomCubeQuery)) {
      throw new ISE("Got a [%s] which isn't a %s", _query.getClass(), AtomCubeQuery.class);
    }

    final AtomCubeQuery atomQ = (AtomCubeQuery) _query;

    final CacheStrategy strategy = toolChest.getCacheStrategy(atomQ);
    final byte[] value = cache.get(atomQ.getKey());
    if (value != null && value.length > 0) {
      Sequence retSeq = fetchResultFromCache(strategy, value);
      if (retSeq != null) {
        log.info("hit the cache!!!!");
        return retSeq;
      } else {
        log.info("evict the cache!!!!");
        cache.put(atomQ.getKey(), new byte[0]);
      }
    }
    final Map<String, Query> queries = atomQ.getQueries();
    AtomCubeResultValue resultValue = new AtomCubeResultValue(null);

    if (queries != null && !queries.isEmpty()) {
      final ListenableFuture<List<ImmutableBitmap>> futures = prepareFutures(atomQ);
      queryWatcher.registerQuery(atomQ, futures);

      executorService.submit(new Callable<Boolean>()
                             {
                               @Override
                               public Boolean call() throws Exception
                               {
                                 boolean result = subQuerySucceed(futures, atomQ);
                                 if (!result) {
                                   log.warn("some queries failed, try again");
                                   try {
                                     Thread.sleep(100);
                                   }
                                   catch (InterruptedException e) {
                                     e.printStackTrace();
                                   }
                                   result = subQuerySucceed(prepareFutures(atomQ), atomQ);
                                   if (!result) {
                                     log.error("Still fail, give up");
                                   }
                                 }
                                 done = true;
                                 return result;
                               }
                             }
      );

      try {
        final Map<String, Object> results = computeSet(atomQ);
        if (!results.isEmpty()) {
          final Map<String, Object> outputResults = new HashMap();
          for (PostAggregator postagg : atomQ.getPostAggregations()) {
            if (postagg instanceof AtomCubeSetPostAggregator) {
              continue;
            }
            outputResults.put(postagg.getName(), results.get(postagg.getName()));
          }
          List tmp = Lists.newArrayList();
          tmp.add(outputResults);
          resultValue = new AtomCubeResultValue(tmp);
        }
      }
      catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    final Function cacheFn = strategy.prepareForCache();
    final List<Object> cacheResults = Lists.newLinkedList();
    cacheResults.add(cacheFn.apply(new Result(new DateTime(), resultValue)));
    CacheUtil.populate(cache, mapper, atomQ.getKey(), cacheResults);

    return BaseSequence.simple(resultValue);
  }

  private Yielder Query(Query query, StringBuilder logstack) throws IOException
  {
    String queryId = query.getId();
    if (queryId == null) {
      queryId = UUID.randomUUID().toString();
      query = query.withId(queryId);
    }
    if (query.getContextValue(QueryContextKeys.TIMEOUT) == null) {
      query = query.withOverriddenContext(
          ImmutableMap.of(
              QueryContextKeys.TIMEOUT,
              config.getMaxIdleTime().toStandardDuration().getMillis()
          )
      );
    }

    if (log.isDebugEnabled()) {
      log.debug("Got query [%s]", query);
    }

    final Map<String, Object> responseContext = new MapMaker().makeMap();
    final Sequence res = query.run(texasRanger, responseContext);
    final Sequence results;
    if (res == null) {
      results = Sequences.empty();
    } else {
      results = res;
    }
    logstack.append("** query.yielder start \n");
    long begin = System.currentTimeMillis();
    final Yielder yielder = results.toYielder(
        null,
        new YieldingAccumulator()
        {
          @Override
          public Object accumulate(Object accumulated, Object in)
          {
            yield();
            return in;
          }
        }
    );
    log.debug("--- query.yielder use " + (System.currentTimeMillis() - begin) + "millis");
    logstack.append("** query.yielder use " + (System.currentTimeMillis() - begin) + "millis");
    return yielder;
  }

  private static ImmutableBitmap goThroughYielder(
      Yielder yielder,
      String metricName,
      Fetcher<String, Object, ImmutableBitmap> actor
  )
  {
    ImmutableBitmap ret = null;
    while (!yielder.isDone()) {
      Object o = yielder.get();
      ImmutableBitmap bitmap = actor.apply(metricName, o);
      ret = ret == null ? bitmap : ret.union(bitmap);
      yielder = yielder.next(null);
    }
    try {
      yielder.close();
    }
    catch (IOException e) {
      e.printStackTrace();
    }
    return ret;
  }

  private static String getAtomCubeMetricName(Query query)
  {
    String metricName = null;
    List<AggregatorFactory> aggs = null;
    if (query instanceof TopNQuery) {
      TopNQuery _query = (TopNQuery) query;
      aggs = _query.getAggregatorSpecs();
    } else if (query instanceof TimeseriesQuery) {
      TimeseriesQuery _query = (TimeseriesQuery) query;
      aggs = _query.getAggregatorSpecs();
    } else if (query instanceof GroupByQuery) {
      GroupByQuery _query = (GroupByQuery) query;
      aggs = _query.getAggregatorSpecs();
    }
    if (aggs != null) {
      for (AggregatorFactory agg : aggs) {
        if (AtomCubeDruidModule.ATOM_CUBE.equals(agg.getTypeName())) {
          metricName = agg.getName();
          break;
        }
      }
    }
    return metricName;
  }

  @FunctionalInterface
  public interface FetchBitmapFunc<Query, Yielder, ImmutableBitmap>
  {
    public ImmutableBitmap apply(Query query, Yielder yielder);
  }

  @FunctionalInterface
  public interface Fetcher<String, Object, ImmutableBitmap>
  {
    public ImmutableBitmap apply(String metricName, Object input);
  }

  private static ImmutableBitmap toImmutable(Object obj)
  {
    ImmutableBitmap ret = null;
    if (obj instanceof RoaringBitmap) {
      RoaringBitmap bitmap = (RoaringBitmap) obj;
      WrappedImmutableRoaringBitmap immutableRoaringBitmap =
          new WrappedImmutableRoaringBitmap(bitmap.toMutableRoaringBitmap());
      ret = immutableRoaringBitmap;
    }
    return ret;
  }
}
