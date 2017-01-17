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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import io.druid.client.cache.Cache;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.Query;
import io.druid.query.Result;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.filter.DimFilter;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.topn.TopNQuery;
import org.joda.time.Interval;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class AtomCubeQuery extends BaseQuery<Result<AtomCubeResultValue>>
{
  private final Map<String, Query> namedQueries;
  private final List<PostAggregator> postAggregators;
  private final Map<String, Cache.NamedKey> subQueryKeys;
  private final Cache.NamedKey queryKey;

  @JsonCreator
  public AtomCubeQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("queries") Map<String, Query> queries,
      @JsonProperty("postAggregations") List<PostAggregator> postAggregations,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(dataSource, querySegmentSpec, false, context);
    this.namedQueries = queries;
    this.postAggregators = postAggregations;
    subQueryKeys = genSubQueryKeys();
    queryKey = genKey();
  }

  @JsonProperty("queries")
  public Map<String, Query> getQueries()
  {
    return namedQueries;
  }

  @JsonProperty("postAggregations")
  public List<PostAggregator> getPostAggregations()
  {
    return postAggregators;
  }

  @Override
  public boolean hasFilters()
  {
    return false;
  }

  @Override
  public DimFilter getFilter()
  {
    return null;
  }

  @Override
  public String getType()
  {
    return AtomCubeDruidModule.ATOM_CUBE;
  }

  @Override
  public AtomCubeQuery withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    return this;
  }

  @Override
  public AtomCubeQuery withDataSource(DataSource dataSource)
  {
    return this;
  }

  @Override
  public AtomCubeQuery withOverriddenContext(Map contextOverrides)
  {
    return new AtomCubeQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        namedQueries,
        postAggregators,
        computeOverridenContext(contextOverrides)
    );
  }

  public Cache.NamedKey getKey()
  {
    return this.queryKey;
  }

  public Map<String, Cache.NamedKey> getSubQueryKeys()
  {
    return this.subQueryKeys;
  }

  private Cache.NamedKey genKey()
  {
    String id = getCombinedSubQueryKey();
    Map<String, Cache.NamedKey> postKeys = Maps.newHashMap(subQueryKeys);
    if (postAggregators != null) {
      for (PostAggregator _postAggregator : postAggregators) {
        genPostAggratorKey(_postAggregator, postKeys);
      }
    }
    StringBuffer sb = new StringBuffer();
    for (Cache.NamedKey postKey : getSortedCacheKeys(postKeys)) {
      sb.append(postKey.namespace);
    }
    String key = sb.toString();
    return new Cache.NamedKey(id, key.getBytes());
  }

  private List<Cache.NamedKey> getSortedCacheKeys(Map<String, Cache.NamedKey> keys)
  {
    List<Cache.NamedKey> sortedKeys = Lists.newArrayList();
    for (Map.Entry<String, Cache.NamedKey> key : keys.entrySet()) {
      sortedKeys.add(key.getValue());
    }
    Collections.sort(sortedKeys, new Comparator<Cache.NamedKey>()
    {
      @Override
      public int compare(Cache.NamedKey o1, Cache.NamedKey o2)
      {
        return o1.namespace.compareTo(o2.namespace);
      }
    });
    return sortedKeys;
  }

  private void genPostAggratorKey(PostAggregator _postAggregator, Map<String, Cache.NamedKey> postKeys)
  {
    String keyWord = "";
    List<String> fields = Lists.newArrayList();
    if (_postAggregator instanceof AtomCubeRawPostAggregator) {
      AtomCubeRawPostAggregator postAggregator = (AtomCubeRawPostAggregator) _postAggregator;
      fields.add(postAggregator.getField());
      keyWord = postAggregator.getFormat().toString();
    } else if (_postAggregator instanceof AtomCubeSetPostAggregator) {
      AtomCubeSetPostAggregator postAggregator = (AtomCubeSetPostAggregator) _postAggregator;
      fields = postAggregator.getFields();
      keyWord = postAggregator.getFunc();
    } else if (_postAggregator instanceof AtomCubeSizePostAggregator) {
      AtomCubeSizePostAggregator postAggregator = (AtomCubeSizePostAggregator) _postAggregator;
      fields.add(postAggregator.getField());
      keyWord = "cardinality";
    }
    StringBuffer sb = new StringBuffer(keyWord);
    for (String field : fields) {
      Cache.NamedKey _key = postKeys.get(field);
      sb.append(Integer.toHexString(_key.hashCode()));
    }
    String _id = Integer.toHexString(sb.toString().hashCode());
    Cache.NamedKey postKey = new Cache.NamedKey(_id, _id.getBytes());
    postKeys.put(_postAggregator.getName(), postKey);
  }

  private Map<String, Cache.NamedKey> genSubQueryKeys()
  {
    ImmutableMap.Builder<String, Cache.NamedKey> builder = ImmutableMap.builder();
    for (Map.Entry<String, Query> query : namedQueries.entrySet()) {
      Cache.NamedKey key = getKey(query.getValue());
      builder.put(query.getKey(), key);
    }
    return builder.build();
  }

  private String getCombinedSubQueryKey()
  {
    List<String> ids = Lists.newArrayList();
    for (Map.Entry<String, Cache.NamedKey> key : subQueryKeys.entrySet()) {
      ids.add(key.getValue().namespace);
    }
    Collections.sort(ids, Ordering.<String>natural());
    StringBuffer sb = new StringBuffer();
    for (String s : ids) {
      sb.append(s);
    }
    return Integer.toHexString(sb.toString().hashCode());
  }

  private Cache.NamedKey getKey(Query _query)
  {
    Cache.NamedKey key = null;
    StringBuffer sb = new StringBuffer();
    List<String> dsNames = _query.getDataSource().getNames();
    Collections.sort(dsNames, Ordering.natural());
    for (String s : dsNames) {
      sb.append(s);
    }
    List<Interval> intervals = _query.getIntervals();
    for (Interval interval : sortIntervals(intervals)) {
      long start = interval.getStartMillis();
      long end = interval.getEndMillis();
      sb.append(start).append(end);
    }
    byte[] filterKey = new byte[0];
    if (_query.hasFilters()) {
      if (_query instanceof TimeseriesQuery) {
        TimeseriesQuery query = (TimeseriesQuery) _query;
        filterKey = query.getDimensionsFilter().getCacheKey();
      } else if (_query instanceof TopNQuery) {
        TopNQuery query = (TopNQuery) _query;
        filterKey = query.getDimensionsFilter().getCacheKey();

      } else if (_query instanceof GroupByQuery) {
        GroupByQuery query = (GroupByQuery) _query;
        filterKey = query.getDimFilter().getCacheKey();
      }
    }
    String queryId = Integer.toHexString(sb.toString().hashCode());
    key = new Cache.NamedKey(
        queryId,
        ByteBuffer.allocate(queryId.getBytes().length + filterKey.length)
                  .put(queryId.getBytes())
                  .put(filterKey)
                  .array()
    );
    return key;
  }

  private static List<Interval> sortIntervals(List<Interval> intervals)
  {
    List<Interval> _intervals = Lists.newCopyOnWriteArrayList();
    List<Interval> ret = Lists.newCopyOnWriteArrayList();
    for (Interval interval : intervals) {
      _intervals.add(interval);
    }
    while (!_intervals.isEmpty()) {
      Interval aInterval = null;
      for (Interval interval : _intervals) {
        if (aInterval == null) {
          aInterval = interval;
        } else {
          if (aInterval.getStartMillis() > interval.getStartMillis()) {
            aInterval = interval;
          }
        }
      }
      ret.add(aInterval);
      _intervals.remove(aInterval);
    }
    return ret;
  }
}
