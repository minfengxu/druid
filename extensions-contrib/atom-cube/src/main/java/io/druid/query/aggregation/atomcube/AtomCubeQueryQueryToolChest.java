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
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.query.CacheStrategy;
import io.druid.query.QueryRunner;
import io.druid.query.QueryToolChest;
import io.druid.query.Result;
import io.druid.query.aggregation.MetricManipulationFn;
import io.druid.query.topn.DimensionAndMetricValueExtractor;
import org.joda.time.DateTime;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class AtomCubeQueryQueryToolChest extends QueryToolChest<Result<AtomCubeResultValue>, AtomCubeQuery>
{
  private static final TypeReference<Result<AtomCubeResultValue>> TYPE_REFERENCE =
      new TypeReference<Result<AtomCubeResultValue>>()
      {
      };
  private static final TypeReference<Object> OBJECT_TYPE_REFERENCE = new TypeReference<Object>()
  {
  };

  public static final long CACHE_EXPIRE = 60000;

  @Override
  public QueryRunner mergeResults(QueryRunner runner)
  {
    return null;
  }

  @Override
  public ServiceMetricEvent.Builder makeMetricBuilder(AtomCubeQuery query)
  {
    return null;
  }

  @Override
  public Function<Result<AtomCubeResultValue>, Result<AtomCubeResultValue>> makePreComputeManipulatorFn(
      AtomCubeQuery query,
      MetricManipulationFn fn
  )
  {
    return null;
  }

  @Override
  public TypeReference getResultTypeReference()
  {
    return TYPE_REFERENCE;
  }

  @Override
  public CacheStrategy<Result<AtomCubeResultValue>, Object, AtomCubeQuery> getCacheStrategy(final AtomCubeQuery query)
  {
    return new CacheStrategy<Result<AtomCubeResultValue>, Object, AtomCubeQuery>()
    {
      private static final byte CACHE_STRATEGY_VERSION = 0x1;

      @Override
      public byte[] computeCacheKey(AtomCubeQuery query)
      {
        return null;
      }

      @Override
      public TypeReference<Object> getCacheObjectClazz()
      {
        return OBJECT_TYPE_REFERENCE;
      }

      @Override
      public Function<Result<AtomCubeResultValue>, Object> prepareForCache()
      {
        return new Function<Result<AtomCubeResultValue>, Object>()
        {
          @Override
          public Object apply(Result<AtomCubeResultValue> input)
          {
            List<DimensionAndMetricValueExtractor> results = Lists.newArrayList(input.getValue());
            final List<Object> retVal = Lists.newArrayList();
            retVal.add(input.getTimestamp().getMillis());
            for (DimensionAndMetricValueExtractor result : results) {
              retVal.add(result.getBaseObject());
            }
            return retVal;
          }
        };
      }

      @Override
      public Function<Object, Result<AtomCubeResultValue>> pullFromCache()
      {
        return new Function<Object, Result<AtomCubeResultValue>>()
        {
          @Override
          public Result<AtomCubeResultValue> apply(Object input)
          {
            List<Object> results = (List<Object>) input;
            List<Map<String, Object>> retVal = Lists.newArrayList();

            Iterator<Object> inputIter = results.iterator();

            DateTime timestamp = new DateTime(((Number) inputIter.next()).longValue());
            DateTime current = new DateTime();
            if ((current.getMillis() - timestamp.getMillis()) > CACHE_EXPIRE) {
              return null;
            }
            while (inputIter.hasNext()) {
              Map<String, Object> vals = (Map<String, Object>) inputIter.next();
              retVal.add(vals);
            }
            return new Result<>(timestamp, new AtomCubeResultValue(retVal));
          }
        };
      }

    };
  }
}
