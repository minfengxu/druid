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

import com.google.common.collect.Maps;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ColumnSelectorFactory;

import java.nio.ByteBuffer;
import java.util.Map;

public class AtomCubeBufferAggregator implements BufferAggregator
{
  private final Map<Integer, Aggregator> buffer = Maps.newHashMap();
  private final ColumnSelectorFactory selectorFactory;
  private final AtomCubeAggregatorFactory aggregatorFactory;

  public AtomCubeBufferAggregator(AtomCubeAggregatorFactory AggFactory, ColumnSelectorFactory selFactory)
  {
    this.aggregatorFactory = AggFactory;
    this.selectorFactory = selFactory;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    buffer.put(position, aggregatorFactory.factorize(selectorFactory));
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    buffer.get(position).aggregate();
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return buffer.get(position).get();
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("AtomCubeAggregator does not support getFloat()");
  }

  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("AtomCubeAggregator does not support getLong()");
  }

  @Override
  public void close()
  {

  }
}
