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

import com.metamx.emitter.EmittingLogger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class Util
{
  public static ConcurrentHashMap<AtomCubeAggregatorFactory, AtomicLong[]> records = new ConcurrentHashMap<>();
  public static final int TYPE_STRING = 0;
  public static final int TYPE_BYTES = 1;
  public static final int TYPE_BYTEBUFFER = 2;

  public static void accumulate(AtomCubeAggregatorFactory target, int type, long value)
  {
    if (type > 2 || type < 0) {
      return;
    }
    AtomicLong[] values = records.get(target);
    if (values == null) {
      synchronized (target) {
        values = records.get(target);
        if (values == null) {
          values = new AtomicLong[]{new AtomicLong(0), new AtomicLong(0), new AtomicLong(0)};
          records.put(target, values);
        }
      }
    }
    values[type].addAndGet(value);
  }

  public static void print(EmittingLogger log)
  {
    for (Map.Entry<AtomCubeAggregatorFactory, AtomicLong[]> entry : records.entrySet()) {
      long time_string = entry.getValue()[0].get();
      long time_bytes = entry.getValue()[1].get();
      long time_bytebuffer = entry.getValue()[2].get();
      log.debug("--- deString:" + time_string + " deByte[]:" + time_bytes + " deByteBuffer:" + time_bytebuffer);
    }
  }
}
