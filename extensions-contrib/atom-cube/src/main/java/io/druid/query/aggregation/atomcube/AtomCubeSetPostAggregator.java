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
import com.google.common.collect.Lists;
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import io.druid.collections.bitmap.ImmutableBitmap;
import io.druid.collections.bitmap.WrappedImmutableRoaringBitmap;
import io.druid.query.aggregation.PostAggregator;
import org.roaringbitmap.IntConsumer;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.RoaringBitmap;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AtomCubeSetPostAggregator implements PostAggregator
{
  private final String name;
  private final List<String> fields;
  private final Func func;

  enum Func
  {
    UNION,
    INTERSECT,
    NOT
  }

  @JsonCreator
  public AtomCubeSetPostAggregator(
      @JsonProperty("name") String name,
      @JsonProperty("func") String func,
      @JsonProperty("fields") List<String> fields
  )
  {
    this.name = name;
    this.fields = fields;
    this.func = Func.valueOf(func.toUpperCase());

    if (fields.size() <= 1) {
      throw new IAE("Illegal number of fields[%s], must be > 1", fields.size());
    }
  }

  @Override
  public Set<String> getDependentFields()
  {
    return null;
  }

  @Override
  public Comparator<RoaringBitmap> getComparator()
  {
    return AtomCubeAggregatorFactory.COMPARATOR;
  }

  @Override
  public Object compute(final Map<String, Object> combinedAggregators)
  {
    List<ImmutableBitmap> bitmaps = Lists.newArrayList();

    for (int i = 0; i < fields.size(); i++) {
      ImmutableBitmap bitmap = (ImmutableBitmap) combinedAggregators.get(fields.get(i));
      if (bitmap != null) {
        bitmaps.add(bitmap);
      } else {
        bitmaps.add(AtomCubeAggregatorFactory.BITMAP_FACTORY.makeEmptyImmutableBitmap());
      }
    }

    if (func == Func.UNION) {
      return AtomCubeAggregatorFactory.BITMAP_FACTORY.union(bitmaps);
    } else if (func == Func.INTERSECT) {
      return AtomCubeAggregatorFactory.BITMAP_FACTORY.intersection(bitmaps);
    } else if (func == Func.NOT) {
      ImmutableBitmap first = bitmaps.get(0);
      final int first_length = getMaxNumber(first);
      List<ImmutableBitmap> sets = Lists.newArrayList();
      sets.addAll(bitmaps);
      sets.remove(0);
      ImmutableBitmap _tmp = AtomCubeAggregatorFactory.BITMAP_FACTORY.union(sets);
      ImmutableBitmap tmp = AtomCubeAggregatorFactory.BITMAP_FACTORY.complement(_tmp, first_length + 1);
      return bitmaps.get(0).intersection(tmp);
    } else {
      throw new ISE("WTF?! No implementation for function[%s]", func);
    }
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public String getFunc()
  {
    return func.toString();
  }

  @JsonProperty
  public List<String> getFields()
  {
    return fields;
  }

  private int getMaxNumber(ImmutableBitmap bitmap)
  {
    final int[] length = {0};
//    int length = 0;
    if (bitmap instanceof WrappedImmutableRoaringBitmap) {
      org.roaringbitmap.buffer.ImmutableRoaringBitmap _bitmap =
          ((WrappedImmutableRoaringBitmap) bitmap).getBitmap();
      _bitmap.forEach(new IntConsumer()
      {
        @Override
        public void accept(int value)
        {
          if (value > length[0]) {
            length[0] = value;
          }
        }
      });
    } else {
      IntIterator iter = bitmap.iterator();
      while (iter.hasNext()) {
        int _length = iter.next();
        if (_length > length[0]) {
          length[0] = _length;
        }
      }
    }
    return length[0];
  }
}
