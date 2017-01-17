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
import com.google.common.collect.Ordering;
import io.druid.query.aggregation.PostAggregator;
import org.roaringbitmap.RoaringBitmap;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AtomCubeTopNSizePostAggregator implements PostAggregator
{
  private final String name;
  private final List<String> fields;
  private final Integer topN;

  @JsonCreator
  public AtomCubeTopNSizePostAggregator(
      @JsonProperty("name") String name,
      @JsonProperty("fields") List<String> fields,
      @JsonProperty("topn") Integer topn
  )
  {
    this.name = name;
    this.fields = fields;
    this.topN = topn > 0 ? topn : 0;
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
  public Object compute(final Map<String, Object> cardinalities)
  {
    List<Integer> cardinalityList = Lists.newArrayList();
    for (String field : fields) {
      cardinalityList.add((Integer) cardinalities.get(field));
    }
    if (cardinalityList.size() > 1) {
      Collections.sort(cardinalityList, Ordering.natural());
    }
    int length = cardinalityList.size() > topN ? topN : cardinalityList.size();
    return cardinalityList.subList(0, length);
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public List<String> getFields()
  {
    return fields;
  }
}
