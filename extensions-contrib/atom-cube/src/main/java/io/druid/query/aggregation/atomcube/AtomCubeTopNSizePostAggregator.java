package io.druid.query.aggregation.atomcube;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.api.client.util.Lists;
import com.google.common.collect.Ordering;
import io.druid.query.aggregation.PostAggregator;
import org.roaringbitmap.RoaringBitmap;

import java.util.*;

public class AtomCubeTopNSizePostAggregator implements PostAggregator {
  private final String name;
  private final List<String> fields;
  private final Integer topN;

  @JsonCreator
  public AtomCubeTopNSizePostAggregator(
    @JsonProperty("name") String name,
    @JsonProperty("fields") List<String> fields,
    @JsonProperty("topn") Integer topn
  ) {
    this.name = name;
    this.fields = fields;
    this.topN = topn > 0 ? topn : 0;
  }

  @Override
  public Set<String> getDependentFields() {
    return null;
  }

  @Override
  public Comparator<RoaringBitmap> getComparator() {
    return AtomCubeAggregatorFactory.COMPARATOR;
  }

  @Override
  public Object compute(final Map<String, Object> cardinalities) {
    List<Integer> cardinalityList = Lists.newArrayList();
    for(String field : fields) {
      cardinalityList.add((Integer) cardinalities.get(field));
    }
    if(cardinalityList.size() > 1) {
      Collections.sort(cardinalityList, Ordering.natural());
    }
    int length = cardinalityList.size() > topN ? topN : cardinalityList.size();
    return cardinalityList.subList(0, length);
  }

  @Override
  @JsonProperty
  public String getName() {
    return name;
  }

  @JsonProperty
  public List<String> getFields() {
    return fields;
  }
}
