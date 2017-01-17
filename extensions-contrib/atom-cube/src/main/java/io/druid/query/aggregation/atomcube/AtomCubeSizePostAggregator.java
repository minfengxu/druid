package io.druid.query.aggregation.atomcube;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.query.aggregation.PostAggregator;
import org.roaringbitmap.RoaringBitmap;

import java.util.Comparator;
import java.util.Map;
import java.util.Set;

public class AtomCubeSizePostAggregator implements PostAggregator {
  private final String name;
  private final String field;

  @JsonCreator
  public AtomCubeSizePostAggregator(
    @JsonProperty("name") String name,
    @JsonProperty("field") String field
  ) {
    this.name = name;
    this.field = field;
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
  public Object compute(final Map<String, Object> combinedAggregators) {
    ImmutableBitmap input = ((ImmutableBitmap) combinedAggregators.get(field));
    return input == null? 0: input.size();
  }

  @Override
  @JsonProperty
  public String getName() {
    return name;
  }

  @JsonProperty
  public String getField() {
    return field;
  }
}
