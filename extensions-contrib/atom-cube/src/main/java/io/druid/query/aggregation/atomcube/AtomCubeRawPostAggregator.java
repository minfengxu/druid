package io.druid.query.aggregation.atomcube;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.bitmap.WrappedImmutableRoaringBitmap;
import com.metamx.common.ISE;
import io.druid.query.aggregation.PostAggregator;
import org.roaringbitmap.IntConsumer;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.RoaringBitmap;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AtomCubeRawPostAggregator implements PostAggregator {
  private final String name;
  private final Format format;
  private final String field;

  enum Format {
    LIST,
    ROARINGBASE64
  }

  @JsonCreator
  public AtomCubeRawPostAggregator(
    @JsonProperty("name") String name,
    @JsonProperty("format") String format,
    @JsonProperty("field") String field
  ) {
    this.name = Preconditions.checkNotNull(name, "name");
    this.format = format == null ? Format.LIST : Format.valueOf(format.toUpperCase());
    this.field = Preconditions.checkNotNull(field, "field");
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
    final ImmutableBitmap bitmap = (ImmutableBitmap) combinedAggregators.get(field);

    if (format == Format.LIST) {
      final List<Integer> retVal = Lists.newArrayList();
      if(bitmap != null) {
        if(bitmap instanceof WrappedImmutableRoaringBitmap) {
          org.roaringbitmap.buffer.ImmutableRoaringBitmap _bitmap =
            ((WrappedImmutableRoaringBitmap)bitmap).getBitmap();
          _bitmap.forEach(new IntConsumer() {
            @Override
            public void accept(int value) {
              retVal.add(value);
            }
          });
        } else {
          final IntIterator iterator = bitmap.iterator();
          while (iterator.hasNext()) {
            retVal.add(iterator.next());
          }
        }
      }
      return retVal;
    } else if (format == Format.ROARINGBASE64) {
      String retVal = "";
      if(bitmap != null) {
        retVal = AtomCubeSerializer.serialize(bitmap, AtomCubeAggregatorFactory.BITMAP_SERDE_FACTORY);
      }
      return retVal;
    } else {
      throw new ISE("WTF?! No implementation for format[%s]", format);
    }
  }

  @Override
  @JsonProperty
  public String getName() {
    return name;
  }

  @JsonProperty
  public Format getFormat() {
    return format;
  }

  @JsonProperty
  public String getField() {
    return field;
  }
}
