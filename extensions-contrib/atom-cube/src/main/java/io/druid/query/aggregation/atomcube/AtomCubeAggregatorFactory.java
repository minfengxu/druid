package io.druid.query.aggregation.atomcube;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Ints;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.bitmap.WrappedImmutableRoaringBitmap;
import com.metamx.common.IAE;
import com.metamx.common.StringUtils;
import io.druid.query.aggregation.*;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.data.ConciseBitmapSerdeFactory;
import io.druid.segment.data.RoaringBitmapSerdeFactory;
import org.apache.commons.codec.binary.Base64;
import org.roaringbitmap.RoaringBitmap;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * Created by minfengxu on 2016/4/22.
 */
@JsonTypeName("atomCube")
public class AtomCubeAggregatorFactory extends AggregatorFactory {

  private static final byte CACHE_TYPE_ID = 0xC;

  private final String name;
  private final String fieldName;
  private final String bitmapType;

  public static BitmapSerdeFactory BITMAP_SERDE_FACTORY;
  public static BitmapFactory BITMAP_FACTORY;

  static final Comparator<RoaringBitmap> COMPARATOR = Ordering.from(
    new Comparator<RoaringBitmap>() {
      @Override
      public int compare(RoaringBitmap lhs, RoaringBitmap rhs) {
        if(lhs == null) {
          return -1;
        }
        if(rhs == null) {
          return 1;
        }
        return Ints.compare(lhs.getSizeInBytes(), rhs.getSizeInBytes());
      }
    }
  ).nullsFirst();

  public AtomCubeAggregatorFactory(
    @JsonProperty("name") String name,
    @JsonProperty("fieldName") String fieldName,
    @JsonProperty("bitmap") String bitmapType
  ) {
    this.name = name;
    this.fieldName = fieldName;
    this.bitmapType = bitmapType;
    if(bitmapType == null || bitmapType.isEmpty() || bitmapType.equals("roaring")) {
      BITMAP_SERDE_FACTORY = new RoaringBitmapSerdeFactory();
    } else {
      BITMAP_SERDE_FACTORY = new ConciseBitmapSerdeFactory();
    }
    BITMAP_FACTORY = BITMAP_SERDE_FACTORY.getBitmapFactory();
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory) {
    ObjectColumnSelector selector = metricFactory.makeObjectColumnSelector(fieldName);

    if (selector == null) {
      return Aggregators.noopAggregator();
    }

    final Class classOfObject = selector.classOfObject();
    if (classOfObject.equals(Object.class) || RoaringBitmap.class.isAssignableFrom(classOfObject)) {
      return new AtomCubeAggregator(name, selector);
    }

    throw new IAE(
      "Incompatible type for metric[%s], expected a AtomCube, got a %s", fieldName, classOfObject
    );
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory) {
    ObjectColumnSelector selector = metricFactory.makeObjectColumnSelector(fieldName);

    if (selector == null) {
      return Aggregators.noopBufferAggregator();
    }

    final Class classOfObject = selector.classOfObject();
    if (classOfObject.equals(Object.class) || RoaringBitmap.class.isAssignableFrom(classOfObject)) {
      return new AtomCubeBufferAggregator(this, metricFactory);
    }

    throw new IAE(
      "Incompatible type for metric[%s], expected a AtomCube, got a %s", fieldName, classOfObject
    );
  }

  @Override
  public Comparator getComparator() {
    return COMPARATOR;
  }

  @Override
  public Object combine(Object lhs, Object rhs) {
    if (rhs == null) {
      return lhs;
    }
    if (lhs == null) {
      return rhs;
    }
    for(int i : ((RoaringBitmap)rhs).toArray()) {
      ((RoaringBitmap)lhs).add(i);
    }
    return lhs;
  }

  @Override
  public AggregatorFactory getCombiningFactory()  {
    return new AtomCubeAggregatorFactory(name, name, bitmapType);
  }

  @Override
  public AggregatorFactory getMergingFactory(AggregatorFactory other) throws AggregatorFactoryNotMergeableException
  {
    if (other.getName().equals(this.getName()) && this.getClass() == other.getClass()) {
      return getCombiningFactory();
    } else {
      throw new AggregatorFactoryNotMergeableException(this, other);
    }
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()  {
    return Arrays.<AggregatorFactory>asList(new AtomCubeAggregatorFactory(fieldName, fieldName, bitmapType));
  }

  @Override
  public RoaringBitmap deserialize(Object object)  {
    final ByteBuffer byteBuffer;
    if ( object == null ) {
      throw new IAE("Cannot deserialize null object");
    } else if (object instanceof byte[]) {
      byteBuffer = ByteBuffer.wrap((byte[]) object);
    } else if (object instanceof String) {
      String value = (String)object;
      if(value.isEmpty()) {
        return new RoaringBitmap();
      }
      byteBuffer = ByteBuffer.wrap(Base64.decodeBase64(value));
    } else if (object instanceof ByteBuffer) {
      byteBuffer = (ByteBuffer)object;
    } else {
      throw new IAE("Cannot deserialize class[%s]", object.getClass().getName());
    }
    ImmutableBitmap immutableBitmap =
      BITMAP_SERDE_FACTORY.getObjectStrategy().fromByteBuffer(byteBuffer, byteBuffer.remaining());
    if(immutableBitmap instanceof WrappedImmutableRoaringBitmap) {
      RoaringBitmap bitmap = ((WrappedImmutableRoaringBitmap) immutableBitmap).getBitmap().toRoaringBitmap();
      return bitmap;
    }
    throw new IAE("Cannot deserialize this type of immutableBitmap object");
  }

  @Override
  public Object finalizeComputation(Object object)  {
    //return ((ImmutableBitmap) object).size();
    return object;
  }

  @Override
  @JsonProperty
  public String getName()  {
    return name;
  }

  @Override
  public List<String> requiredFields()  {
    return Arrays.asList(fieldName);
  }

  @JsonProperty
  public String getFieldName()  {
    return fieldName;
  }

  @Override
  public byte[] getCacheKey()  {
    byte[] fieldNameBytes = StringUtils.toUtf8(fieldName);

    return ByteBuffer.allocate(1 + fieldNameBytes.length).put(CACHE_TYPE_ID).put(fieldNameBytes).array();
  }

  @Override
  public String getTypeName()  {
    return "atomCube";
  }

  @Override
  public int getMaxIntermediateSize()  {
    return 0;//not implement yet
  }

  @Override
  public Object getAggregatorStartValue()  {
    throw new UnsupportedOperationException();
  }

  @Override
  public String toString()  {
    return "AtomCubeAggregatorFactory{" +
      "name='" + name + '\'' +
      ", fieldName='" + fieldName + '\'' +
      '}';
  }

  @Override
  public boolean equals(Object o)  {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    AtomCubeAggregatorFactory that = (AtomCubeAggregatorFactory) o;

    if (!fieldName.equals(that.fieldName)) return false;
    if (!name.equals(that.name)) return false;

    return true;
  }

  @Override
  public int hashCode()  {
    int result = name.hashCode();
    result = 13 * result + fieldName.hashCode();
    return result;
  }
}
