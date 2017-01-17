package io.druid.query.aggregation.atomcube;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.google.common.io.BaseEncoding;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.bitmap.WrappedImmutableRoaringBitmap;
import io.druid.segment.data.BitmapSerdeFactory;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;

/**
 * Created by minfengxu on 2016/4/29.
 */
public class AtomCubeSerializer<T extends RoaringBitmap> extends JsonSerializer<T> {

  private final BitmapSerdeFactory bitmapSerdeFactory;

  public AtomCubeSerializer(BitmapSerdeFactory bitmapSerdeFactory) {
    this.bitmapSerdeFactory = bitmapSerdeFactory;
  }

  @Override
  public void serialize(RoaringBitmap value, JsonGenerator jgen, SerializerProvider provider) throws IOException, JsonProcessingException {
    WrappedImmutableRoaringBitmap _bitmap = new WrappedImmutableRoaringBitmap(value.toMutableRoaringBitmap());
    jgen.writeString(serialize(_bitmap, bitmapSerdeFactory));
//    jgen.writeBinary(bitmapSerdeFactory.getObjectStrategy().toBytes(value));
  }

  static String serialize(ImmutableBitmap bitmap, BitmapSerdeFactory bitmapSerdeFactory) {
    return BaseEncoding.base64().encode(bitmapSerdeFactory.getObjectStrategy().toBytes(bitmap));
  }
}
