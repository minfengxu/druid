package io.druid.query.aggregation.atomcube;

import com.metamx.common.IAE;
import io.druid.data.input.InputRow;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.data.*;
import io.druid.segment.serde.ComplexColumnPartSupplier;
import io.druid.segment.serde.ComplexMetricExtractor;
import io.druid.segment.serde.ComplexMetricSerde;
import org.roaringbitmap.RoaringBitmap;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.List;

public class AtomCubeComplexMetricSerde extends ComplexMetricSerde
{

  public AtomCubeComplexMetricSerde()
  {
  }

  @Override
  public String getTypeName()
  {
    return AtomCubeDruidModule.ATOM_CUBE;
  }

  @Override
  public ComplexMetricExtractor getExtractor()
  {
    return new ComplexMetricExtractor()
    {
      @Override
      public Class<RoaringBitmap> extractedClass()
      {
        return RoaringBitmap.class;
      }

      @Override
      public RoaringBitmap extractValue(InputRow inputRow, String metricName)
      {
        Object rawValue = inputRow.getRaw(metricName);

        if (rawValue instanceof RoaringBitmap) {
          return (RoaringBitmap) rawValue;
        } else {
          RoaringBitmap bitmap = new RoaringBitmap();

          List<String> dimValues = inputRow.getDimension(metricName);
          if (dimValues == null) {
            return bitmap;
          }

          for (String dimensionValue : dimValues) {
            bitmap.add(Integer.parseInt(dimensionValue));
          }
          return bitmap;
        }
      }
    };
  }

  @Override
  public void deserializeColumn(ByteBuffer byteBuffer, ColumnBuilder columnBuilder)
  {
    final GenericIndexed column = GenericIndexed.read(byteBuffer, getObjectStrategy());
    columnBuilder.setComplexColumn(new ComplexColumnPartSupplier(getTypeName(), column));
  }

  @Override
  public ObjectStrategy getObjectStrategy()
  {
    return new ObjectStrategy<RoaringBitmap>()
    {
      @Override
      public Class<? extends RoaringBitmap> getClazz()
      {
        return RoaringBitmap.class;
      }

      @Override
      public RoaringBitmap fromByteBuffer(ByteBuffer buffer, int numBytes)
      {
        final ByteBuffer readOnlyBuffer = buffer.asReadOnlyBuffer();
        readOnlyBuffer.limit(readOnlyBuffer.position() + numBytes);
        byte[] buf = new byte[readOnlyBuffer.remaining()];
        readOnlyBuffer.get(buf);
        InputStream inputStream = new ByteArrayInputStream(buf);

        RoaringBitmap bitmap = new RoaringBitmap();
        try {
          bitmap.deserialize(new DataInputStream(inputStream));
        }
        catch (IOException e) {
          throw new IAE("bitmap can't be deserialized:" + e.getMessage());
        }
        return bitmap;
      }

      @Override
      public byte[] toBytes(RoaringBitmap bitmap)
      {
        if (bitmap == null) {
          return new byte[]{};
        }
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream wheretoserialize = new DataOutputStream(bos);
        bitmap.runOptimize();
        try {
          bitmap.serialize(wheretoserialize);
        }
        catch (IOException e) {
          throw new IAE("bitmap can't be serialized:" + e.getMessage());
        }
        return bos.toByteArray();
      }

      @Override
      public int compare(RoaringBitmap o1, RoaringBitmap o2)
      {
        return o1.getSizeInBytes() - o2.getSizeInBytes();
      }
    };
  }
}
