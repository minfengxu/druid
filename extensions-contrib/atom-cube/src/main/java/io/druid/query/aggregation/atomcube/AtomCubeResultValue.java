package io.druid.query.aggregation.atomcube;

import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.metamx.common.IAE;
import io.druid.query.topn.DimensionAndMetricValueExtractor;

import javax.annotation.Nullable;
import java.io.*;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by minfengxu on 2016/5/30 0030.
 */
public class AtomCubeResultValue implements Iterable<DimensionAndMetricValueExtractor>, Serializable {

  private final List<DimensionAndMetricValueExtractor> value;

  public AtomCubeResultValue(List<?> value) {
    this.value = (value == null) ? Lists.<DimensionAndMetricValueExtractor>newArrayList() : Lists.transform(
      value,
      new Function<Object, DimensionAndMetricValueExtractor>()
      {
        @Override
        public DimensionAndMetricValueExtractor apply(@Nullable Object input)
        {
          if (input instanceof Map) {
            return new DimensionAndMetricValueExtractor((Map) input);
          } else if (input instanceof DimensionAndMetricValueExtractor) {
            return (DimensionAndMetricValueExtractor) input;
          } else {
            throw new IAE("Unknown type for input[%s]", input.getClass());
          }
        }
      }
    );
  }

  @JsonValue
  public List<DimensionAndMetricValueExtractor> getValue()
  {
    return value;
  }

  @Override
  public Iterator<DimensionAndMetricValueExtractor> iterator()
  {
    return value.iterator();
  }

  @Override
  public String toString()
  {
    return "AtomCubeResultValue{" +
      "value=" + value +
      '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    AtomCubeResultValue that = (AtomCubeResultValue) o;

    if (value != null ? !value.equals(that.value) : that.value != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return value != null ? value.hashCode() : 0;
  }

//  public static byte[] toByteArray(AtomCubeResultValue obj) throws IOException {
//    byte[] bytes = null;
//    ByteArrayOutputStream bos = null;
//    ObjectOutputStream oos = null;
//    try {
//      bos = new ByteArrayOutputStream();
//      oos = new ObjectOutputStream(bos);
//      oos.writeObject(obj);
//      oos.flush();
//      bytes = bos.toByteArray();
//    } finally {
//      if (oos != null) {
//        oos.close();
//      }
//      if (bos != null) {
//        bos.close();
//      }
//    }
//    return bytes;
//  }
//
//  public static AtomCubeResultValue toResult(byte[] bytes) throws IOException, ClassNotFoundException {
//    Object obj = null;
//    ByteArrayInputStream bis = null;
//    ObjectInputStream ois = null;
//    try {
//      bis = new ByteArrayInputStream(bytes);
//      ois = new ObjectInputStream(bis);
//      obj = ois.readObject();
//    } finally {
//      if (bis != null) {
//        bis.close();
//      }
//      if (ois != null) {
//        ois.close();
//      }
//    }
//    return (AtomCubeResultValue)obj;
//  }

}
