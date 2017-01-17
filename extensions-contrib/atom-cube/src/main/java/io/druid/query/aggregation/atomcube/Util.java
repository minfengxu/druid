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
