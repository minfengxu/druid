package io.druid.query.aggregation.atomcube;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.metamx.emitter.EmittingLogger;
import io.druid.client.cache.CacheConfig;
import io.druid.guice.*;
import io.druid.initialization.DruidModule;
import io.druid.query.QueryWatcher;
import io.druid.segment.data.RoaringBitmapSerdeFactory;
import io.druid.segment.serde.ComplexMetrics;
import io.druid.server.QueryManager;
import org.roaringbitmap.RoaringBitmap;

import java.util.List;

public class AtomCubeDruidModule implements DruidModule
{
  private static final EmittingLogger log = new EmittingLogger(AtomCubeDruidModule.class);

  public static final String ATOM_CUBE = "atomCube";

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of(
        new SimpleModule().registerSubtypes(
            new NamedType(io.druid.query.aggregation.atomcube.AtomCubeAggregatorFactory.class, ATOM_CUBE),
            new NamedType(AtomCubeSetPostAggregator.class, "atomCubeSet"),
            new NamedType(AtomCubeSizePostAggregator.class, "atomCubeSize"),
            new NamedType(AtomCubeRawPostAggregator.class, "atomCubeRaw"),
            new NamedType(AtomCubeTopNSizePostAggregator.class, "atomCubeSizeTopN"),
            new NamedType(AtomCubeQuery.class, "atomCube")
        ).addSerializer(//todo should set with roaring or concise according the configure
                        RoaringBitmap.class,
                        new AtomCubeSerializer<RoaringBitmap>(new RoaringBitmapSerdeFactory())
        )
    );
  }

  @Override
  public void configure(Binder binder)
  {
    if (ComplexMetrics.getSerdeForType(ATOM_CUBE) == null) {
      ComplexMetrics.registerSerde(ATOM_CUBE, new AtomCubeComplexMetricSerde());
    }
    if (binder != null) {
      binder.bind(QueryWatcher.class).to(QueryManager.class).in(LazySingleton.class);
      binder.bind(QueryManager.class).in(LazySingleton.class);
      DruidBinders.queryToolChestBinder(binder)
                  .addBinding(AtomCubeQuery.class).to(AtomCubeQueryQueryToolChest.class);
      binder.bind(AtomCubeQueryQueryToolChest.class).in(LazySingleton.class);

      DruidBinders.queryRunnerFactoryBinder(binder)
                  .addBinding(AtomCubeQuery.class).to(AtomCubeQueryRunnerFactory.class);
      binder.bind(AtomCubeQueryRunnerFactory.class).in(LazySingleton.class);

      JsonConfigProvider.bind(binder, "druid.broker.cache", CacheConfig.class);
      binder.install(new CacheModule());

      Jerseys.addResource(binder, AtomCubeQueryResource.class);
      LifecycleModule.register(binder, AtomCubeQueryResource.class);
    } else {
      log.debug("for test");
    }
  }
}
