package com.inovaworkscc.quartz.cassandra;

import com.inovaworkscc.quartz.cassandra.clojure.DynamicClassLoadHelper;
import org.quartz.spi.ClassLoadHelper;

public class DynamicCassandraJobStore extends CassandraJobStore {

    public DynamicCassandraJobStore() {
        super();
    }

    @Override
    protected ClassLoadHelper getClassLoaderHelper(ClassLoadHelper original) {
        return new DynamicClassLoadHelper();
    }
}
