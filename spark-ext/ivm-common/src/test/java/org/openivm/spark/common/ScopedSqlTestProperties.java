package org.openivm.spark.common;

import java.util.Properties;
import org.apache.spark.SparkContext;

/** Access the same JVM-public Spark local-properties API as the production bridge. */
public final class ScopedSqlTestProperties {
    private ScopedSqlTestProperties() {}

    public static Properties get(SparkContext context) {
        return context.getLocalProperties();
    }

    public static void set(SparkContext context, Properties properties) {
        context.setLocalProperties(properties);
    }
}
