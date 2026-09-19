package org.openivm.spark.common;

import java.util.Properties;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;

/**
 * One JVM invocation owns the complete SQL/capture/local-property lifetime.
 *
 * <p>Py4J callers must feature-detect apiVersion() == 1 and
 * QueryLogExport.apiVersion() == 1, then call execute directly, without separate
 * begin/end or local-property RPCs. This eager, result-discarding API is intended
 * for CREATE/REFRESH commands. It is neither a result-data API nor a transaction.
 * Exceptions propagate; a lost transport response is not a SQL outcome.
 * Inspect QueryLogExport independently using the reserved request ID, then release it.
 *
 * <p>Java deliberately uses SparkContext's JVM-public get/setLocalProperties,
 * which are Scala package-private. No reflection or thread-ID lookup is needed.
 */
public final class ScopedSqlBridge {
    private ScopedSqlBridge() {}

    public static int apiVersion() {
        return 1;
    }

    /**
     * Executes and materializes SQL before sealing its capture, even if the client
     * disconnects before receiving the return. No global lock or async handoff is used.
     * Group and pool must be explicit nonempty values; description may be empty.
     * Duplicate requests and capture limits are rejected before SQL runs.
     */
    public static void execute(
            SparkSession spark,
            String sqlText,
            String requestId,
            String jobGroupId,
            String jobDescription,
            String schedulerPool,
            boolean interruptOnCancel) throws InterruptedException {
        require(spark != null, "spark must not be null");
        require(sqlText != null && !sqlText.trim().isEmpty(), "sqlText must be nonempty");
        require(jobGroupId != null && !jobGroupId.isEmpty(), "jobGroupId must be nonempty");
        require(jobDescription != null, "jobDescription must not be null");
        require(schedulerPool != null && !schedulerPool.isEmpty(), "schedulerPool must be nonempty");

        SparkContext context = spark.sparkContext();
        // Detach both saved and working state from any inherited/shared Properties.
        Properties current = context.getLocalProperties();
        Properties previous = current == null ? null : (Properties) current.clone();
        QueryLogExport.begin(spark, requestId);
        boolean[] succeeded = {false};
        preservingFailure(() -> preservingFailure(() -> {
            context.setLocalProperties(previous == null ? new Properties() : (Properties) previous.clone());
            context.setLocalProperty(QueryLogExport.RequestIdProperty(), requestId);
            context.setJobGroup(jobGroupId, jobDescription, interruptOnCancel);
            context.setLocalProperty("spark.scheduler.pool", schedulerPool);
            checkInterrupted();
            spark.sql(sqlText).collectAsList();
            checkInterrupted();
            succeeded[0] = true;
        }, () -> QueryLogExport.end(spark, requestId, succeeded[0])),
                () -> context.setLocalProperties(previous));
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }

    private static void checkInterrupted() throws InterruptedException {
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("Scoped SQL execution interrupted");
        }
    }

    @FunctionalInterface
    interface Action {
        void run() throws InterruptedException;
    }

    /** Attempt all cleanups without replacing the original SQL/cancellation error. */
    static void preservingFailure(Action body, Action cleanup) throws InterruptedException {
        Throwable primary = null;
        try {
            body.run();
        } catch (Throwable error) {
            primary = error;
            throw error;
        } finally {
            if (primary == null) {
                cleanup.run();
            } else {
                try {
                    cleanup.run();
                } catch (Throwable error) {
                    if (error != primary) {
                        primary.addSuppressed(error);
                    }
                }
            }
        }
    }
}
