package org.openivm.spark.compiler;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.TimeUnit;

/** Owns the POSIX lock in a different process from the driver's probe descriptors. */
final class OpenIvmCliGateHolderFixture {
    private OpenIvmCliGateHolderFixture() {}

    public static void main(String[] args) throws IOException, InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        try (FileChannel channel = FileChannel.open(Paths.get(args[0]), StandardOpenOption.WRITE);
                FileLock gate = acquire(channel, deadline)) {
            Files.createFile(Paths.get(args[1]));
            while (!Files.exists(Paths.get(args[2]))) {
                if (deadline - System.nanoTime() <= 0) {
                    throw new IOException("Gate holder release deadline expired");
                }
                Thread.sleep(10);
            }
        }
    }

    private static FileLock acquire(FileChannel channel, long deadline)
            throws IOException, InterruptedException {
        while (deadline - System.nanoTime() > 0) {
            FileLock gate = channel.tryLock();
            if (gate != null) {
                return gate;
            }
            Thread.sleep(10);
        }
        throw new IOException("Gate holder acquisition deadline expired");
    }
}
