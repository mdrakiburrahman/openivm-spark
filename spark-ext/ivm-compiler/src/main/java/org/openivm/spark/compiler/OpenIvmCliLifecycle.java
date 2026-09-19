package org.openivm.spark.compiler;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/** Per-request launch permission and native-process ownership acknowledgement. */
final class OpenIvmCliLifecycle {
    private final Path directory;
    private final Path lockPath;
    private final Path cancellation;
    private final Path launching;
    private final Path identity;
    private final Path acknowledged;
    private final Path exited;

    OpenIvmCliLifecycle(Path directory) {
        this.directory = directory;
        lockPath = directory.resolve("cli-launch.lock");
        cancellation = directory.resolve("cli-cancelled");
        launching = directory.resolve("cli-launching");
        identity = directory.resolve("cli-native.identity");
        acknowledged = directory.resolve("cli-native.ready");
        exited = directory.resolve("cli-native.exited");
    }

    void initialize() throws IOException {
        Files.createFile(lockPath);
    }

    boolean beginLaunch() throws IOException, InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(OpenIvmCliWorker.CLEANUP_TIMEOUT_SECONDS);
        try (Locked ignored = lockUntil(deadline)) {
            if (cancellationRequested()) {
                return false;
            }
            Files.createFile(launching);
            return true;
        }
    }

    void requestCancellation(long deadline) throws IOException, InterruptedException {
        // Terminal intent must survive failure to acquire the ownership gate.
        try {
            Files.createFile(cancellation);
        } catch (FileAlreadyExistsException alreadyCancelled) {
            // Retain the original marker and its inode on repeated cancellation.
        }
        try (Locked ignored = lockUntil(deadline)) {
            // This barrier orders reconciliation against a committed launch.
        }
    }

    boolean cancellationRequested() {
        return Files.exists(cancellation);
    }

    boolean launchCommitted() {
        return Files.exists(launching);
    }

    boolean nativeExited() {
        return Files.exists(exited);
    }

    void acknowledgeNative(ProcessHandle process) throws IOException {
        writeIdentity(identity, process);
        // Publish only after the complete immutable identity record is closed.
        Files.createFile(acknowledged);
    }

    void acknowledgeHelper(ProcessHandle process) throws IOException {
        writeIdentity(directory.resolve("cli-helper.identity"), process);
    }

    private static void writeIdentity(Path destination, ProcessHandle process) throws IOException {
        String start = process.info().startInstant().map(Instant::toString).orElse("");
        Files.write(destination, (process.pid() + "\n" + start + "\n").getBytes(StandardCharsets.UTF_8));
    }

    void acknowledgeExit() throws IOException {
        if (!Files.exists(exited)) {
            Files.createFile(exited);
        }
    }

    Optional<NativeIdentity> nativeIdentity() throws IOException {
        if (!Files.exists(acknowledged)) {
            return Optional.empty();
        }
        List<String> lines = Files.readAllLines(identity, StandardCharsets.UTF_8);
        if (lines.size() != 2) {
            throw new IOException("Incomplete native CLI identity record");
        }
        try {
            long pid = Long.parseLong(lines.get(0));
            if (pid <= 0) {
                throw new IOException("Invalid native CLI process ID");
            }
            Optional<Instant> start = lines.get(1).isEmpty()
                    ? Optional.empty() : Optional.of(Instant.parse(lines.get(1)));
            return Optional.of(new NativeIdentity(pid, start));
        } catch (NumberFormatException | DateTimeParseException invalid) {
            throw new IOException("Invalid native CLI identity record", invalid);
        }
    }

    Path directory() {
        return directory;
    }

    private Locked lockUntil(long deadline) throws IOException, InterruptedException {
        FileChannel channel = FileChannel.open(lockPath, StandardOpenOption.WRITE);
        boolean acquired = false;
        try {
            while (deadline - System.nanoTime() > 0) {
                FileLock lock;
                try {
                    lock = channel.tryLock();
                } catch (OverlappingFileLockException busy) {
                    lock = null;
                }
                if (lock != null) {
                    Locked result = new Locked(channel, lock);
                    acquired = true;
                    return result;
                }
                Thread.sleep(10);
            }
            throw new IOException("Timed out coordinating native CLI startup");
        } finally {
            if (!acquired) {
                channel.close();
            }
        }
    }

    static final class NativeIdentity {
        final long pid;
        final Optional<Instant> start;

        NativeIdentity(long pid, Optional<Instant> start) {
            this.pid = pid;
            this.start = start;
        }
    }

    private static final class Locked implements AutoCloseable {
        private final FileChannel channel;
        private final FileLock lock;

        Locked(FileChannel channel, FileLock lock) {
            this.channel = channel;
            this.lock = lock;
        }

        @Override
        public void close() throws IOException {
            try {
                lock.release();
            } finally {
                channel.close();
            }
        }
    }
}
