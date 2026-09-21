package kafkalite.common;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class TestUtils {
    private static final Random random = new Random();

    /** Binds all sockets first, then releases, so the ports don't collide with each other. */
    public static List<Integer> choosePorts(int count) {
        List<ServerSocket> sockets = new ArrayList<>();
        List<Integer> ports = new ArrayList<>();
        try {
            for (int i = 0; i < count; i++) {
                ServerSocket socket = new ServerSocket(0);
                sockets.add(socket);
                ports.add(socket.getLocalPort());
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to open server socket", e);
        } finally {
            for (ServerSocket socket : sockets) {
                try { socket.close(); } catch (IOException ignored) { }
            }
        }
        return ports;
    }

    public static int choosePort() { return choosePorts(1).get(0); }

    public static File tempDir() {
        File f = new File(System.getProperty("java.io.tmpdir"), "kafkalite-" + random.nextInt(1_000_000));
        f.mkdirs();
        f.deleteOnExit();
        return f;
    }

    public static void rm(File fileOrDirectory) {
        if (fileOrDirectory == null || !fileOrDirectory.exists()) return;
        try {
            Files.walkFileTree(fileOrDirectory.toPath(), new SimpleFileVisitor<>() {
                @Override public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.delete(file); return FileVisitResult.CONTINUE;
                }
                @Override public FileVisitResult postVisitDirectory(Path dir, IOException e) throws IOException {
                    Files.delete(dir); return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException ignored) { }
    }
}
