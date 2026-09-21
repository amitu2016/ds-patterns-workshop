package kafkalite.log;

import kafkalite.api.Message;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A log is an append-only sequence of messages stored on disk.
 * Each message is assigned a unique, monotonically increasing 64-bit offset.
 *
 * <p><b>Binary Format per Message:</b>
 * <pre>
 *   [4 bytes: total message size]
 *   [4 bytes: key size (K)]
 *   [K bytes: key bytes]
 *   [4 bytes: value size (V)]
 *   [V bytes: value bytes]
 * </pre>
 *
 * <p><b>Storage Design Decision:</b>
 * Uses direct {@link FileChannel} on-disk append rather than tickloom's {@code Storage} interface.
 * Tickloom's storage abstraction is not well-suited for managing multiple partition segment files
 * per broker, and its asynchronous nature introduces callback complexity that obscures the core
 * mental model of sequential log append, index recovery, and High Watermark consistency.
 *
 * <p>Thread-safe for writes (via {@link ReentrantLock}) and lock-free for concurrent reads.
 */
public class Log implements AutoCloseable {

    private static final int MESSAGE_SIZE_LENGTH = 4;
    private static final int KEY_SIZE_LENGTH = 4;
    private static final int VALUE_SIZE_LENGTH = 4;

    private final File file;
    private final FileChannel channel;
    private final RandomAccessFile randomAccessFile;

    private final AtomicLong nextOffset = new AtomicLong(1);
    private final Map<Long, Long> offsetIndex = new ConcurrentHashMap<>();
    private final Lock writeLock = new ReentrantLock();

    public Log(File file) throws IOException {
        this.file = file;
        File parent = file.getParentFile();
        if (parent != null && !parent.exists()) {
            parent.mkdirs();
        }
        this.randomAccessFile = new RandomAccessFile(file, "rw");
        this.channel = randomAccessFile.getChannel();
        recoverIndex();
    }

    private void recoverIndex() throws IOException {
        long position = 0;
        long fileSize = channel.size();
        long currentOffset = 1;

        while (position + MESSAGE_SIZE_LENGTH <= fileSize) {
            int messageSize = readLength(position);
            if (position + MESSAGE_SIZE_LENGTH + messageSize > fileSize) {
                // Incomplete write at the end of file; truncate
                channel.truncate(position);
                break;
            }
            offsetIndex.put(currentOffset, position);
            position += MESSAGE_SIZE_LENGTH + messageSize;
            currentOffset++;
        }
        channel.position(position);
        nextOffset.set(currentOffset);
    }

    /**
     * Appends key-value bytes to the log and returns its assigned offset.
     */
    public long append(byte[] key, byte[] value) throws IOException {
        writeLock.lock();
        try {
            long position = channel.position();
            long offset = nextOffset.getAndIncrement();

            writeToFile(key, value);
            offsetIndex.put(offset, position);
            return offset;
        } finally {
            writeLock.unlock();
        }
    }

    public long append(Message message) throws IOException {
        return append(message.getKey(), message.getValue());
    }

    /**
     * Reads a single message at the given offset.
     */
    public Message readSingleMessage(long offset) throws IOException {
        Long filePosition = offsetIndex.get(offset);
        if (filePosition == null) {
            throw new IOException("Offset " + offset + " does not exist in log " + file.getName());
        }

        int messageSize = readLength(filePosition);
        ByteBuffer buffer = readMessage(filePosition, messageSize);
        byte[] keyBytes = readKey(buffer);
        byte[] valueBytes = readValue(buffer);

        return new Message(keyBytes, valueBytes);
    }

    /**
     * Reads all messages between startOffset and maxOffset (inclusive).
     */
    public List<Message> read(long startOffset, long maxOffset) throws IOException {
        List<Message> messages = new ArrayList<>();
        for (long i = startOffset; i <= maxOffset; i++) {
            if (!offsetIndex.containsKey(i)) {
                break;
            }
            messages.add(readSingleMessage(i));
        }
        return messages;
    }

    /**
     * Returns the offset of the last written message in the log, or 0 if empty.
     */
    public long lastOffset() {
        return nextOffset.get() - 1;
    }

    public long nextOffset() {
        return nextOffset.get();
    }

    public File file() {
        return file;
    }

    private void writeToFile(byte[] key, byte[] value) throws IOException {
        byte[] safeKey = key != null ? key : new byte[0];
        byte[] safeValue = value != null ? value : new byte[0];
        int messageSize = KEY_SIZE_LENGTH + safeKey.length + VALUE_SIZE_LENGTH + safeValue.length;

        ByteBuffer buffer = ByteBuffer.allocate(MESSAGE_SIZE_LENGTH + messageSize);
        buffer.putInt(messageSize);
        buffer.putInt(safeKey.length);
        buffer.put(safeKey);
        buffer.putInt(safeValue.length);
        buffer.put(safeValue);

        buffer.flip();
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }

    private int readLength(long fileLocation) throws IOException {
        ByteBuffer length = ByteBuffer.allocate(MESSAGE_SIZE_LENGTH);
        channel.read(length, fileLocation);
        length.flip();
        return length.getInt();
    }

    private ByteBuffer readMessage(long fileLocation, int recordSize) throws IOException {
        ByteBuffer records = ByteBuffer.allocate(recordSize);
        channel.read(records, fileLocation + MESSAGE_SIZE_LENGTH);
        records.flip();
        return records;
    }

    private byte[] readKey(ByteBuffer message) {
        int keySize = message.getInt();
        byte[] key = new byte[keySize];
        message.get(key);
        return key;
    }

    private byte[] readValue(ByteBuffer message) {
        int valueSize = message.getInt();
        byte[] value = new byte[valueSize];
        message.get(value);
        return value;
    }

    @Override
    public void close() throws IOException {
        writeLock.lock();
        try {
            if (channel.isOpen()) {
                channel.force(true);
                channel.close();
            }
            randomAccessFile.close();
        } finally {
            writeLock.unlock();
        }
    }
}
