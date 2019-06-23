package com.facebook.presto.parquet;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import parquet.bytes.BytesInput;
import parquet.column.values.ValuesWriter;
import parquet.column.values.plain.PlainValuesWriter;
import parquet.column.values.rle.RunLengthBitPackingHybridEncoder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

import static com.facebook.presto.parquet.ParquetWriterUtils.getParquetType;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.format.Encoding.*;

public class LongColumnWriter
        implements ColumnWriter
{
    private static final int DEFAULT_PAGE_SIZE = 1024 * 1024;
    private static final int INITIAL_SLAB_SIZE = 64;

    private final Type type;
    private final List<String> path;
    private final List<Encoding> encodings;
    CompressionCodec compressionCodec;

    private final org.apache.parquet.format.Type parquetType;
    private final ValuesWriter valuesWriter;
    private final RunLengthBitPackingHybridEncoder definitionLevel;
    private final RunLengthBitPackingHybridEncoder replicationLevel;

    private final ParquetMetadataConverter parquetMetadataConverter = new ParquetMetadataConverter();

    private boolean closed;
    private boolean getDataStreamsCalled;
    private int rows;

    private byte[] pageheader;
    private byte[] data;
    private byte[] replicationLevelBytes;
    private byte[] definitionLevelBytes;

    public LongColumnWriter(Type type, String name)
    {
        this.type = requireNonNull(type, "type is null");
        this.valuesWriter = new PlainValuesWriter(INITIAL_SLAB_SIZE, DEFAULT_PAGE_SIZE);
        this.definitionLevel = new RunLengthBitPackingHybridEncoder(1, INITIAL_SLAB_SIZE, DEFAULT_PAGE_SIZE);
        this.replicationLevel = new RunLengthBitPackingHybridEncoder(1, INITIAL_SLAB_SIZE, DEFAULT_PAGE_SIZE);

        this.path = ImmutableList.of(name);
        this.parquetType = getParquetType(type);
        this.encodings = ImmutableList.of(PLAIN);
        this.compressionCodec = CompressionCodec.UNCOMPRESSED;
    }

    @Override
    public void writeBlock(Block block)
    {
        checkState(!closed);
        checkArgument(block.getPositionCount() > 0, "Block is empty");

        // record nulls
        for (int position = 0; position < block.getPositionCount(); position++) {
            // TODO deal with null value
            if (block.isNull(position)) {
                throw new UnsupportedOperationException("Not support null value yet");
            }
        }

        // record values
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (!block.isNull(position)) {
                long value = type.getLong(block, position);
                valuesWriter.writeLong(value);
                try {
                    definitionLevel.writeInt(1);
                    replicationLevel.writeInt(0);
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
                rows++;
            }
        }
    }

    @Override
    public void close()
    {
        closed = true;
    }

    // Returns ColumnMetaData that offset is invalid
    @Override
    public ColumnMetaData getColumnMetaData()
    {
        checkState(closed);
        checkState(getDataStreamsCalled);

        long totalBytes = pageheader.length + data.length + replicationLevelBytes.length + definitionLevelBytes.length;
        return new ColumnMetaData(
                parquetType,
                encodings,
                path,
                compressionCodec,
                rows,
                totalBytes,
                totalBytes,
                0);
    }

    // page header
    // replication levels
    // definition levels
    // data
    @Override
    public List<ParquetDataOutput> getDataStreams()
    {
        checkState(closed);

        ImmutableList.Builder<ParquetDataOutput> outputDataStreams = ImmutableList.builder();
        BytesInput bytes = valuesWriter.getBytes();
        try {
            data = bytes.toByteArray();
            replicationLevelBytes = replicationLevel.toBytes().toByteArray();
            definitionLevelBytes = definitionLevel.toBytes().toByteArray();

            long uncompressedSize = data.length + replicationLevelBytes.length + definitionLevelBytes.length;
            long compressedSize = uncompressedSize;

            ByteArrayOutputStream pageHeaderOutputStream = new ByteArrayOutputStream();
            parquetMetadataConverter.writeDataPageV2Header((int) uncompressedSize, (int) compressedSize, rows, 0, rows, org.apache.parquet.column.Encoding.PLAIN, replicationLevelBytes.length, definitionLevelBytes.length, pageHeaderOutputStream);
            pageheader = pageHeaderOutputStream.toByteArray();
        }
        catch (IOException e) {
            throw new RuntimeException("Unable to write bytes", e);
        }

        outputDataStreams.add(ParquetDataOutput.createDataOutput(Slices.wrappedBuffer(pageheader)));
        outputDataStreams.add(ParquetDataOutput.createDataOutput(Slices.wrappedBuffer(replicationLevelBytes)));
        outputDataStreams.add(ParquetDataOutput.createDataOutput(Slices.wrappedBuffer(definitionLevelBytes)));
        outputDataStreams.add(ParquetDataOutput.createDataOutput(Slices.wrappedBuffer(data)));

        getDataStreamsCalled = true;
        return outputDataStreams.build();
    }

    @Override
    public long getBufferedBytes()
    {
        return valuesWriter.getBufferedSize() + definitionLevel.getBufferedSize() + replicationLevel.getBufferedSize();
    }

    @Override
    public long getRetainedBytes()
    {
        return valuesWriter.getAllocatedSize() + definitionLevel.getAllocatedSize() + replicationLevel.getAllocatedSize();
    }

    @Override
    public int getRows()
    {
        return rows;
    }

    @Override
    public void reset()
    {
        closed = false;
        rows = 0;
        valuesWriter.reset();
        definitionLevel.reset();
        replicationLevel.reset();

        getDataStreamsCalled = false;
        pageheader = null;
        data = null;
        replicationLevelBytes = null;
        definitionLevelBytes = null;
    }
}
