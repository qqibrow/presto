package com.facebook.presto.parquet;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.plain.PlainValuesWriter;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridEncoder;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.converter.ParquetMetadataConverter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;

import static com.facebook.presto.parquet.ParquetWriterUtils.getParquetType;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.format.Encoding.PLAIN;

public class LongColumnWriter
        implements ColumnWriter
{
    private static final int DEFAULT_PAGE_SIZE = 1024 * 1024;
    private static final int INITIAL_SLAB_SIZE = 64;

    private final Type type;
    private final List<String> path;
    private final List<Encoding> encodings;
    private final BiConsumer<Block, Integer> writer;
    CompressionCodec compressionCodec;

    private final org.apache.parquet.format.Type parquetType;
    private final ValuesWriter valuesWriter;
    private final RunLengthBitPackingHybridEncoder definitionLevel;
    private final RunLengthBitPackingHybridEncoder replicationLevel;

    private final ParquetMetadataConverter parquetMetadataConverter = new ParquetMetadataConverter();

    private boolean closed;
    private boolean getDataStreamsCalled;
    private int rows;
    private int nullCounts;

    private byte[] pageheader;
    private byte[] data;
    private byte[] replicationLevelBytes;
    private byte[] definitionLevelBytes;

    private final int maxDefinitionLevel;

    public LongColumnWriter(Type type, List<String> name, int maxDefinitionLevel, int maxRepetitionLevel)
    {
        this.type = requireNonNull(type, "type is null");
        HeapByteBufferAllocator allocator = HeapByteBufferAllocator.getInstance();
        this.valuesWriter = new PlainValuesWriter(INITIAL_SLAB_SIZE, DEFAULT_PAGE_SIZE, allocator);
        this.definitionLevel = new RunLengthBitPackingHybridEncoder(BytesUtils.getWidthFromMaxInt(maxDefinitionLevel), INITIAL_SLAB_SIZE, DEFAULT_PAGE_SIZE, allocator);
        this.replicationLevel = new RunLengthBitPackingHybridEncoder(BytesUtils.getWidthFromMaxInt(maxRepetitionLevel), INITIAL_SLAB_SIZE, DEFAULT_PAGE_SIZE, allocator);
        this.writer = ParquetWriterUtils.getWriter(this.type, valuesWriter);

        this.path = ImmutableList.copyOf(name);
        this.parquetType = getParquetType(type);
        this.encodings = ImmutableList.of(PLAIN);
        this.compressionCodec = CompressionCodec.UNCOMPRESSED;
        this.maxDefinitionLevel = maxDefinitionLevel;
    }

    @Override
    public void writeBlock(ColumnTrunk columnTrunk)
    {
        checkState(!closed);

        // empty iterator
        ColumnTrunk current;
        if (!columnTrunk.getDefIterator().hasNext()) {
            current = new ColumnTrunk(columnTrunk.getBlock(),
                    new DefinitionValueIterator.PrimitiveIterator(columnTrunk.getBlock(), maxDefinitionLevel),
                    new RepetitionValueIterator.BlockIterator(columnTrunk.getBlock()));
        }
        else {
            current = new ColumnTrunk(columnTrunk.getBlock(),
                    new DefinitionValueIterator.PrimitiveIterator(columnTrunk.getDefIterator(), columnTrunk.getBlock(), maxDefinitionLevel),
                    new RepetitionValueIterator.BlockIterator(columnTrunk.getRepIterator(), columnTrunk.getBlock()));
        }

        // record values
        checkArgument(current.getBlock().getPositionCount() > 0, "Block is empty");
        for (int position = 0; position < current.getBlock().getPositionCount(); position++) {
            if (!current.getBlock().isNull(position)) {
                writer.accept(current.getBlock(), position);
            }
        }

        // write definitionLevels
        Iterator<Optional<Integer>> defIterator = current.getDefIterator();
        while (defIterator.hasNext()) {
            Optional<Integer> next = defIterator.next();
            checkArgument(next.isPresent());
            try {
                definitionLevel.writeInt(next.get());
                if (next.get() != maxDefinitionLevel) {
                    nullCounts++;
                }
                rows++;
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }

        // write reptitionLevel
        Iterator<RepetitionValueIterator.RepetitionValue> repIterator = current.getRepIterator();
        while (repIterator.hasNext()) {
            RepetitionValueIterator.RepetitionValue next = repIterator.next();
            try {
                replicationLevel.writeInt(next.getValue());
            }
            catch (IOException e) {
                e.printStackTrace();
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
    public List<ColumnMetaData> getColumnMetaData()
    {
        checkState(closed);
        checkState(getDataStreamsCalled);

        long totalBytes = pageheader.length + data.length + replicationLevelBytes.length + definitionLevelBytes.length;
        return ImmutableList.of(new ColumnMetaData(
                parquetType,
                encodings,
                path,
                compressionCodec,
                rows,
                totalBytes,
                totalBytes,
                0));
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
            parquetMetadataConverter.writeDataPageV2Header((int) uncompressedSize, (int) compressedSize, rows, nullCounts, rows, org.apache.parquet.column.Encoding.PLAIN, replicationLevelBytes.length, definitionLevelBytes.length, pageHeaderOutputStream);
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

    public long getRetainedBytes()
    {
        return valuesWriter.getAllocatedSize() + definitionLevel.getAllocatedSize() + replicationLevel.getAllocatedSize();
    }

    @Override
    public void reset()
    {
        closed = false;
        rows = 0;
        nullCounts = 0;
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
