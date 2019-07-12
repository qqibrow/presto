/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.parquet;

import com.facebook.presto.parquet.DefValueV2.DefDefIterator;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridEncoder;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.converter.ParquetMetadataConverter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiConsumer;

import static com.facebook.presto.parquet.ParquetDataOutput.createDataOutput;
import static com.facebook.presto.parquet.ParquetWriterUtils.getParquetType;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.format.Encoding.PLAIN;

public class PrimitiveColumnWriter
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
    private long totalBytes;

    private byte[] pageheader;

    private final int maxDefinitionLevel;

    public PrimitiveColumnWriter(Type type, List<String> name, int maxDefinitionLevel, int maxRepetitionLevel, ValuesWriter valuesWriter)
    {
        this.type = requireNonNull(type, "type is null");
        HeapByteBufferAllocator allocator = HeapByteBufferAllocator.getInstance();
        this.valuesWriter = requireNonNull(valuesWriter);
        this.definitionLevel = new RunLengthBitPackingHybridEncoder(BytesUtils.getWidthFromMaxInt(maxDefinitionLevel), INITIAL_SLAB_SIZE, DEFAULT_PAGE_SIZE, allocator);
        this.replicationLevel = new RunLengthBitPackingHybridEncoder(BytesUtils.getWidthFromMaxInt(maxRepetitionLevel), INITIAL_SLAB_SIZE, DEFAULT_PAGE_SIZE, allocator);
        this.writer = ParquetWriterUtils.getWriter(this.type, this.valuesWriter);

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

        ColumnTrunk current = new ColumnTrunk(columnTrunk.getBlock(),
                ImmutableList.<DefValueV2>builder().addAll(columnTrunk.getDefList()).add(DefValueV2.getIterator(columnTrunk.getBlock(), maxDefinitionLevel)).build(),
                ImmutableList.<RepValueV2>builder().addAll(columnTrunk.getRepValueV2List()).add(RepValueV2.getIterator(columnTrunk.getBlock())).build());

        // record values
        for (int position = 0; position < current.getBlock().getPositionCount(); position++) {
            if (!current.getBlock().isNull(position)) {
                writer.accept(current.getBlock(), position);
            }
        }

        // write definitionLevels
        List<Integer> defs = new ArrayList<>();
        Iterator<Integer> defIterator = new DefDefIterator(current.getDefList());
        while (defIterator.hasNext()) {
            int next = defIterator.next();
            try {
                definitionLevel.writeInt(next);
                defs.add(next);
                if (next != maxDefinitionLevel) {
                    nullCounts++;
                }
                rows++;
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
        System.out.println("defs: " + defs);

        // write reptitionLevel
        List<Integer> reps = new ArrayList<>();
        Iterator<Integer> repIterator = new RepValueV2.RepRepIterator(current.getRepValueV2List());
        while (repIterator.hasNext()) {
            int next = repIterator.next();
            try {
                replicationLevel.writeInt(next);
                reps.add(next);
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
        System.out.println("reptitions: " + reps);
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
        try {
            ParquetDataOutput data = createDataOutput(valuesWriter.getBytes());
            ParquetDataOutput replicationLevelOutput = createDataOutput(replicationLevel.toBytes());
            ParquetDataOutput definitionLevelOutput = createDataOutput(definitionLevel.toBytes());
            long uncompressedSize = data.size() + replicationLevelOutput.size() + definitionLevelOutput.size();
            long compressedSize = uncompressedSize;

            ByteArrayOutputStream pageHeaderOutputStream = new ByteArrayOutputStream();
            parquetMetadataConverter.writeDataPageV2Header((int) uncompressedSize, (int) compressedSize, rows, nullCounts, rows, org.apache.parquet.column.Encoding.PLAIN, (int) replicationLevelOutput.size(), (int) definitionLevelOutput.size(), pageHeaderOutputStream);
            pageheader = pageHeaderOutputStream.toByteArray();

            outputDataStreams.add(createDataOutput(Slices.wrappedBuffer(pageheader)));
            outputDataStreams.add(replicationLevelOutput);
            outputDataStreams.add(definitionLevelOutput);
            outputDataStreams.add(data);
        }
        catch (IOException e) {
            throw new RuntimeException("Unable to write bytes", e);
        }

        getDataStreamsCalled = true;
        List<ParquetDataOutput> dataOutputs = outputDataStreams.build();
        totalBytes = dataOutputs.stream().mapToLong(ParquetDataOutput::size).sum();
        return dataOutputs;
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
        totalBytes = 0;

        valuesWriter.reset();
        definitionLevel.reset();
        replicationLevel.reset();

        getDataStreamsCalled = false;
        pageheader = null;
    }
}
