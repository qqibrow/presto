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

import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.OutputStreamSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.util.Objects.requireNonNull;

public class ParquetWriter
        implements Closeable
{
    private final List<ColumnWriter> columnWriters;
    private final MetadataWriter metadataWriter;
    private final OutputStream outputStream;
    private final List<Type> types;

    private boolean closed;

    private static final int PARQUET_METADATA_LENGTH = 4;
    public static final byte[] MAGIC = "PAR1".getBytes(US_ASCII);

    public ParquetWriter(OutputStream outputStream, List<String> columnNames, List<Type> types)
    {
        this.outputStream = requireNonNull(outputStream, "outputstream is null");
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.columnWriters = ImmutableList.of(new LongColumnWriter(BigintType.BIGINT));
        this.metadataWriter = new MetadataWriter();
    }

    public void write(com.facebook.presto.spi.Page page)
            throws IOException
    {
        requireNonNull(page, "page is null");
        if (page.getPositionCount() == 0) {
            return;
        }

        checkArgument(page.getChannelCount() == columnWriters.size());

        int bufferedBytes = 0;
        for (int channel = 0; channel < page.getChannelCount(); channel++) {
            ColumnWriter writer = columnWriters.get(channel);
            writer.writeBlock(page.getBlock(channel));
            bufferedBytes += writer.getBufferedBytes();
        }

        // TODO flush under different reasons
    }

    @Override
    public void close()
            throws IOException
    {
        if (closed) {
            return;
        }
        closed = true;
        columnWriters.forEach(ColumnWriter::close);
        flush();
        outputStream.close();
    }

    // Parquet File Layout:
    //
    // MAGIC
    // variable: Data
    // variable: Metadata
    // 4 bytes: MetadataLength
    // MAGIC

    private void flush()
            throws IOException
    {
        Preconditions.checkArgument(closed, "Only support flush at last for now");

        List<ParquetDataOutput> outputData = new ArrayList<>();
        long stripeStartOffset = 0;

        ParquetDataOutput magicOutput = ParquetDataOutput.createDataOutput(Slices.wrappedBuffer(MAGIC));
        outputData.add(magicOutput);

        List<Statistics> statistics = new ArrayList<>();
        for (ColumnWriter columnWriter : columnWriters) {
            List<ParquetDataOutput> streams = columnWriter.getDataStreams();
            int rows = columnWriter.getRows();
            long bytes = streams.stream().mapToLong(stream -> stream.size()).sum();
            statistics.add(new Statistics(rows, bytes));
            outputData.addAll(streams);
        }
        Slice footer = metadataWriter.getFooter(2, statistics);
        outputData.add(ParquetDataOutput.createDataOutput(footer));

        Slice footerSize = Slices.allocate(SIZE_OF_INT);
        footerSize.setInt(0, footer.length());
        outputData.add(ParquetDataOutput.createDataOutput(footerSize));

        outputData.add(magicOutput);

        OutputStreamSliceOutput output = new OutputStreamSliceOutput(outputStream);
        outputData.forEach(data -> data.writeData(output));
        output.close();
    }
}
