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
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.OutputStreamSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.facebook.presto.parquet.ParquetDataOutput.createDataOutput;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.util.Objects.requireNonNull;

public class ParquetWriter
        implements Closeable
{
    private final List<ColumnWriter> columnWriters;
    private final MetadataWriter metadataWriter;
    private final OutputStreamSliceOutput outputStream;
    private final List<Type> types;
    private final List<String> names;

    private List<RowGroup> rowGroups = new ArrayList<>();

    private long rows;
    private boolean closed;
    private boolean writeHeader;

    public static final byte[] MAGIC = "PAR1".getBytes(US_ASCII);

    private static class WriteBuilder
            extends ParquetTypeVisitor<ColumnWriter>
    {
        private final MessageType type;
        private final ImmutableList.Builder<ColumnWriter> builder = ImmutableList.builder();

        WriteBuilder(MessageType type)
        {
            this.type = type;
        }

        List<ColumnWriter> build()
        {
            return builder.build();
        }

        @Override
        public ColumnWriter message(MessageType message, List<ColumnWriter> fields)
        {
            builder.addAll(fields);
            return super.message(message, fields);
        }

        @Override
        public ColumnWriter struct(GroupType struct, List<ColumnWriter> fields)
        {
            int fieldDefinitionLevel = type.getMaxDefinitionLevel(path(struct.getName()));
            int fieldRepetitionLevel = type.getMaxRepetitionLevel(path(struct.getName()));
            return new StructColumnWriter(ImmutableList.copyOf(fields), fieldDefinitionLevel, fieldRepetitionLevel);
        }

        @Override
        public ColumnWriter list(GroupType array, ColumnWriter element)
        {
            throw new UnsupportedOperationException("not supported");
        }

        @Override
        public ColumnWriter map(GroupType map, ColumnWriter key, ColumnWriter value)
        {
            throw new UnsupportedOperationException("not supported");
        }

        @Override
        public ColumnWriter primitive(PrimitiveType primitive)
        {
            String[] repeatedPath = currentPath();
            int repeatedD = type.getMaxDefinitionLevel(repeatedPath);
            return new LongColumnWriter(getType(primitive), ImmutableList.copyOf(repeatedPath), repeatedD);
        }

        Type getType(PrimitiveType primitive)
        {
            switch (primitive.getPrimitiveTypeName()) {
                case BINARY:
                    return VarcharType.VARCHAR;
                case INT32:
                    return IntegerType.INTEGER;
                case INT64:
                    return BigintType.BIGINT;
                default:
                    throw new UnsupportedOperationException("Unsupported type: " + primitive);
            }
        }

        private String[] currentPath()
        {
            String[] path = new String[fieldNames.size()];
            if (!fieldNames.isEmpty()) {
                Iterator<String> iter = fieldNames.descendingIterator();
                for (int i = 0; iter.hasNext(); i += 1) {
                    path[i] = iter.next();
                }
            }

            return path;
        }

        private String[] path(String name)
        {
            String[] path = new String[fieldNames.size() + 1];
            path[fieldNames.size()] = name;

            if (!fieldNames.isEmpty()) {
                Iterator<String> iter = fieldNames.descendingIterator();
                for (int i = 0; iter.hasNext(); i += 1) {
                    path[i] = iter.next();
                }
            }
            return path;
        }
    }

    public ParquetWriter(OutputStream outputStream, List<String> columnNames, List<Type> types)
    {
        this.outputStream = new OutputStreamSliceOutput(requireNonNull(outputStream, "outputstream is null"));

        Preconditions.checkArgument(types.size() == columnNames.size(), "type size not equals to name size");
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.names = ImmutableList.copyOf(requireNonNull(columnNames, "columnNames is null"));

        MessageType messageType = ParquetSchemaConverter.convert(types, names);
        System.out.println(messageType);
        WriteBuilder writeBuilder = new WriteBuilder(messageType);
        ParquetTypeVisitor.visit(messageType, writeBuilder);
        this.columnWriters = writeBuilder.build();
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
            writer.writeBlock(new ColumnTrunk(page.getBlock(channel)));
            bufferedBytes += writer.getBufferedBytes();
        }

        rows += page.getPositionCount();

        columnWriters.forEach(ColumnWriter::close);
        flush();
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

        if (rows > 0) {
            flush();
        }
        flushFooter();
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
        // write header
        if (!writeHeader) {
            getHeader().writeData(outputStream);
            writeHeader = true;
        }

        // write pages
        List<ParquetDataOutput> outputData = new ArrayList<>();
        long stripeStartOffset = outputStream.size();
        List<ColumnMetaData> columnMetaDatas = new ArrayList<>();
        for (ColumnWriter columnWriter : columnWriters) {
            List<ParquetDataOutput> streams = columnWriter.getDataStreams();
            columnMetaDatas.addAll(columnWriter.getColumnMetaData());
            outputData.addAll(streams);
        }

        // update stats
        updateRowGroups(getOffsetColumnMetadata(columnMetaDatas, stripeStartOffset));
        outputData.forEach(data -> data.writeData(outputStream));

        finishRowGroup();
    }

    private void finishRowGroup()
    {
        rows = 0;
        columnWriters.forEach(ColumnWriter::reset);
    }

    private void flushFooter()
    {
        checkState(closed);
        getFooter().forEach(data -> data.writeData(outputStream));
    }

    private void updateRowGroups(List<ColumnMetaData> columnMetaData)
    {
        long totalBytes = columnMetaData.stream().mapToLong(ColumnMetaData::getTotal_compressed_size).sum();
        Preconditions.checkArgument(totalBytes > 0);
        ImmutableList<ColumnChunk> columnChunks = columnMetaData.stream().map(ParquetWriter::toColumnChunk).collect(toImmutableList());
        rowGroups.add(new RowGroup(columnChunks, totalBytes, rows));
    }

    private static ColumnChunk toColumnChunk(ColumnMetaData metaData)
    {
        // TODO(lu.niu) Not sure whether file_offset is used
        ColumnChunk columnChunk = new ColumnChunk(0);
        columnChunk.setMeta_data(metaData);
        return columnChunk;
    }

    private ParquetDataOutput getHeader()
    {
        return createDataOutput(wrappedBuffer(MAGIC));
    }

    private List<ParquetDataOutput> getFooter()
    {

        List<ParquetDataOutput> outputData = new ArrayList<>();
        Slice footer = metadataWriter.getFooter(rowGroups, metadataWriter.getSchema(types, names));
        outputData.add(createDataOutput(footer));

        Slice footerSize = Slices.allocate(SIZE_OF_INT);
        footerSize.setInt(0, footer.length());
        outputData.add(createDataOutput(footerSize));

        outputData.add(createDataOutput(wrappedBuffer(MAGIC)));
        return outputData;
    }

    private List<ColumnMetaData> getOffsetColumnMetadata(List<ColumnMetaData> columnMetaDatas, long offset)
    {
        ImmutableList.Builder<ColumnMetaData> builder = ImmutableList.builder();
        long currentOffset = offset;
        for (ColumnMetaData metaData : columnMetaDatas) {
            builder.add(new ColumnMetaData(metaData.type, metaData.encodings, metaData.path_in_schema, metaData.codec, metaData.num_values, metaData.total_uncompressed_size, metaData.total_compressed_size, currentOffset));
            currentOffset += metaData.getTotal_compressed_size();
        }
        return builder.build();
    }
}
