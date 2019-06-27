package com.facebook.presto.parquet;

import com.google.common.collect.Lists;
import org.apache.hadoop.record.Record;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Iterator;
import java.util.List;

import static com.facebook.presto.parquet.ParquetValueWriters.byteBuffers;
import static com.facebook.presto.parquet.ParquetValueWriters.collections;
import static com.facebook.presto.parquet.ParquetValueWriters.maps;
import static com.facebook.presto.parquet.ParquetValueWriters.option;
import static com.facebook.presto.parquet.ParquetValueWriters.strings;
import static com.facebook.presto.parquet.ParquetValueWriters.unboxed;

public class GenericParquetWriter
{
    private GenericParquetWriter()
    {
    }

    public static <T> ParquetValueWriter<T> buildWriter(MessageType type)
    {
        return (ParquetValueWriter<T>) ParquetTypeVisitor.visit(type, new WriteBuilder(type));
    }

    private static class WriteBuilder
            extends ParquetTypeVisitor<ParquetValueWriter<?>>
    {
        private final MessageType type;

        WriteBuilder(MessageType type)
        {
            this.type = type;
        }

        @Override
        public ParquetValueWriter<?> message(MessageType message,
                List<ParquetValueWriter<?>> fieldWriters)
        {
            return struct(message.asGroupType(), fieldWriters);
        }

        @Override
        public ParquetValueWriter<?> struct(GroupType struct,
                List<ParquetValueWriter<?>> fieldWriters)
        {
            List<Type> fields = struct.getFields();
            List<ParquetValueWriter<?>> writers = Lists.newArrayListWithExpectedSize(fieldWriters.size());
            for (int i = 0; i < fields.size(); i += 1) {
                Type fieldType = struct.getType(i);
                int fieldD = type.getMaxDefinitionLevel(path(fieldType.getName()));
                writers.add(option(fieldType, fieldD, fieldWriters.get(i)));
            }

            return new RecordWriter(writers);
        }

        @Override
        public ParquetValueWriter<?> list(GroupType array, ParquetValueWriter<?> elementWriter)
        {
            GroupType repeated = array.getFields().get(0).asGroupType();
            String[] repeatedPath = currentPath();

            int repeatedD = type.getMaxDefinitionLevel(repeatedPath);
            int repeatedR = type.getMaxRepetitionLevel(repeatedPath);

            org.apache.parquet.schema.Type elementType = repeated.getType(0);
            int elementD = type.getMaxDefinitionLevel(path(elementType.getName()));

            return collections(repeatedD, repeatedR, option(elementType, elementD, elementWriter));
        }

        @Override
        public ParquetValueWriter<?> map(GroupType map,
                ParquetValueWriter<?> keyWriter,
                ParquetValueWriter<?> valueWriter)
        {
            GroupType repeatedKeyValue = map.getFields().get(0).asGroupType();
            String[] repeatedPath = currentPath();

            int repeatedD = type.getMaxDefinitionLevel(repeatedPath);
            int repeatedR = type.getMaxRepetitionLevel(repeatedPath);

            org.apache.parquet.schema.Type keyType = repeatedKeyValue.getType(0);
            int keyD = type.getMaxDefinitionLevel(path(keyType.getName()));
            org.apache.parquet.schema.Type valueType = repeatedKeyValue.getType(1);
            int valueD = type.getMaxDefinitionLevel(path(valueType.getName()));

            return maps(repeatedD, repeatedR,
                    option(keyType, keyD, keyWriter), option(valueType, valueD, valueWriter));
        }

        @Override
        public ParquetValueWriter<?> primitive(PrimitiveType primitive)
        {
            ColumnDescriptor desc = type.getColumnDescription(currentPath());

            if (primitive.getOriginalType() != null) {
                switch (primitive.getOriginalType()) {
                    case ENUM:
                    case JSON:
                    case UTF8:
                        return strings(desc);
                    case INT_8:
                    case INT_16:
                    case INT_32:
                    case INT_64:
                        return unboxed(desc);
                    default:
                        throw new UnsupportedOperationException(
                                "Unsupported logical type: " + primitive.getOriginalType());
                }
            }
            switch (primitive.getPrimitiveTypeName()) {
                case FIXED_LEN_BYTE_ARRAY:
                    return new FixedWriter(desc);
                case BINARY:
                    return byteBuffers(desc);
                case BOOLEAN:
                case INT32:
                case INT64:
                case FLOAT:
                case DOUBLE:
                    return unboxed(desc);
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

    private static final OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);
    private static final LocalDate EPOCH_DAY = EPOCH.toLocalDate();

    private static class TimestampWriter
            extends ParquetValueWriters.PrimitiveWriter<LocalDateTime>
    {
        private TimestampWriter(ColumnDescriptor desc)
        {
            super(desc);
        }

        @Override
        public void write(int repetitionLevel, LocalDateTime value)
        {
            column.writeLong(repetitionLevel,
                    ChronoUnit.MICROS.between(EPOCH, value.atOffset(ZoneOffset.UTC)));
        }
    }

    private static class TimestamptzWriter
            extends ParquetValueWriters.PrimitiveWriter<OffsetDateTime>
    {
        private TimestamptzWriter(ColumnDescriptor desc)
        {
            super(desc);
        }

        @Override
        public void write(int repetitionLevel, OffsetDateTime value)
        {
            column.writeLong(repetitionLevel, ChronoUnit.MICROS.between(EPOCH, value));
        }
    }

    private static class FixedWriter
            extends ParquetValueWriters.PrimitiveWriter<byte[]>
    {
        private FixedWriter(ColumnDescriptor desc)
        {
            super(desc);
        }

        @Override
        public void write(int repetitionLevel, byte[] value)
        {
            column.writeBinary(repetitionLevel, Binary.fromReusedByteArray(value));
        }
    }
}
