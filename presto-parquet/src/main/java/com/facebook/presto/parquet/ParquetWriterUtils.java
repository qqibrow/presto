package com.facebook.presto.parquet;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.CharType;
import com.facebook.presto.spi.type.VarcharType;
import io.airlift.slice.Slice;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.format.Type;
import org.apache.parquet.io.api.Binary;

import java.util.function.BiConsumer;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static java.lang.String.format;

public final class ParquetWriterUtils
{
    private ParquetWriterUtils()
    {
    }

    public static BiConsumer<Block, Integer> getWriter(com.facebook.presto.spi.type.Type type, ValuesWriter valuesWriter)
    {
        if (BOOLEAN.equals(type)) {
            throw new PrestoException(NOT_SUPPORTED, format("Unsupported type in parquet writer: %s", type));
        }
        if (INTEGER.equals(type)) {
            return (block, i) -> valuesWriter.writeInteger((int) type.getLong(block, i));
        }
        if (BIGINT.equals(type)) {
            return (block, i) -> valuesWriter.writeLong(type.getLong(block, i));
        }
        if (DOUBLE.equals(type)) {
            return (block, i) -> valuesWriter.writeDouble(type.getDouble(block, i));
        }
        if (type instanceof VarcharType || type instanceof CharType) {
            return (block, i) -> {
                Slice slice = type.getSlice(block, i);
                valuesWriter.writeBytes(Binary.fromConstantByteArray(slice.getBytes()));
            };
        }
//        if (DATE.equals(type)) {
//            return Type.
//        }
//        if (TIMESTAMP.equals(type)) {
//            return Type.INT96;
//        }
//        if (type instanceof DecimalType) {
//            DecimalType decimalType = (DecimalType) type;
//            return ImmutableList.of(new OrcType(OrcTypeKind.DECIMAL, decimalType.getPrecision(), decimalType.getScale()));
//        }
        throw new PrestoException(NOT_SUPPORTED, format("Unsupported type in parquet writer: %s", type));
    }


    public static Type getParquetType(com.facebook.presto.spi.type.Type type)
    {
        if (BOOLEAN.equals(type)) {
            return Type.BOOLEAN;
        }
        if (INTEGER.equals(type)) {
            return Type.INT32;
        }
        if (BIGINT.equals(type)) {
            return Type.INT64;
        }
        if (DOUBLE.equals(type)) {
            return Type.DOUBLE;
        }
//        if (REAL.equals(type)) {
//            return ImmutableList.of(new OrcType(OrcTypeKind.FLOAT));
//        }
        if (type instanceof VarcharType || type instanceof CharType) {
            return Type.BYTE_ARRAY;
        }
//        if (DATE.equals(type)) {
//            return Type.
//        }
        if (TIMESTAMP.equals(type)) {
            return Type.INT96;
        }
//        if (type instanceof DecimalType) {
//            DecimalType decimalType = (DecimalType) type;
//            return ImmutableList.of(new OrcType(OrcTypeKind.DECIMAL, decimalType.getPrecision(), decimalType.getScale()));
//        }
        throw new PrestoException(NOT_SUPPORTED, format("Unsupported type in parquet writer: %s", type));
    }

//    public static ConvertedType getConvertedType(com.facebook.presto.spi.type.Type type)
//    {
//        if (BOOLEAN.equals(type)) {
//            return ConvertedType.ENUM;
//        }
//        if (INTEGER.equals(type)) {
//            return ConvertedType.INT_32;
//        }
//        if (BIGINT.equals(type)) {
//            return ConvertedType.INT_64;
//        }
//        if (DOUBLE.equals(type)) {
//            return ConvertedType.
//        }
////        if (REAL.equals(type)) {
////            return ImmutableList.of(new OrcType(OrcTypeKind.FLOAT));
////        }
//        if (type instanceof VarcharType || type instanceof CharType) {
//            return Type.BYTE_ARRAY;
//        }
////        if (DATE.equals(type)) {
////            return Type.
////        }
//        if (TIMESTAMP.equals(type)) {
//            return Type.INT96;
//        }
////        if (type instanceof DecimalType) {
////            DecimalType decimalType = (DecimalType) type;
////            return ImmutableList.of(new OrcType(OrcTypeKind.DECIMAL, decimalType.getPrecision(), decimalType.getScale()));
////        }
//        throw new PrestoException(NOT_SUPPORTED, format("Unsupported type in parquet writer: %s", type));
//
//    }
}
