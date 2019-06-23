package com.facebook.presto.parquet;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.CharType;
import com.facebook.presto.spi.type.VarcharType;
import org.apache.parquet.format.Type;

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
