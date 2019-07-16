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

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.CharType;
import com.facebook.presto.spi.type.RealType;
import com.facebook.presto.spi.type.VarbinaryType;
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
import static java.lang.Float.intBitsToFloat;
import static java.lang.String.format;

public final class ParquetWriterUtils
{
    private ParquetWriterUtils()
    {
    }

    public static BiConsumer<Block, Integer> getWriter(com.facebook.presto.spi.type.Type type, ValuesWriter valuesWriter)
    {
        if (BOOLEAN.equals(type)) {
            return (block, i) -> valuesWriter.writeBoolean(type.getBoolean(block, i));
        }
        if (INTEGER.equals(type)) {
            return (block, i) -> valuesWriter.writeInteger((int) type.getLong(block, i));
        }
        if (BIGINT.equals(type) || TIMESTAMP.equals(type)) {
            return (block, i) -> valuesWriter.writeLong(type.getLong(block, i));
        }
        if (DOUBLE.equals(type)) {
            return (block, i) -> valuesWriter.writeDouble(type.getDouble(block, i));
        }
        if (RealType.REAL.equals(type)) {
            return (block, i) -> {
                int intBits = (int) type.getLong(block, i);
                float value = intBitsToFloat(intBits);
                valuesWriter.writeFloat(value);
            };
        }
        if (type instanceof VarcharType || type instanceof CharType || type instanceof VarbinaryType) {
            return (block, i) -> {
                Slice slice = type.getSlice(block, i);
                valuesWriter.writeBytes(Binary.fromConstantByteBuffer(slice.toByteBuffer()));
            };
        }
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
