package com.facebook.presto.parquet;

import org.apache.parquet.format.Type;
import org.apache.parquet.schema.PrimitiveType;

// Copy from
public class ParquetTypeConverter
{
    private ParquetTypeConverter() {}

    public static org.apache.parquet.format.Type getType(PrimitiveType.PrimitiveTypeName type)
    {
        switch (type) {
            case INT64:
                return Type.INT64;
            case INT32:
                return Type.INT32;
            case BOOLEAN:
                return Type.BOOLEAN;
            case BINARY:
                return Type.BYTE_ARRAY;
            case FLOAT:
                return Type.FLOAT;
            case DOUBLE:
                return Type.DOUBLE;
            case INT96:
                return Type.INT96;
            case FIXED_LEN_BYTE_ARRAY:
                return Type.FIXED_LEN_BYTE_ARRAY;
            default:
                throw new RuntimeException("Unknown primitive type " + type);
        }
    }
}
