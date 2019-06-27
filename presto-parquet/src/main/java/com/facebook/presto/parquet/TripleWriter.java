package com.facebook.presto.parquet;

import org.apache.parquet.io.api.Binary;

public interface TripleWriter<T> {
    // TODO: should definition level be included, or should it be part of the column?

    /**
     * Write a value.
     *
     * @param rl repetition level
     * @param value the value
     */
    void write(int rl, T value);

    /**
     * Write a triple.
     *
     * @param rl repetition level
     * @param value the boolean value
     */
    default void writeBoolean(int rl, boolean value) {
        throw new UnsupportedOperationException("Not a boolean column");
    }

    /**
     * Write a triple.
     *
     * @param rl repetition level
     * @param value the boolean value
     */
    default void writeInteger(int rl, int value) {
        throw new UnsupportedOperationException("Not an integer column");
    }

    /**
     * Write a triple.
     *
     * @param rl repetition level
     * @param value the boolean value
     */
    default void writeLong(int rl, long value) {
        throw new UnsupportedOperationException("Not an long column");
    }

    /**
     * Write a triple.
     *
     * @param rl repetition level
     * @param value the boolean value
     */
    default void writeFloat(int rl, float value) {
        throw new UnsupportedOperationException("Not an float column");
    }

    /**
     * Write a triple.
     *
     * @param rl repetition level
     * @param value the boolean value
     */
    default void writeDouble(int rl, double value) {
        throw new UnsupportedOperationException("Not an double column");
    }

    /**
     * Write a triple.
     *
     * @param rl repetition level
     * @param value the boolean value
     */
    default void writeBinary(int rl, Binary value) {
        throw new UnsupportedOperationException("Not an binary column");
    }

    /**
     * Write a triple for a null value.
     *
     * @param rl repetition level
     * @param dl definition level
     */
    void writeNull(int rl, int dl);
}
