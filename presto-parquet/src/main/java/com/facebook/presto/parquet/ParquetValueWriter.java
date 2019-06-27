package com.facebook.presto.parquet;

import org.apache.parquet.column.ColumnWriteStore;

import java.util.List;

public interface ParquetValueWriter<T> {
    void write(int repetitionLevel, T value);

    List<TripleWriter<?>> columns();

    void setColumnStore(ColumnWriteStore columnStore);
}
