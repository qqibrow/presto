package com.facebook.presto.parquet;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.ColumnarArray;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.Iterator;
import java.util.List;

public interface RepetitionValueIterator
{
    Iterator<RepetitionValue> iterator();

    class RepetitionValue
    {
        private final int value;
        private final boolean isNull;

        RepetitionValue(int value, boolean isNull)
        {
            this.value = value;
            this.isNull = isNull;
        }

        boolean isNull()
        {
            return isNull;
        }

        int getValue()
        {
            return this.value;
        }
    }

    static RepetitionValue nullValue(int value)
    {
        return new RepetitionValue(value, true);
    }

    static RepetitionValue nonNullValue(int value)
    {
        return new RepetitionValue(value, false);
    }

    class ArrayIterator
            implements RepetitionValueIterator
    {
        private final List<RepetitionValue> list;

        ArrayIterator(ColumnarArray columnarArray)
        {
            ImmutableList.Builder<RepetitionValue> builder = ImmutableList.builder();

            for (int i = 0; i < columnarArray.getPositionCount(); i++) {
                if (columnarArray.isNull(i)) {
                    builder.add(nullValue(0));
                }
                else {
                    Preconditions.checkArgument(columnarArray.getLength(i) > 0);
                    for (int j = 0; j < columnarArray.getLength(i); ++j) {
                        if (j == 0) {
                            builder.add(nonNullValue(0));
                        }
                        else {
                            builder.add(nonNullValue(1));
                        }
                    }
                }
            }
            this.list = builder.build();
        }

        ArrayIterator(Iterator<RepetitionValue> parent, ColumnarArray columnarArray, int maxRepititionLevel)
        {
            ImmutableList.Builder<RepetitionValue> builder = ImmutableList.builder();
            int index = 0;
            while (parent.hasNext()) {
                RepetitionValue next = parent.next();
                if (next.isNull()) {
                    builder.add(next);
                }
                else {
                    if (columnarArray.isNull(index)) {
                        builder.add(nullValue(next.getValue()));
                    }
                    else {
                        Preconditions.checkArgument(columnarArray.getLength(index) > 0);
                        for (int j = 0; j < columnarArray.getLength(index); ++j) {
                            if (j == 0) {
                                builder.add(nonNullValue(next.getValue()));
                            }
                            else {
                                builder.add(nonNullValue(maxRepititionLevel));
                            }
                        }
                    }
                    index++;
                }
            }
            Preconditions.checkArgument(index == columnarArray.getPositionCount(), "index is not length of columnarArray");
            this.list = builder.build();
        }

        @Override
        public Iterator<RepetitionValue> iterator()
        {
            return list.iterator();
        }
    }

    static class BlockIterator
        implements RepetitionValueIterator
    {
        private final List<RepetitionValue> list;

        BlockIterator(Block nullCheckBlock)
        {
            ImmutableList.Builder<RepetitionValue> builder = ImmutableList.builder();
            for (int i = 0; i < nullCheckBlock.getPositionCount(); i++) {
                if (nullCheckBlock.isNull(i)) {
                    builder.add(nullValue(0));
                }
                else {
                    builder.add(nonNullValue(0));
                }
            }
            this.list = builder.build();
        }

        BlockIterator(Iterator<RepetitionValue> parent, Block nullCheckBlock)
        {
            ImmutableList.Builder<RepetitionValue> builder = ImmutableList.builder();
            int index = 0;
            while (parent.hasNext()) {
                RepetitionValue next = parent.next();
                if (next.isNull()) {
                    builder.add(next);
                }
                else {
                    if (nullCheckBlock.isNull(index)) {
                        builder.add(nullValue(next.getValue()));
                    }
                    else {
                        builder.add(next);
                    }
                    index++;
                }
            }
            Preconditions.checkArgument(index == nullCheckBlock.getPositionCount(), "index is not length of block");
            this.list = builder.build();
        }

        @Override
        public Iterator<RepetitionValue> iterator()
        {
            return list.iterator();
        }
    }
}
