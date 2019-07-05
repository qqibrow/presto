package com.facebook.presto.parquet;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.ColumnarArray;
import com.facebook.presto.spi.block.ColumnarRow;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;

import java.util.Iterator;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class NestedBlockIterator
{
    static class PrimitiveIterator
            extends AbstractIterator<Optional<Integer>>
    {
        private final Iterator<Optional<Integer>> iterator;

        PrimitiveIterator(Block block, int maxDefinitionLevel)
        {
            ImmutableList.Builder<Optional<Integer>> builder = ImmutableList.builder();
            for (int i = 0; i < block.getPositionCount(); i++) {
                if (block.isNull(i)) {
                    builder.add(Optional.of(maxDefinitionLevel - 1));
                }
                else {
                    builder.add(Optional.of(maxDefinitionLevel));
                }
            }
            this.iterator = builder.build().iterator();
        }

        PrimitiveIterator(Iterator<Optional<Integer>> parent, Block block, int maxDefinitionLevel)
        {
            ImmutableList.Builder<Optional<Integer>> builder = ImmutableList.builder();
            int index = 0;
            while (parent.hasNext()) {
                Optional<Integer> next = parent.next();
                if (next.isPresent()) {
                    builder.add(next);
                }
                else {
                    if (block.isNull(index)) {
                        builder.add(Optional.of(maxDefinitionLevel - 1));
                    }
                    else {
                        builder.add(Optional.of(maxDefinitionLevel));
                    }
                    index++;
                }
            }
            Preconditions.checkArgument(index == block.getPositionCount(), "index is not length of block");
            this.iterator = builder.build().iterator();
        }

        @Override
        protected Optional<Integer> computeNext()
        {
            if (iterator.hasNext()) {
                return iterator.next();
            }
            return endOfData();
        }
    }

    static class RowIterator
            extends AbstractIterator<Optional<Integer>>
    {
        private final ColumnarRow columnarRow;
        private final int maxDefinitionLevel;
        private final Iterator<Optional<Integer>> iterator;

        RowIterator(ColumnarRow columnarRow, int maxDefinitionLevel)
        {
            this.columnarRow = requireNonNull(columnarRow, "columnarRow is null");
            this.maxDefinitionLevel = maxDefinitionLevel;

            ImmutableList.Builder<Optional<Integer>> builder = ImmutableList.builder();
            for (int i = 0; i < columnarRow.getPositionCount(); i++) {
                if (columnarRow.isNull(i)) {
                    builder.add(Optional.of(maxDefinitionLevel - 1));
                }
                else {
                    builder.add(Optional.empty());
                }
            }
            this.iterator = builder.build().iterator();
        }

        RowIterator(Iterator<Optional<Integer>> parent, ColumnarRow columnarRow, int maxDefinitionLevel)
        {
            this.columnarRow = requireNonNull(columnarRow, "columnarRow is null");
            this.maxDefinitionLevel = maxDefinitionLevel;

            ImmutableList.Builder<Optional<Integer>> builder = ImmutableList.builder();
            int index = 0;
            while (parent.hasNext()) {
                Optional<Integer> next = parent.next();
                if (next.isPresent()) {
                    builder.add(next);
                }
                else {
                    Optional<Integer> def = columnarRow.isNull(index) ? Optional.of(maxDefinitionLevel - 1) : Optional.empty();
                    index++;
                    builder.add(def);
                }
            }
            Preconditions.checkArgument(index == columnarRow.getPositionCount(), "index is not length of columnarRow");
            this.iterator = builder.build().iterator();
        }

        @Override
        protected Optional<Integer> computeNext()
        {
            if (iterator.hasNext()) {
                return iterator.next();
            }
            return endOfData();
        }
    }

    static class ArrayIterator
            extends AbstractIterator<Optional<Integer>>
    {
        private final ColumnarArray columnarArray;
        private final int maxDefinitionLevel;
        private Iterator<Optional<Integer>> iterator;

        ArrayIterator(ColumnarArray columnarArray, int maxDefinitionLevel)
        {
            this.columnarArray = requireNonNull(columnarArray, "columnarArray is null");
            this.maxDefinitionLevel = maxDefinitionLevel;

            ImmutableList.Builder<Optional<Integer>> builder = ImmutableList.builder();
            for (int i = 0; i < columnarArray.getPositionCount(); ++i) {
                if (columnarArray.isNull(i)) {
                    builder.add(Optional.of(maxDefinitionLevel - 1));
                }
                else {
                    Preconditions.checkArgument(columnarArray.getLength(i) > 0);
                    for (int j = 0; j < columnarArray.getLength(i); ++j) {
                        builder.add(Optional.empty());
                    }
                }
            }
            iterator = builder.build().iterator();
        }

        ArrayIterator(Iterator<Optional<Integer>> parent, ColumnarArray columnarArray, int maxDefinitionLevel)
        {
            this.columnarArray = requireNonNull(columnarArray, "columnarArray is null");
            this.maxDefinitionLevel = maxDefinitionLevel;

            ImmutableList.Builder<Optional<Integer>> builder = ImmutableList.builder();
            int index = 0;
            while (parent.hasNext()) {
                Optional<Integer> next = parent.next();
                if (next.isPresent()) {
                    builder.add(next);
                }
                else {
                    if (columnarArray.isNull(index)) {
                        builder.add(Optional.of(maxDefinitionLevel - 1));
                    }
                    else {
                        Preconditions.checkArgument(columnarArray.getLength(index) > 0);
                        for (int j = 0; j < columnarArray.getLength(index); ++j) {
                            builder.add(Optional.empty());
                        }
                    }
                    index++;
                }
            }
            Preconditions.checkArgument(index == columnarArray.getPositionCount(), "index is not length of columnarArray");
            iterator = builder.build().iterator();
        }

        @Override
        protected Optional<Integer> computeNext()
        {
            if (iterator.hasNext()) {
                return iterator.next();
            }
            return endOfData();
        }
    }
}
