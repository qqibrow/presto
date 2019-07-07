package com.facebook.presto.parquet;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.ColumnarArray;
import com.facebook.presto.spi.block.ColumnarRow;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;

public interface DefinitionValueIterator
{
    Iterator<Optional<Integer>> iterator();

    class PrimitiveIterator
            implements DefinitionValueIterator
    {
        private final List<Optional<Integer>> list;

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
            this.list = builder.build();
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
            this.list = builder.build();
        }

        @Override
        public Iterator<Optional<Integer>> iterator()
        {
            return list.iterator();
        }
    }

    class RowIterator
            implements DefinitionValueIterator
    {
        private final List<Optional<Integer>> list;

        RowIterator(ColumnarRow columnarRow, int maxDefinitionLevel)
        {
            ImmutableList.Builder<Optional<Integer>> builder = ImmutableList.builder();
            for (int i = 0; i < columnarRow.getPositionCount(); i++) {
                if (columnarRow.isNull(i)) {
                    builder.add(Optional.of(maxDefinitionLevel - 1));
                }
                else {
                    builder.add(Optional.empty());
                }
            }
            this.list = builder.build();
        }

        RowIterator(Iterator<Optional<Integer>> parent, ColumnarRow columnarRow, int maxDefinitionLevel)
        {
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
            this.list = builder.build();
        }

        @Override
        public Iterator<Optional<Integer>> iterator()
        {
            return list.iterator();
        }
    }

    class ArrayIterator
            implements DefinitionValueIterator
    {
        private List<Optional<Integer>> list;

        ArrayIterator(ColumnarArray columnarArray, int maxDefinitionLevel)
        {
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
            list = builder.build();
        }

        ArrayIterator(Iterator<Optional<Integer>> parent, ColumnarArray columnarArray, int maxDefinitionLevel)
        {
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
            list = builder.build();
        }

        @Override
        public Iterator<Optional<Integer>> iterator()
        {
            return list.iterator();
        }
    }
}
