package com.facebook.presto.parquet;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.ColumnarArray;
import com.facebook.presto.spi.block.ColumnarMap;
import com.facebook.presto.spi.block.ColumnarRow;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;

import java.util.List;
import java.util.Optional;

public abstract class DefValueV2
        extends AbstractIterator<Optional<Integer>>
{
    abstract boolean end();

    static DefValueV2 getIterator(Block block, int maxDefinitionLevel)
    {

        return new DefValueV2()
        {
            private int position = -1;

            @Override
            boolean end()
            {
                return true;
            }

            @Override
            protected Optional<Integer> computeNext()
            {
                position++;
                if (position == block.getPositionCount()) {
                    return endOfData();
                }
                if (block.isNull(position)) {
                    return Optional.of(maxDefinitionLevel - 1);
                }
                else {
                    return Optional.of(maxDefinitionLevel);
                }
            }
        };
    }

    static DefValueV2 getIterator(ColumnarRow block, int maxDefinitionLevel)
    {
        return new DefValueV2()
        {
            private int position = -1;

            @Override
            boolean end()
            {
                return true;
            }

            @Override
            protected Optional<Integer> computeNext()
            {
                position++;
                if (position == block.getPositionCount()) {
                    return endOfData();
                }
                if (block.isNull(position)) {
                    return Optional.of(maxDefinitionLevel - 1);
                }
                else {
                    return Optional.empty();
                }
            }
        };
    }

    static DefValueV2 getIterator(ColumnarMap block, int maxDefinitionLevel)
    {
        return new DefValueV2()
        {
            private int position = -1;

            @Override
            boolean end()
            {
                return true;
            }

            @Override
            protected Optional<Integer> computeNext()
            {
                position++;
                if (position == block.getPositionCount()) {
                    return endOfData();
                }
                if (block.isNull(position)) {
                    return Optional.of(maxDefinitionLevel - 1);
                }
                else {
                    return Optional.empty();
                }
            }
        };
    }

    static DefValueV2 getIterator(ColumnarArray block, int maxDefinitionLevel)
    {
        return new DefValueV2()
        {
            private int position = -1;
            private FixedValueIterator iterator;

            @Override
            boolean end()
            {
                return iterator == null || !iterator.hasNext();
            }

            @Override
            protected Optional<Integer> computeNext()
            {
                if (iterator != null && iterator.hasNext()) {
                    return iterator.next();
                }
                position++;
                if (position == block.getPositionCount()) {
                    return endOfData();
                }
                if (block.isNull(position)) {
                    return Optional.of(maxDefinitionLevel - 1);
                }
                else {
                    iterator = new FixedValueIterator(block.getLength(position));
                    Preconditions.checkArgument(iterator.hasNext(), "Not support empty array yet");
                    return iterator.next();
                }
            }
        };
    }

    private static class FixedValueIterator
            extends AbstractIterator<Optional<Integer>>
    {
        private final int length;
        private int position = -1;

        FixedValueIterator(int length)
        {
            this.length = length;
        }

        @Override
        protected Optional<Integer> computeNext()
        {
            position++;
            if (position < length) {
                return Optional.empty();
            }
            return endOfData();
        }
    }

    public static class DefDefIterator
            extends AbstractIterator<Integer>
    {
        private final List<DefValueV2> defValueV2List;
        private int iteratorIndex;

        DefDefIterator(List<DefValueV2> defValueV2List)
        {
            this.defValueV2List = defValueV2List;
        }

        @Override
        protected Integer computeNext()
        {
            DefValueV2 current = defValueV2List.get(iteratorIndex);
            while (iteratorIndex > 0 && current.end()) {
                iteratorIndex--;
                current = defValueV2List.get(iteratorIndex);
            }

            while (current.hasNext()) {
                Optional<Integer> next = current.next();
                if (next.isPresent()) {
                    return next.get();
                }
                else {
                    iteratorIndex++;
                    current = defValueV2List.get(iteratorIndex);
                }
            }
            Preconditions.checkArgument(defValueV2List.stream().noneMatch(AbstractIterator::hasNext));
            return endOfData();
        }
    }
}
