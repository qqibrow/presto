package com.facebook.presto.parquet;

import com.facebook.presto.spi.block.Block;

import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;

public class ColumnTrunk
{
    private final Block block;
    private final Iterator<Optional<Integer>> defIterator;
    private final Iterator<RepetitionValueIterator.RepetitionValue> repIterator;

    ColumnTrunk(Block block)
    {
        this.block = block;
        this.defIterator = Collections.emptyIterator();
        this.repIterator = Collections.emptyIterator();
    }

    ColumnTrunk(Block block, Iterator<Optional<Integer>> defIterator, Iterator<RepetitionValueIterator.RepetitionValue> repIterator)
    {
        this.block = block;
        this.defIterator = Collections.emptyIterator();
        this.repIterator = Collections.emptyIterator();
    }

    public Block getBlock()
    {
        return block;
    }

    public Iterator<Optional<Integer>> getDefIterator()
    {
        return defIterator;
    }

    public Iterator<RepetitionValueIterator.RepetitionValue> getRepIterator()
    {
        return repIterator;
    }
}
