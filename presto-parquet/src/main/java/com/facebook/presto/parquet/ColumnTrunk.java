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

import com.facebook.presto.spi.block.Block;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

public class ColumnTrunk
{
    private final Block block;
    private final DefinitionValueIterator defIterator;
    private final RepetitionValueIterator repIterator;
    private final List<DefValueV2> list;

    ColumnTrunk(Block block)
    {
        this.block = block;
        this.defIterator = null;
        this.repIterator = null;
        this.list = null;
    }

    List<DefValueV2> getList()
    {
        return list;
    }

    ColumnTrunk(Block block, DefinitionValueIterator defIterator, RepetitionValueIterator repIterator, List<DefValueV2> defValueV2List)
    {
        this.block = block;
        this.defIterator = defIterator;
        this.repIterator = repIterator;
        this.list = defValueV2List;
    }

    public Block getBlock()
    {
        return block;
    }

    public Iterator<Optional<Integer>> getDefIterator()
    {
        return defIterator == null ? Collections.emptyIterator() : defIterator.iterator();
    }

    public Iterator<RepetitionValueIterator.RepetitionValue> getRepIterator()
    {
        return repIterator == null ? Collections.emptyIterator() : repIterator.iterator();
    }
}
