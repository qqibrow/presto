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
import com.google.common.collect.ImmutableList;

import java.util.List;

public class ColumnTrunk
{
    private final Block block;
    private final List<DefValueV2> list;
    private final List<RepValueV2> repValueV2List;

    ColumnTrunk(Block block)
    {
        this.block = block;
        this.list = ImmutableList.of();
        this.repValueV2List = ImmutableList.of();
    }

    List<DefValueV2> getDefList()
    {
        return list;
    }

    List<RepValueV2> getRepValueV2List()
    {
        return repValueV2List;
    }

    ColumnTrunk(Block block, List<DefValueV2> defValueV2List, List<RepValueV2> repValueV2List)
    {
        this.block = block;
        this.list = defValueV2List;
        this.repValueV2List = repValueV2List;
    }

    public Block getBlock()
    {
        return block;
    }
}
