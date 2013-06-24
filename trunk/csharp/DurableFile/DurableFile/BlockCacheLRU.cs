/* Copyright (c) 2013 Johnny Azzi.
 *
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
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DurableFile
{
    class BlockCacheLRU
    {
        private int _capacity;

        private LinkedList<BlockCacheItem> _lruItemList = new LinkedList<BlockCacheItem>();

        private Dictionary<long, LinkedListNode<BlockCacheItem>> _itemCacheDict = new Dictionary<long, LinkedListNode<BlockCacheItem>>();

        public BlockCacheLRU(int capacity)
        {
            _capacity = capacity;
        }

        public BlockCacheItem Get(long blockNo)
        {
            LinkedListNode<BlockCacheItem> node;

            if (_itemCacheDict.TryGetValue(blockNo, out node))
            {
                _lruItemList.Remove(node);
                _lruItemList.AddLast(node);
                node.Value.timestamp = DateTime.Now;
                return node.Value;
            }

            return null;
        }

        /// <summary>
        /// Unpin block, means the block can be removed from cache
        /// because the modified block was persisted on disk
        /// </summary>
        /// <param name="blockNo"></param>
        /// <returns></returns>
        public bool UnpinBlock(long blockNo)
        {
            LinkedListNode<BlockCacheItem> node;

            if (_itemCacheDict.TryGetValue(blockNo, out node))
            {
                node.Value.pinned = false;
                return true;
            }

            return false;
        }

        public void Add(long blockNo, byte[] block, int count, bool pinned)
        {
            if (_itemCacheDict.Count >= _capacity)
            {
                RemoveFirst(_itemCacheDict.Count - _capacity + 1);
            }

            BlockCacheItem cacheItem = new BlockCacheItem(blockNo, block, count, pinned);
            cacheItem.timestamp = DateTime.Now;

            LinkedListNode<BlockCacheItem> node = new LinkedListNode<BlockCacheItem>(cacheItem);

            if (!_itemCacheDict.ContainsKey(blockNo))
            {
                _lruItemList.AddLast(node);
                _itemCacheDict.Add(blockNo, node);
            }
            else
            {
                _itemCacheDict[blockNo].Value = cacheItem;
            }
        }

        public void Remove(long blockNo)
        {
            LinkedListNode<BlockCacheItem> node;

            if (_itemCacheDict.TryGetValue(blockNo, out node))
            {
                _lruItemList.Remove(node);
                _itemCacheDict.Remove(blockNo);
            }
        }

        /// <summary>
        /// Remove at least one unpinned cache blocks
        /// Remove 10 unpinned cache blocks of timestamp of at least 10 minutes old
        /// </summary>
        protected void RemoveFirst(int count)
        {
            LinkedListNode<BlockCacheItem> node = _lruItemList.First;

            int removedCount = 0;

            while (node != null)
            {
                LinkedListNode<BlockCacheItem> nextNode = node.Next;

                if (!node.Value.pinned)
                {
                    _lruItemList.Remove(node.Value);
                    _itemCacheDict.Remove(node.Value.blockNo);
                    removedCount++;
                    if (removedCount >= count + 10)
                        break;
                }

                if ((removedCount >= count) && (node.Value.timestamp.AddMinutes(10) <= DateTime.Now))
                {
                    break;
                }

                node = nextNode;
            }
        }
    }

    class BlockCacheItem
    {
        public long blockNo;
        public byte[] block;
        public int count;
        public DateTime timestamp;

        /// <summary>
        /// if pinned == true then the block cannot be written back to the disk yet
        /// </summary>
        public bool pinned = false;

        public BlockCacheItem(long blockNo, byte[] block, int count, bool pinned)
        {
            this.blockNo = blockNo;
            this.block = block;
            this.count = count;
            this.pinned = pinned;
        }
    }
}
