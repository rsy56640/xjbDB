# xjbDB：一个xjb 写的 DB

- [数据类型的限制](#1)
- [Pager](#2)
- [Tree](#3)
- [VM](#4)
- []()

&nbsp;   
<a id="1"></a>
## 数据类型的限制

- 支持的数据类型：int, char, varchar（字符串有效长度 <= **57**B）
- 一个 tuple 的长度 <= 67B
  - INT 4B
  - VARCHAR(n)  始终占 n B，如果少，用 `'\0'` 填充
  - 例如：INT, VARCHAR(15), INT, CHAR(20) 占 4+15+4+20 B


&nbsp;   
<a id="2"></a>
## Pager

文件名：file.xjbDB   
日志名：file.xjbDB.log

### Internal Page
- (4B) page_t
- (4B) page_id
- (4B) parent_page_id
- (4B) nEntry 有几个 key
- (2B) key_t `INT` 或 `(VAR)CHAR`
- (2B) str_len 如果 key_t 为 `(VAR)CHAR`，指定长度
- (4B * (2nEntry+1))
  - (4B) 孩子 page_id
  - (4B) key `INT` 或 `(VAR)CHAR`
  - ...
  - ...
  - (4B) 孩子 page_id
- key 字符串（从152B开始，每58B一个block）

支持的基本操作：

- 插入 key：找到一个 `OBSOLETE` 的 record，然后写入
- 删除 key：将 `index` 所对应的 record 标记为 `OBSOLETE`

### Value Page（只存15个 record，其他 metadata 不存，生命周期绑定到 Leaf Page）
- (4B) page_t
- (4B) page_id
- (4B) parent_page_id
- (4B) nEntry 有几项
- 15 个 record

record 格式为：`mark(1B), content(<=67B)`   
把 1024B 分成 15个 68B，这样每个68B是一个block，管理起来很方便。   

支持的基本操作：

- 插入：遍历这15个block，找到一个 `OBSOLETE`（即0）
- 删除：标记这个block为 `OBSOLETE`

### Leaf Page
- (4B) page_t
- (4B) page_id
- (4B) parent_page_id
- (4B) nEntry 有几个 k-v
- (2B) key_t `INT` 或 `(VAR)CHAR`
- (2B) str_len 如果 key_t 为 `(VAR)CHAR`，指定长度
- (4B) value_page_id
- (4B) previous_page_id
- (4B) next_page_id
- (8B * nEntry) nEntry 个 k-v
  - (4B) key `INT` 或 `(VAR)CHAR`
  - (4B) value-index，指向 value_page_id 中该内容的 index（index*68 即可得到 Value Page 中的 offset）
- key 字符串（从152B开始，每58B一个block）

支持的基本操作：

- 插入 value：调用 `value_page->insert()`
- 删除 value：调用 `value_page->erase()`


### BufferPoolManager

- 管理所有 Page，**除了 ValuePage**，ValuePage 总是和 LeafPage 生命周期相同。


#### Concurrent Hash + LRU

结合了 hash 与 lru，结点构成：`page_id`, `Page*` 和 4个指针：`prev_hash`, `next_hash`, `prev_lru`, `next_lru`   
每个 hash bucket 中都是一个双向链表；整个 LRU 也是一个双向链表。

![](assets/hash_lru.png)

如果不需要支持多线程，那么下面这个结构就可以：其中 lru 持有真正的数据内容，hash 加快索引。

```cpp
std::list<pair<Key, Value>> lru;
std::unordered_map<Key, list<pair<Key, Value>>::iterator> hash;
```

需要多线程时，让两者其中某一方持有数据都不合适，于是就让它俩是一个结点了。（参考 levelDB/util/cache.cc LRUCache）

- find
  - 在 hash bucket 里面找，这里把结点 ref 一下，防止 evict
  - 进了 lru，unref 结点，然后 update lru（即把结点放到开头）
- insert
  - 添加进 bucket
  - 再添加进 lru
- erase
  - 把结点 ref 一下，**防止 evict 和 erase 同时搞一个结点**，然后从 bucket 中移除
  - 进入 lru
      - 如果已经被 evict，那么只 unref 结点（不 `size--`，冲突时 evict 负责 `size--`）
      - 否则正常删除，unref 结点，并 `size--`
- evict（private）
  - `size--`
  - 把结点 ref 一下，然后从 lru 中删除
  - 进入 bucket
      - 如果已经不在了，只 unref 结点
      - 正常删除，并 unref 结点
- rehash（private）
  - 全局写锁（其他操作都要拿全局读锁）
  - bucket 数量翻倍（注意 `std::deque::resize(size_t)` 不要求 copyable 或 movable 语义）
  - 遍历原来的每一个桶
      - 如果 hash 值不是这个桶，那么把它平移到后面那个桶里去，距离就是原来的 bucket 数量
      - 否则不动


### DiskManager



### LogManager




&nbsp;   
<a id="3"></a>
## Tree

让 VM 以 table 为视角处理，向下管理 page，使用 B+tree。

注意：

- 并发 B+，leaf chain
- 日志
- table 元信息，checksum，magic num


BTreeDegree = 8，所以 nEntry 在 [7, 15]

如果 key 是字符串，key记录offset   
keystr强制 <= 57B，于是从 152B开始，每58B一个block   
格式为：`mark(1B), content(<=57B)`   

关于 Page 的引用计数，除了 root 一直保留，其他 page 用完就 `unref()`。


### B+树的实现
一开始被 sqlite3 骗了，《SQLite Database System Design and Implementation》上说 root 只能是 internal，写着写着愈发觉得诡异，搞得我快自闭了。直到看了《Fundamentals of Database Systems (7th edition)》 $17.3.2，才发现 root 一开始就是 leaf。然后我自己设计了一套B+树算法，应该是正确的：)

#### B+树的结构

总共有2种类型的结点：internal 和 leaf   
**注意：root 结点既可以是 internal，也可以是 leaf**   
每个结点的 `nEntry` 都在 [7, 15]

![](assets/BT_page_t.png)

##### internal 结点
key 数量记为 `nEntry`，branch 数量为 `nEntry+1`

![](assets/BT_internal.png)

它们 key 的关系是：（`branch[i]` 表示其中的所有 key 值）   
`branch[0] <= key[0] < branch[1] <= ... <= key[4] < branch[5]`

之后我们称 **确定 index**，是指找到第一个 index，使得 `kEntry <= node.key[index]`（index 允许是 `node.nEntry`）

##### leaf 结点

![](assets/BT_leaf.png)


#### serch 操作
- 对于 internal 结点
  - 确定 index
  - 从 `node.branch[index]` 递归往下走
- 对于 leaf 结点
  - 直接找

#### insert 操作

- 从 root 进入
  - 如果 root 满，那么先 split
  - 确定 index
  - a) 如果 root 是 leaf
      - 检查冲突并尝试插入，插入前其他kv向后移动
  - b) 如果 root 是 internal
      - 如果 `node = root.branch[index]` 满，那么 split，注意 split 之后有可能调整 index（+1）
      - 调用 `INSERT_NONFULLINSERT_NONFULL(node)` 向下

<a></a>

&nbsp;   

`INSERT_NONFULLINSERT_NONFULL(node)` 要求 node 是 **非满的** 并且 **非root**

- a) 如果 node 是 leaf
  - 确定 index，检查冲突并尝试插入，插入前其他kv向后移动
- b) 如果 node 是 internal
  - 确定 index
  - 如果 `child = node.branch[index]` 满，那么 split，注意 split 之后有可能调整 index（+1）
  - 调用 `INSERT_NONFULLINSERT_NONFULL(child)` 向下

> node 非满的性质是在 `split(child)` 时用到的

&nbsp;   

split 分为 `split_internal` 和 `split_leaf`，要求 node 是 **非满的**

- `split_internal(node, index, L)`，其中 `L = node.branch[index]`
  - 创建一个 internal 结点，称作 `R`
  - 把 `L.key[8..14]` 移到 `R.key[0..6]`，`L.branch[8..15]` 移到 `R.branch[0..7]`
  - 更新 parent_id
  - 将 `node.key[index..]` 和 `node.branch[index+1..]` 右移
  - 将 `L.key[7]`（删除） 向上提到 `node.key[index]`，并设置 `node.branch[index+1] = R`
- `split_leaf(node, index, L)`，其中 `L = node.branch[index]`
  - 创建一个 leaf 结点，称作 `R`
  - 把 `L.key-value[8..14]` 移到 `R.key-value[0..6]`
  - 调整 leaf-chain
  - 将 `node.key[index..]` 和 `node.branch[index+1..]` 右移
  - 设置 `node.key[index] = L.key[7]`，`node.branch[index+1] = R`

#### delete 操作



#### 其他注意事项

每次要维护的一些数据成员:

- BTreePage
  - `nEntry_`
  - `keys_[0..14]`
  - `parent_id_`
- InternalPage
  - `branch_[0..15]`
- LeafPage
  - `values_[0..14]`
  - `left_` and `right_`

<a></a>

&nbsp;   

- steal branch 时要注意
  - set node[index] = max_KeyEntry(..)
  - update parent_id


&nbsp;   
<a id="4"></a>
## VM

