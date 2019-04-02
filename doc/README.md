# xjbDB

## 数据类型的限制

- 支持的数据类型：int, char, varchar（字符串有效长度 <= **57**B）
- 一个 tuple 的长度 <= 67B
  - INT 4B
  - VARCHAR(n)  始终占 n B，如果少，用 `'\0'` 填充
  - 例如：INT, VARCHAR(15), INT, CHAR(20) 占 4+15+4+20 B

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



### DiskManager



### LogManager



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



#### serch



#### insert



#### delete



#### 其他注意事项



## VM

