# xjbDB


支持的数据类型：int, char, varchar（字符串有效长度 <= **57**B）


## Pager

文件名：file.xjbDB   
日志名：file.xjbDB.log

### Hash LRU

### DiskManager

### LogManager



## Tree

让 VM 以 table 为视角处理，向下管理 page，使用 B+tree。

注意：

- 并发 B+，leaf chain
- 日志
- table 元信息，checksum，magic num


BTreeDegree = 8，所以 nEntry 在 [7, 15]

**BTree Page 中有一个在内存中的数据 `last_offset_`，并不落盘，在每次读 page 的时候 遍历所有 key，取最小的。**

### Internal Page
- (4B) page_t
- (4B) page_id
- (4B) parent_page_id
- (4B) nEntry 有几个 key
- (4B) key_t `INT` 或 `(VAR)CHAR`
- (4B) str_len 如果 key_t 为 `(VAR)CHAR`，指定长度
- (4B * (2nEntry + 1))
  - (4B) 孩子 page_id
  - (4B) key `INT` 或 `(VAR)CHAR`
  - ...
  - ...
  - (4B) 孩子 page_id
- key 字符串

### Value Page
- (4B) page_t
- (4B) page_id
- (4B) parent_page_id
- (4B) nEntry 有几项
- (4B) cur 下一个写的位置
- nEntry 个 record

record 格式为：`mark(1B), content(<=57B), '\0'(1B)`

### Leaf Page
- (4B) page_t
- (4B) page_id
- (4B) parent_page_id
- (4B) nEntry 有几个 k-v
- (4B) key_t `INT` 或 `(VAR)CHAR`
- (4B) str_len 如果 key_t 为 `(VAR)CHAR`，指定长度
- (4B) value_page_id
- (8B * nEntry) nEntry 个 k-v
  - (4B) key `INT` 或 `(VAR)CHAR`
  - (4B) value-offset，指向 value_page_id 中该内容的 mark
- key 字符串


## VM

