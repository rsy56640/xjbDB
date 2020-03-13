# Query 方案

- [Query Compilation 方案](#query-compile)
  - [](#)
  - [](#)
- [Query Execution 方案](#query-exec)
  - [query execution 面临的问题及决策](#query-exec-decision)
  - [数据流代码框架](#query-exec-code)
  - [Vectorized Hash Join](#query-exec-simdhashjoin)
  - [其他](#query-exec-miscellaneous)
- [代码生成样例](#codegen-example)


&nbsp;   
<a id="query-compile"></a>
## Query Compilation 方案






&nbsp;   
<a id="query-exec"></a>
## Query Execution 方案

<img src="assets/SQL-presentation.png" width=600/>

总体来说不复杂，输入是一系列表，输出是一张表，query-exec 事实上应该提供 “数据流访问基础设施”，“节点功能” 就OK了。

<a id="query-exec-decision"></a>
### query execution 面临的问题及决策

如果选择每个结点间传递 tuple 的话，那其实是非常简单的 pipeline style，但是因为决定要做 simd hash join，所以决定以 block 为单位传递。

遇到了一些困难，尤其是**数据组织形式**，和 **pipeline 控制流**：

- **数据组织形式**
  - row content
      - 不用额外维护一些表示关系，尤其是 join，所以**决定用 row content**
  - row handler
      - join 之后的表示
          - join 输出全部存着
          - 存对应的两个 row handler，并且存对应的 splice 方法
- **pipeline 控制流**
  - Table 输出 block
  - Sigma 输入 block，输出 block
  - Join Build 输入 block
  - Join Probe 输入 block，输出 不定量 tuple
      - 输出以 block 为单位遍历
  - Projection 输入 block，输出 block
  - 一开始想法是 block 要不要沿着 pipeline 一直走，思考另一种方案很久：block 攒够 8 个才继续走。比如说 join probe，需要在右边放一个 buffer 做贮存工作，这样会极大的影响 control flow。比如 table 拿出来8个 tuple，经过 sigma，只剩5个，那么就需要重新去拿。这使得 pipeline 从 stateless 变为 stateful。此外 table 中结束后，还需要检查所有 buffer 是否有剩余，重新走一遍
  - 最好是 codegen 的代码可以分块对应到 query AST，所以我**决定让 block 沿着 pipeline 一直走**

<a id="query-exec-code"></a>
### 数据流代码框架

数据以 block 形式在结点间传递：

- `class ap_table_t;`
  - 提供 `ap_block_iter_t`
- `class join_result_buf_t;`
  - 提供 `ap_block_iter_t`
- `class ap_block_iter_t`
  - 来源于 `class ap_table_t;` 或 `class join_result_buf_t;`
  - 判断是否结束 `bool is_end() const;`
  - 向后消费 block `block_tuple_t consume_block();`
- `class block_tuple_t`
  - 提供 `block_tuple_iter_t` 用于遍历 tuple
  - `VECTOR_INT getINT(page::range_t range) const;`
  - 复杂条件，如数字计算，字符串比较，`block_tuple_iter_t`
- `class hash_table_t;`
  - `void insert(const block_tuple_t&);`
  - `void build();`
  - `join_result_buf_t probe(const block_tuple_t&) const;`
- `struct VECTOR_INT;`
  - 表示 8 个 int32
  - arithmetic 运算
  - logical 运算
  - mask 运算
  - gather

<a id="query-exec-simdhashjoin"></a>
### Vectorized Hash Join

- [Vectorization vs. Compilation in Query Execution 论文阅读笔记](https://github.com/rsy56640/paper-reading/tree/master/%E6%95%B0%E6%8D%AE%E5%BA%93/content/Vectorization%20vs.%20Compilation%20in%20Query%20Execution)
- [Rethinking SIMD Vectorization for In-Memory Databases 论文阅读笔记](https://github.com/rsy56640/paper-reading/tree/master/%E6%95%B0%E6%8D%AE%E5%BA%93/content/Rethinking%20SIMD%20Vectorization%20for%20In-Memory%20Databases)

<a id="query-exec-miscellaneous"></a>
### 其他

- 多线程执行：以 pipeline 为单位


&nbsp;   
<a id="codegen-example"></a>
## 代码生成样例

```c++
//////////////////////////////////////////////////////////////////
//////////////////////  example of codegen  //////////////////////
//////////////////////////////////////////////////////////////////
#include "ap_exec.h"

static
DB::ap::block_tuple_t example_projection(const DB::ap:: block_tuple_t& block) {
    return block;
}

extern "C"
DB::ap::VMEmitOp example_query(const DB::ap::ap_table_array_t& tables) {
    const DB::ap::ap_table_t& T1 = tables.at(1);
    const DB::ap::ap_table_t& T2 = tables.at(2);
    const DB::ap::ap_table_t& T3 = tables.at(3);
    DB::page::range_t rng1{ 0, 4 };
    DB::page::range_t rng2{ 4, 4};
    DB::page::range_t rng3{ 4, 4 };
    DB::page::range_t rng_j2{ 0, 4 };
    DB::ap::hash_table_t ht1(rng1, rng_j2, 8, 8, true);
    DB::ap::hash_table_t ht2(rng2, rng3, 8, 16, false);
    DB::ap::VMEmitOp emit;


    for(DB::ap::ap_block_iter_t it = T1.get_block_iter(); !it.is_end();) {
        DB::ap::block_tuple_t block = it.consume_block();

        block.selectivity_and(block.getINT({ 4, 4 }) > 42);

        ht1.insert(block);
    }
    ht1.build();


    for(DB::ap::ap_block_iter_t it = T2.get_block_iter(); !it.is_end();) {
        DB::ap::block_tuple_t block = it.consume_block();

        block.selectivity_and(block.getINT({ 4, 4 }) < 233);

        ht2.insert(block);
    }
    ht2.build();


    for(DB::ap::ap_block_iter_t it = T3.get_block_iter(); !it.is_end();) {
        DB::ap::block_tuple_t block = it.consume_block();

        DB::ap::join_result_buf_t join_result2 = ht2.probe(block);
        for(DB::ap::ap_block_iter_t it = join_result2.get_block_iter(); !it.is_end();) {
            DB::ap::block_tuple_t block = it.consume_block();

            DB::ap::join_result_buf_t join_result1 = ht1.probe(block);
            for(DB::ap::ap_block_iter_t it = join_result2.get_block_iter(); !it.is_end();) {
                DB::ap::block_tuple_t block = it.consume_block();

                block = example_projection(block);

                emit.emit(block);
            }
        }
    }

    return emit;

} // end example_codegen function
```
