# Query 方案

总结：由 sql 语句构建 query tree；query tree 分块对应到 codegen；为 query 执行提供向量化基础设施。

- [Query Compilation 方案](#query-compile)
  - [SQL 处理](#query-compile-sql)
      - [文法产生式](#query-compile-sql-grammar)
      - [词法分析、语法解析](#query-compile-sql-parser)
      - [语义检查](#query-compile-sql-check)
  - [构建 Query Tree](#query-compile-tree)
  - [生成代码](#query-compile-codegen)
  - [其他](#query-compile-miscellaneous)
- [Query Execution 方案](#query-exec)
  - [query execution 面临的问题及决策](#query-exec-decision)
  - [数据流代码框架](#query-exec-code)
  - [Vectorized Hash Join](#query-exec-simdhashjoin)
  - [其他](#query-exec-miscellaneous)
- [代码生成样例](#codegen-example)

<img src="assets/SQL-presentation.png" width=720/>


&nbsp;   
<a id="query-compile"></a>
## Query Compilation 方案

TODO: 简要概括工作...

<a id="query-compile-sql"></a>
### SQL 处理

<a id="query-compile-sql-grammar"></a>
#### 文法产生式

```
sqlStatement := selectStatement | "EXIT" | "SWITCH" | "SHOW";

selectStatement := "SELECT" selectElements fromClause ["WHERE" expression];

selectElements := "$";

fromClause := "FROM" tableName [{"," tableName}*];

tableName := "ID";

columnName := "ID" "." "ID";

expression := expression "AND" expression | predicate;

predicate := expressionAtom comparisonOperator expressionAtom | expressionAtom;

expressionAtom := constantNum | "STR_LITERAL" | columnName | expressionAtomOp;

expressionAtomOp := expressionAtom {"*"|"+"|"-"} expressionAtom;

constantNum := ["-"] positiveNum;
	
positiveNum := "NUMBER_CONSTANT";
				 
comparisonOperator := "==" | ">" | "<" | "<=" | ">=" | "!=";
```

<a id="query-compile-sql-parser"></a>
#### 词法分析、语法解析

切割 token，然后使用 LALR 分析，使用 [黎同学](https://github.com/ssyram) 提供的 [NovelRulesTranslator](https://github.com/ssyram/NovelRulesTranslator)，根据 [parse_ap.tsl](https://github.com/rsy56640/xjbDB/blob/query/doc/parse_ap.tsl) 中的 **文法产生式** 及 **规约时的语义动作** 生成 [parse_ap.h](https://github.com/rsy56640/xjbDB/blob/query/code/src/include/parse_ap.h) 文件。

经过语法解析后会得到

- `std::vector<std::pair<std::string, std::string>> columns;`：select 的 column 
- `std::vector<std::string> tables;`：from 哪些 table
- `std::vector<std::shared_ptr<ast::BaseExpr>> conditions;`：用 `AND` 连接的 condition

<a id="query-compile-sql-check"></a>
#### 语义检查

TODO: 检查，加工 condition

- table, column
- join
- ...

处理结果是。。。

<a id="query-compile-tree"></a>
### 构建 Query Tree

TODO: 如何从之前的若干个 vector 生成一棵树

```c++
class APMap;	// 逻辑列到物理列的映射
struct APBaseOp {

    virtual bool isJoin();	// 判断是否为Join结点

    // used to traversal
    virtual void produce() {}

    // source are used to check left/right child, map is constructed from bottom to top
    virtual void consume(APBaseOp *source, APMap &map) {}

    ap_op_t_t op_t_; // 节点类型
    APBaseOp* _parentOp; // 父节点
};
```



<a id="query-compile-codegen"></a>
### 生成代码

TODO: 主要描述 produce consume 流程，列映射，每个结点的 codegen




<a id="query-compile-miscellaneous"></a>
### 其他



&nbsp;   
<a id="query-exec"></a>
## Query Execution 方案

<img src="assets/push-based-pipeline.png" width=600/>

总体来说不复杂，对于生成的代码来说，输入是一系列表，输出是一张表，query-exec 事实上应该提供 **数据流访问基础设施** 和 **节点功能** 就OK了。

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
  - 最简单方案是 codegen 的代码可以分块对应到 query tree，所以我**决定让 block 沿着 pipeline 一直走**

<a id="query-exec-code"></a>
### 数据流代码框架

数据以 block 形式在结点间传递：

- `class ap_table_t;`：table 结点
  - 提供 `ap_block_iter_t`
- `class join_result_buf_t;`：hash probe 的结果，作为 join 结点的输出
  - 提供 `ap_block_iter_t`
- `class ap_block_iter_t`：块迭代器
  - 来源于 `class ap_table_t;` 或 `class join_result_buf_t;`
  - 判断是否结束 `bool is_end() const;`
  - 向后消费 block `block_tuple_t consume_block();`
- `class block_tuple_t`：数据流基本单位，aka 一个块
  - 提供 `block_tuple_iter_t` 用于遍历 tuple
  - `VECTOR_INT getINT(page::range_t range) const;`：向量化运算基础设施
  - 复杂条件，如多表数字计算，字符串比较，`block_tuple_iter_t` 提供块内部的单个 row 访问
- `class hash_table_t;`
  - `void insert(const block_tuple_t&);`：hash build 结点，将数据流在此 break
  - `void build();`：insert 结束后，进行 build
  - `join_result_buf_t probe(const block_tuple_t&) const;`：probe 结点，输入 block，输出若干个 block
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

- 多线程执行：以 pipeline 为单位任务
  - pipeline breaker 就要支持同步功能
      - hash join 结点的 build 最后通知完成，probe 开始要等待通知完成
      - 由于 emit 结点的数据在 local，所以在返回之前要等待完成。因此在 `return emit;` 之前加了一句 `future_last.wait();`
- 由于只有 AVX2 的支持，SIMD compare 是用位运算模拟的。另外 AVX 对 mask 的支持很诡异，之前看向量化论文的很多想法都很难实现，用 AVX2 感觉就像戴着枷锁跳舞


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
