# xjbDB
Team Project for Software Engineering course

- [设计文档](https://github.com/rsy56640/xjbDB/tree/master/doc)
- [参考资料](https://github.com/rsy56640/xjbDB/tree/master/reference)

为了软件工程课程的项目展示，新开一个branch叫做 presentation，其中 sql 不支持 subquery。主要原因是我现在没有想清楚，对于复杂查询，表名和列名如何变化，以及这之间的映射应该谁来维护。

## 谈一谈最近的经验
基础服务很重要，前期存储引擎里面的基础服务抽象的挺不错。比如：

- page 层对 k-v 的服务
- B+树迭代器
- VirtualTable 作为一个 channel

写 query process 的时候有点赶，基本是上来就写，基础服务做的很不好。   
在抽象基础服务之前，需要在大脑中过一遍整个执行流程的细节，最好是把每个模块之间的交互都要模拟一遍，这样才知道怎么提供一套infra。写存储引擎之前，我大概把 《SQLite Database System Design and Implementation》 翻了一遍，并且把一些重要细节理清了，才动手开始写，在写B+树之前就已经想好了page层需要提供什么服务了。    
query process 的基础服务做的很烂，主要有几点：

- 逻辑列（`std::string`）到物理列（`DB::page::range_t`）的映射，这个我在用的地方才现场翻译，，实在太糟糕了
- FK 的处理，这个我没看一般DB是怎么写的，，自己胡写，做了一个 pk view 在内存里，然后每次修改都手动维护
- 关于 NULL 的支持，我现在的想法是在每个物理列之前用1B来标记，，但是基础服务抽象的太差了，，很多物理列都是当场用的时候手写，所以现在很难加了

## 一些值得优化的地方

- query process 的基础服务
- WHERE 中可以把 PK 的判断单独提出来，这样就可以用到B+树上的pk查询范围
- 能否设计一个协议，让 query plan 使用物理列
- 数据库历史统计信息（由于 query 树是流式处理，所以join的时候不知道输入表的行数，只能随机等一个）