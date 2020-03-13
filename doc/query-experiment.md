# Query 实验方案

## Join

- TP: loop join
- AP: hash join

几k大小的表做join，列不是 pk（之后 TP 也许支持多表join）

## Where

- trivial
- complicated

比较 AP算数指令执行 和 TP语法树解释执行

## 数据分布

- scattered
- skewed

针对 AP hash join，研究数据分布对性能的影响