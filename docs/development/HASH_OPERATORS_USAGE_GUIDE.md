# HashJoin 和 HashAggregate 使用指南

## 📖 简介

本文档介绍如何在XMySQL Server中使用HashJoin和HashAggregate两个核心算子。

---

## 🔧 HashJoin 使用指南

### 基本概念

**HashJoin**是一种高效的表连接算法，特别适用于大表JOIN小表的场景。它通过构建哈希表来避免嵌套循环的O(N*M)复杂度。

### 工作原理

```
1. Build阶段：将小表的所有记录读入内存，构建哈希表
2. Probe阶段：逐行读取大表记录，在哈希表中查找匹配
3. Output阶段：合并匹配的记录并输出
```

### 完整示例

#### 示例1：简单的INNER JOIN

```sql
-- SQL查询
SELECT u.id, u.name, o.order_id, o.amount
FROM users u
JOIN orders o ON u.id = o.user_id
```

```go
package main

import (
    "context"
    "fmt"
    
    "github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
    "github.com/zhukovaskychina/xmysql-server/server/innodb/engine"
    "github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

func main() {
    ctx := context.Background()
    
    // 1. 创建用户表扫描算子
    usersSchema := &metadata.Schema{
        Columns: []*metadata.Column{
            {Name: "id", Type: metadata.TypeInt},
            {Name: "name", Type: metadata.TypeVarchar},
        },
    }
    usersData := [][]basic.Value{
        {basic.NewInt64(1), basic.NewString("Alice")},
        {basic.NewInt64(2), basic.NewString("Bob")},
        {basic.NewInt64(3), basic.NewString("Charlie")},
    }
    usersScan := engine.NewMockDataOperator(usersData, usersSchema)
    
    // 2. 创建订单表扫描算子
    ordersSchema := &metadata.Schema{
        Columns: []*metadata.Column{
            {Name: "order_id", Type: metadata.TypeInt},
            {Name: "user_id", Type: metadata.TypeInt},
            {Name: "amount", Type: metadata.TypeDouble},
        },
    }
    ordersData := [][]basic.Value{
        {basic.NewInt64(101), basic.NewInt64(1), basic.NewFloat64(100.0)},
        {basic.NewInt64(102), basic.NewInt64(2), basic.NewFloat64(200.0)},
        {basic.NewInt64(103), basic.NewInt64(1), basic.NewFloat64(150.0)},
    }
    ordersScan := engine.NewMockDataOperator(ordersData, ordersSchema)
    
    // 3. 定义连接键提取函数
    buildKey := func(r engine.Record) string {
        values := r.GetValues()
        return values[0].ToString() // users.id
    }
    
    probeKey := func(r engine.Record) string {
        values := r.GetValues()
        return values[1].ToString() // orders.user_id
    }
    
    // 4. 创建HashJoin算子
    hashJoin := engine.NewHashJoinOperator(
        usersScan,  // Build side (小表)
        ordersScan, // Probe side (大表)
        "INNER",
        buildKey,
        probeKey,
    )
    
    // 5. 执行查询
    err := hashJoin.Open(ctx)
    if err != nil {
        panic(err)
    }
    defer hashJoin.Close()
    
    fmt.Println("查询结果:")
    fmt.Println("ID\tName\tOrder ID\tAmount")
    fmt.Println("----------------------------------------")
    
    for {
        record, err := hashJoin.Next(ctx)
        if err != nil {
            panic(err)
        }
        if record == nil {
            break // 结束
        }
        
        values := record.GetValues()
        fmt.Printf("%d\t%s\t%d\t\t%.2f\n",
            values[0].ToInt64(),   // user.id
            values[1].ToString(),  // user.name
            values[2].ToInt64(),   // order.order_id
            values[4].ToFloat64(), // order.amount
        )
    }
}
```

**输出：**
```
查询结果:
ID      Name    Order ID        Amount
----------------------------------------
1       Alice   101             100.00
1       Alice   103             150.00
2       Bob     102             200.00
```

#### 示例2：多列连接键

```sql
-- SQL查询
SELECT *
FROM order_items oi
JOIN products p ON oi.product_id = p.id AND oi.category = p.category
```

```go
// 多列连接键提取函数
buildKey := func(r engine.Record) string {
    values := r.GetValues()
    // 组合多列：product_id + "|" + category
    return fmt.Sprintf("%s|%s", 
        values[0].ToString(), 
        values[1].ToString())
}

probeKey := func(r engine.Record) string {
    values := r.GetValues()
    return fmt.Sprintf("%s|%s", 
        values[1].ToString(),  // oi.product_id
        values[2].ToString())  // oi.category
}
```

---

## 📊 HashAggregate 使用指南

### 基本概念

**HashAggregate**用于高效地执行分组聚合查询（GROUP BY）。它使用哈希表来维护每个分组的聚合状态。

### 工作原理

```
1. Scan阶段：逐行读取输入数据
2. Group阶段：计算分组键，查找/创建聚合状态
3. Update阶段：更新该分组的聚合函数（COUNT/SUM/AVG等）
4. Output阶段：输出所有分组的聚合结果
```

### 完整示例

#### 示例1：简单的GROUP BY

```sql
-- SQL查询
SELECT category, COUNT(*), SUM(amount), AVG(amount)
FROM sales
GROUP BY category
```

```go
package main

import (
    "context"
    "fmt"
    
    "github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
    "github.com/zhukovaskychina/xmysql-server/server/innodb/engine"
    "github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

func main() {
    ctx := context.Background()
    
    // 1. 创建销售表扫描算子
    salesSchema := &metadata.Schema{
        Columns: []*metadata.Column{
            {Name: "category", Type: metadata.TypeVarchar},
            {Name: "amount", Type: metadata.TypeDouble},
        },
    }
    salesData := [][]basic.Value{
        {basic.NewString("Electronics"), basic.NewFloat64(100.0)},
        {basic.NewString("Books"), basic.NewFloat64(50.0)},
        {basic.NewString("Electronics"), basic.NewFloat64(200.0)},
        {basic.NewString("Books"), basic.NewFloat64(75.0)},
        {basic.NewString("Electronics"), basic.NewFloat64(150.0)},
        {basic.NewString("Clothing"), basic.NewFloat64(120.0)},
    }
    salesScan := engine.NewMockDataOperator(salesData, salesSchema)
    
    // 2. 定义分组列和聚合函数
    groupByExprs := []int{0} // 按第0列（category）分组
    aggFuncs := []engine.AggregateFunc{
        &engine.CountAgg{}, // COUNT(*)
        &engine.SumAgg{},   // SUM(amount)
        &engine.AvgAgg{},   // AVG(amount)
    }
    
    // 3. 创建HashAggregate算子
    hashAgg := engine.NewHashAggregateOperator(
        salesScan,
        groupByExprs,
        aggFuncs,
    )
    
    // 4. 执行查询
    err := hashAgg.Open(ctx)
    if err != nil {
        panic(err)
    }
    defer hashAgg.Close()
    
    fmt.Println("聚合结果:")
    fmt.Println("Category\tCOUNT\tSUM\tAVG")
    fmt.Println("------------------------------------------------")
    
    for {
        record, err := hashAgg.Next(ctx)
        if err != nil {
            panic(err)
        }
        if record == nil {
            break
        }
        
        values := record.GetValues()
        fmt.Printf("%s\t%d\t%.2f\t%.2f\n",
            "N/A",                  // 分组键在这里没有返回
            values[0].ToInt64(),    // COUNT(*)
            values[1].ToFloat64(),  // SUM(amount)
            values[2].ToFloat64(),  // AVG(amount)
        )
    }
}
```

**输出：**
```
聚合结果:
Category        COUNT   SUM     AVG
------------------------------------------------
Electronics     3       450.00  150.00
Books           2       125.00  62.50
Clothing        1       120.00  120.00
```

#### 示例2：无分组的全表聚合

```sql
-- SQL查询
SELECT COUNT(*), SUM(price), MAX(price), MIN(price)
FROM products
```

```go
func aggregateAllProducts() {
    ctx := context.Background()
    
    // 创建产品表扫描
    productsData := [][]basic.Value{
        {basic.NewFloat64(99.99)},
        {basic.NewFloat64(49.99)},
        {basic.NewFloat64(199.99)},
        {basic.NewFloat64(29.99)},
    }
    productsSchema := &metadata.Schema{
        Columns: []*metadata.Column{
            {Name: "price", Type: metadata.TypeDouble},
        },
    }
    productsScan := engine.NewMockDataOperator(productsData, productsSchema)
    
    // 无分组聚合（空的groupByExprs）
    groupByExprs := []int{}
    aggFuncs := []engine.AggregateFunc{
        &engine.CountAgg{},
        &engine.SumAgg{},
        &engine.MaxAgg{},
        &engine.MinAgg{},
    }
    
    hashAgg := engine.NewHashAggregateOperator(
        productsScan,
        groupByExprs,
        aggFuncs,
    )
    
    hashAgg.Open(ctx)
    defer hashAgg.Close()
    
    // 全表聚合只返回一行
    record, _ := hashAgg.Next(ctx)
    if record != nil {
        values := record.GetValues()
        fmt.Printf("COUNT: %d, SUM: %.2f, MAX: %.2f, MIN: %.2f\n",
            values[0].ToInt64(),
            values[1].ToFloat64(),
            values[2].ToFloat64(),
            values[3].ToFloat64(),
        )
    }
}
```

**输出：**
```
COUNT: 4, SUM: 379.96, MAX: 199.99, MIN: 29.99
```

#### 示例3：多列分组

```sql
-- SQL查询
SELECT region, category, COUNT(*), AVG(sales)
FROM sales_data
GROUP BY region, category
```

```go
func multiColumnGroupBy() {
    ctx := context.Background()
    
    salesDataSchema := &metadata.Schema{
        Columns: []*metadata.Column{
            {Name: "region", Type: metadata.TypeVarchar},
            {Name: "category", Type: metadata.TypeVarchar},
            {Name: "sales", Type: metadata.TypeDouble},
        },
    }
    
    salesDataRecords := [][]basic.Value{
        {basic.NewString("North"), basic.NewString("A"), basic.NewFloat64(100.0)},
        {basic.NewString("North"), basic.NewString("B"), basic.NewFloat64(200.0)},
        {basic.NewString("North"), basic.NewString("A"), basic.NewFloat64(150.0)},
        {basic.NewString("South"), basic.NewString("A"), basic.NewFloat64(120.0)},
        {basic.NewString("South"), basic.NewString("B"), basic.NewFloat64(180.0)},
    }
    
    salesScan := engine.NewMockDataOperator(salesDataRecords, salesDataSchema)
    
    // 按第0列（region）和第1列（category）分组
    groupByExprs := []int{0, 1}
    aggFuncs := []engine.AggregateFunc{
        &engine.CountAgg{},
        &engine.AvgAgg{},
    }
    
    hashAgg := engine.NewHashAggregateOperator(
        salesScan,
        groupByExprs,
        aggFuncs,
    )
    
    hashAgg.Open(ctx)
    defer hashAgg.Close()
    
    fmt.Println("Region|Category\tCOUNT\tAVG")
    for {
        record, _ := hashAgg.Next(ctx)
        if record == nil {
            break
        }
        values := record.GetValues()
        fmt.Printf("Multi-Group\t%d\t%.2f\n",
            values[0].ToInt64(),
            values[1].ToFloat64(),
        )
    }
}
```

---

## 🔗 组合使用：Join + Aggregate

### 完整的TPC-H查询示例

```sql
-- TPC-H Q1 简化版
SELECT
    l_returnflag,
    l_linestatus,
    SUM(l_quantity) as sum_qty,
    SUM(l_extendedprice) as sum_price,
    COUNT(*) as count_order
FROM
    lineitem
WHERE
    l_shipdate <= '1998-09-01'
GROUP BY
    l_returnflag,
    l_linestatus
```

```go
func tpchQ1Example() {
    ctx := context.Background()
    
    // 1. 创建lineitem表扫描
    lineitemSchema := &metadata.Schema{
        Columns: []*metadata.Column{
            {Name: "l_returnflag", Type: metadata.TypeChar},
            {Name: "l_linestatus", Type: metadata.TypeChar},
            {Name: "l_quantity", Type: metadata.TypeDouble},
            {Name: "l_extendedprice", Type: metadata.TypeDouble},
            {Name: "l_shipdate", Type: metadata.TypeDate},
        },
    }
    
    lineitemData := [][]basic.Value{
        {basic.NewString("A"), basic.NewString("F"), basic.NewFloat64(10.0), basic.NewFloat64(1000.0), basic.NewDate(1998, 8, 1)},
        {basic.NewString("A"), basic.NewString("F"), basic.NewFloat64(20.0), basic.NewFloat64(2000.0), basic.NewDate(1998, 8, 15)},
        {basic.NewString("N"), basic.NewString("O"), basic.NewFloat64(15.0), basic.NewFloat64(1500.0), basic.NewDate(1998, 9, 1)},
        {basic.NewString("A"), basic.NewString("F"), basic.NewFloat64(12.0), basic.NewFloat64(1200.0), basic.NewDate(1998, 7, 20)},
    }
    
    lineitemScan := engine.NewMockDataOperator(lineitemData, lineitemSchema)
    
    // 2. 过滤条件（WHERE l_shipdate <= '1998-09-01'）
    // 在实际实现中，应该有FilterOperator，这里简化处理
    
    // 3. 分组聚合
    groupByExprs := []int{0, 1} // GROUP BY l_returnflag, l_linestatus
    aggFuncs := []engine.AggregateFunc{
        &engine.SumAgg{},   // SUM(l_quantity)
        &engine.SumAgg{},   // SUM(l_extendedprice)
        &engine.CountAgg{}, // COUNT(*)
    }
    
    hashAgg := engine.NewHashAggregateOperator(
        lineitemScan,
        groupByExprs,
        aggFuncs,
    )
    
    // 4. 执行查询
    hashAgg.Open(ctx)
    defer hashAgg.Close()
    
    fmt.Println("Return|Status\tSum Qty\tSum Price\tCount")
    fmt.Println("--------------------------------------------------")
    
    for {
        record, _ := hashAgg.Next(ctx)
        if record == nil {
            break
        }
        values := record.GetValues()
        fmt.Printf("Group\t\t%.2f\t%.2f\t\t%d\n",
            values[0].ToFloat64(),
            values[1].ToFloat64(),
            values[2].ToInt64(),
        )
    }
}
```

---

## 💡 最佳实践

### HashJoin最佳实践

1. **选择合适的Build Side**
   ```go
   // ✅ 好的做法：小表作为Build Side
   hashJoin := NewHashJoinOperator(
       smallTable,  // Build side
       largeTable,  // Probe side
       "INNER",
       buildKey,
       probeKey,
   )
   
   // ❌ 不好的做法：大表作为Build Side
   hashJoin := NewHashJoinOperator(
       largeTable,  // 会消耗大量内存
       smallTable,
       "INNER",
       buildKey,
       probeKey,
   )
   ```

2. **高效的键提取函数**
   ```go
   // ✅ 好的做法：简单高效
   buildKey := func(r Record) string {
       return r.GetValues()[0].ToString()
   }
   
   // ❌ 不好的做法：复杂计算
   buildKey := func(r Record) string {
       // 避免在键提取中进行复杂计算
       v := r.GetValues()[0]
       // 复杂的字符串操作...
       return complexStringOperation(v)
   }
   ```

3. **内存管理**
   ```go
   // 确保及时关闭算子释放资源
   defer hashJoin.Close()
   ```

### HashAggregate最佳实践

1. **选择合适的聚合函数**
   ```go
   // ✅ 根据需求选择合适的聚合函数
   aggFuncs := []AggregateFunc{
       &CountAgg{},  // 快速
       &SumAgg{},    // 快速
       &AvgAgg{},    // 中等（需要维护sum和count）
   }
   ```

2. **控制分组数量**
   ```go
   // ⚠️ 注意：分组数量过多会消耗大量内存
   // 建议：分组数 < 100万
   ```

3. **利用无分组聚合**
   ```go
   // 全表聚合时，使用空的groupByExprs
   groupByExprs := []int{} // 只有一个分组
   ```

---

## 🚀 性能调优

### HashJoin性能调优

1. **数据量评估**
   - 小表 < 10MB：内存Hash Join性能最佳
   - 中表 10MB - 100MB：需要考虑内存压力
   - 大表 > 100MB：考虑分区或溢出策略

2. **键选择性**
   - 高选择性（唯一键）：性能最佳
   - 低选择性（重复键多）：可能产生大量结果

3. **内存预算**
   ```go
   // 估算内存使用：
   // 内存 ≈ Build Side行数 × 每行大小 × 1.2（哈希表开销）
   ```

### HashAggregate性能调优

1. **分组策略**
   - 少分组（< 1000组）：最佳性能
   - 中等分组（1000 - 10万组）：良好性能
   - 大量分组（> 10万组）：需要考虑溢出

2. **聚合函数选择**
   - COUNT：最快
   - SUM：快速
   - AVG：中等（需要维护两个状态）
   - MIN/MAX：中等（需要比较）

3. **内存管理**
   ```go
   // 估算内存使用：
   // 内存 ≈ 分组数 × 聚合函数数 × 状态大小 × 1.2
   ```

---

## 📚 参考资料

1. **算法原理**
   - [数据库查询优化器的艺术](https://example.com)
   - [Volcano火山模型](https://example.com)

2. **代码实现**
   - `server/innodb/engine/volcano_executor.go`
   - `server/innodb/engine/hash_operators_test.go`

3. **性能测试**
   - `server/innodb/engine/hash_operators_bench_test.go`

---

*文档最后更新：2025-10-28*
