#!/usr/bin/env python3
import re

# 修复enhanced_btree_index.go中的atomic用法
file_path = "/Users/zhukovasky/GolandProjects/xmysql-server/server/innodb/manager/enhanced_btree_index.go"

with open(file_path, 'r') as f:
    content = f.read()

# 替换isLoaded.Load()为atomic.LoadUint32(&idx.isLoaded) == 1
content = re.sub(r'idx\.isLoaded\.Load\(\)', r'atomic.LoadUint32(&idx.isLoaded) == 1', content)
content = re.sub(r'isLoaded\.Load\(\)', r'atomic.LoadUint32(&isLoaded) == 1', content)

# 替换isLoaded.Store为atomic.StoreUint32
content = re.sub(r'idx\.isLoaded\.Store\(true\)', r'atomic.StoreUint32(&idx.isLoaded, 1)', content)
content = re.sub(r'idx\.isLoaded\.Store\(false\)', r'atomic.StoreUint32(&idx.isLoaded, 0)', content)

# 替换refCount.Add为atomic.AddInt32
content = re.sub(r'idx\.refCount\.Add\((-?\d+)\)', r'atomic.AddInt32(&idx.refCount, \1)', content)

with open(file_path, 'w') as f:
    f.write(content)

print(f"Fixed: {file_path}")
