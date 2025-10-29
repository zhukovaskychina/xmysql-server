#!/bin/bash

# 批量修复Go 1.16兼容性问题

cd /Users/zhukovasky/GolandProjects/xmysql-server

# 修复system目录下的所有.go文件
for file in server/innodb/storage/wrapper/system/{dict,fsp,ibuf,trx,xdes}.go; do
    if [ -f "$file" ]; then
        # 添加atomic import（如果不存在）
        if ! grep -q "sync/atomic" "$file"; then
            sed -i '' '0,/^import ($/s//import (\n\t"sync\/atomic"/' "$file"
        fi
        
        # 替换atomic操作
        sed -i '' 's/\.stats\.Reads\.Add(1)/atomic.AddUint64(\&\0.stats.Reads, 1)/g' "$file"
        sed -i '' 's/\.stats\.Writes\.Add(1)/atomic.AddUint64(\&\0.stats.Writes, 1)/g' "$file"  
        sed -i '' 's/\.stats\.Corruptions\.Add(1)/atomic.AddUint32(\&\0.stats.Corruptions, 1)/g' "$file"
        sed -i '' 's/\.stats\.Recoveries\.Add(1)/atomic.AddUint32(\&\0.stats.Recoveries, 1)/g' "$file"
    fi
done

echo "修复完成！"
