#!/usr/bin/env python3
import re
import os

files_to_fix = [
    "server/innodb/storage/wrapper/system/dict.go",
    "server/innodb/storage/wrapper/system/fsp.go",
    "server/innodb/storage/wrapper/system/ibuf.go",
    "server/innodb/storage/wrapper/system/trx.go",
    "server/innodb/storage/wrapper/system/xdes.go",
]

base_path = "/Users/zhukovasky/GolandProjects/xmysql-server"

for file_path in files_to_fix:
    full_path = os.path.join(base_path, file_path)
    
    if not os.path.exists(full_path):
        continue
        
    with open(full_path, 'r') as f:
        content = f.read()
    
    # 检查是否已有atomic import
    if '"sync/atomic"' not in content:
        # 在import块后添加atomic
        content = re.sub(r'(import \(\n)', r'\1\t"sync/atomic"\n', content, count=1)
    
    # 替换atomic操作
    replacements = [
        (r'(\w+)\.stats\.Reads\.Add\(1\)', r'atomic.AddUint64(&\1.stats.Reads, 1)'),
        (r'(\w+)\.stats\.Writes\.Add\(1\)', r'atomic.AddUint64(&\1.stats.Writes, 1)'),
        (r'(\w+)\.stats\.Corruptions\.Add\(1\)', r'atomic.AddUint32(&\1.stats.Corruptions, 1)'),
        (r'(\w+)\.stats\.Recoveries\.Add\(1\)', r'atomic.AddUint32(&\1.stats.Recoveries, 1)'),
    ]
    
    for pattern, replacement in replacements:
        content = re.sub(pattern, replacement, content)
    
    with open(full_path, 'w') as f:
        f.write(content)
    
    print(f"Fixed: {file_path}")

print("All files fixed!")
