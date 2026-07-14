import fs from 'node:fs';
import path from 'node:path';

const projectRoot = '/Users/zhukovasky/GolandProjects/xmysql-server';
const uaRoot = path.join(projectRoot, '.understand-anything');
const tmpDir = path.join(uaRoot, 'tmp');
const intermediateDir = path.join(uaRoot, 'intermediate');
const batchesPath = path.join(intermediateDir, 'batches.json');
const assigned = [5, 8, 11, 16];

const batchDocument = JSON.parse(fs.readFileSync(batchesPath, 'utf8'));

function roleFor(filePath) {
  const lower = filePath.toLowerCase();
  const rules = [
    ['logger/', ['日志格式化、级别控制与输出适配', 'logging']],
    ['/conf/', ['服务配置加载、校验与默认值处理', 'configuration']],
    ['/dispatcher/', ['SQL 请求分发与系统变量处理', 'dispatcher']],
    ['system_variable', ['MySQL 系统变量定义、读取与更新', 'system-variable']],
    ['decoupled_handler', ['解耦网络协议处理与请求执行', 'network-handler']],
    ['/net/handler', ['MySQL 网络连接和命令处理', 'network-handler']],
    ['/wrapper/system/', ['InnoDB 系统页结构的读取、写入与封装', 'system-page']],
    ['/wrapper/types/', ['InnoDB 页、记录和索引结构的统一封装', 'page-wrapper']],
    ['/util/buffer', ['二进制缓冲区读写和字节序转换', 'binary-buffer']],
    ['/util/checksum', ['InnoDB 页校验和计算与验证', 'checksum']],
    ['/util/hash_table', ['存储层哈希表结构与查找逻辑', 'hash-table']],
    ['/util/unsafe', ['底层内存与字节转换辅助操作', 'unsafe-memory']],
    ['dml_index_sync', ['DML 与二级索引同步的一致性验证', 'index-sync']],
    ['dml_operator', ['Volcano 执行模型中的 DML 算子', 'dml-operator']],
    ['dml_executor', ['INSERT、UPDATE、DELETE 的执行与事务协作', 'dml-executor']],
    ['storage_integrated_dml', ['DML 执行与持久化存储的集成', 'storage-dml']],
    ['storage_integrated_index', ['索引维护与持久化存储的集成', 'storage-index']],
    ['storage_integrated_checkpoint', ['存储检查点、刷盘与恢复状态管理', 'checkpoint']],
    ['storage_adapter', ['执行引擎到存储管理器的适配', 'storage-adapter']],
    ['index_transaction_adapter', ['索引操作与事务生命周期的适配', 'transaction-adapter']],
    ['recovery_adapter', ['恢复流程与执行引擎的适配', 'recovery']],
    ['persistence_demo', ['持久化执行路径的示例和验证', 'persistence']],
    ['show_parsing', ['SHOW 语句参数和过滤条件解析', 'show-parser']],
    ['show_where_filter', ['SHOW 结果的 WHERE 条件求值与过滤', 'show-filter']],
    ['/engine/enginx', ['SQL 执行引擎编排与管理器初始化', 'execution-engine']],
    ['/engine/result', ['执行结果的数据结构', 'query-result']],
    ['executor_record', ['执行器记录的值访问与模式映射', 'executor-record']],
    ['window_function', ['窗口函数分区、排序与结果计算', 'window-function']],
    ['hash_operator', ['哈希连接与哈希聚合算子的执行行为', 'hash-operator']],
    ['outer_join', ['外连接算子的结果语义', 'join-operator']],
    ['subquery_executor', ['子查询算子的执行与结果传递', 'subquery']],
    ['volcano_refactor', ['Volcano 执行器重构后的接口兼容性', 'volcano-executor']],
    ['/manager/lsn_', ['日志序列号分配、推进与持久化', 'lsn-manager']],
    ['/manager/mtr', ['Mini-Transaction 的页修改与日志提交', 'mini-transaction']],
    ['/manager/mvcc', ['MVCC 事务可见性、版本链与快照管理', 'mvcc']],
    ['optimizer_manager', ['查询优化器生命周期与计划优化入口', 'optimizer']],
    ['/manager/page_allocator', ['表空间页分配、回收与空闲空间维护', 'page-allocation']],
    ['/manager/page_cache', ['页缓存读写、命中与淘汰协调', 'page-cache']],
    ['page_fix', ['页修复流程与异常页状态处理', 'page-repair']],
    ['page_initialization', ['新分配页的类型化初始化', 'page-initialization']],
    ['/manager/page_tx', ['页级事务的提交、回滚与脏页管理', 'page-transaction']],
    ['redo_log_batch', ['Redo Log 批量写入、合并与刷盘', 'redo-log']],
    ['redo_log_compression', ['Redo Log 记录压缩与解压', 'redo-compression']],
    ['redo_log_format', ['Redo Log 二进制格式的编码与解码', 'redo-format']],
    ['redo_log_manager', ['Redo Log 缓冲、写入与恢复协调', 'redo-manager']],
    ['redo_', ['Redo Log 修复与恢复行为', 'redo-recovery']],
    ['savepoint', ['事务保存点创建、回滚与释放', 'savepoint']],
    ['schema_manager', ['数据库模式、表和索引元数据管理', 'schema-manager']],
    ['schema_types', ['模式管理使用的元数据类型', 'schema-model']],
    ['secondary_index', ['二级索引与主记录的一致性', 'secondary-index']],
    ['segment_space_optimizer', ['段空间使用统计与回收优化', 'space-optimizer']],
    ['space_expansion', ['表空间扩容、并发控制与状态跟踪', 'space-expansion']],
    ['storage_provider_space_adapter', ['存储提供者的表空间接口适配', 'space-adapter']],
    ['buffer_pool_manager_optimized', ['缓冲池页获取、淘汰和刷盘的优化实现', 'buffer-pool']],
    ['/buffer_pool/', ['InnoDB 缓冲池缓存、淘汰与脏页管理', 'buffer-pool']],
    ['enhanced_btree_index', ['增强 B+Tree 索引的查找、写入与维护', 'btree-index']],
    ['extent_manager', ['区段分配、释放与使用统计', 'extent-manager']],
    ['mysql_user_data', ['mysql.user 系统表的数据存取与权限字段管理', 'user-metadata']],
    ['segment_manager', ['段创建、页分配和空间回收', 'segment-manager']],
    ['space_manager', ['表空间创建、扩展、页分配与统计', 'space-manager']],
    ['tablespace_defragmenter', ['表空间碎片分析、页迁移与压缩整理', 'defragmentation']],
    ['/basic/types', ['InnoDB 基础接口和共享类型定义', 'type-definition']],
    ['/basic/value', ['SQL 值类型、比较和 Go 类型转换', 'value-conversion']],
    ['/metadata/column', ['列元数据、约束和类型属性定义', 'column-metadata']],
    ['/metadata/utils', ['元数据名称与类型转换辅助逻辑', 'metadata-utility']],
    ['cbo_integrated_optimizer', ['基于代价的优化器集成与物理计划选择', 'cost-optimizer']],
    ['explain_plan', ['执行计划树的 EXPLAIN 文本生成', 'explain-plan']],
  ];
  for (const [needle, role] of rules) {
    if (lower.includes(needle)) return role;
  }
  return ['InnoDB 存储与 SQL 执行相关逻辑', 'innodb'];
}

function complexityFor(lines) {
  if (lines < 50) return 'simple';
  if (lines <= 200) return 'moderate';
  return 'complex';
}

function uniqueTags(tags) {
  return [...new Set(tags.filter(Boolean))].slice(0, 5);
}

function exportedNames(result) {
  return new Set((result.exports || []).map((item) => item.name));
}

function sampleSymbols(result) {
  const symbols = [
    ...(result.classes || []).map((item) => item.name),
    ...(result.functions || []).map((item) => item.name),
  ];
  return [...new Set(symbols)].slice(0, 4);
}

function fileSummary(result) {
  const [role] = roleFor(result.path);
  const symbols = sampleSymbols(result);
  const symbolText = symbols.length ? `，覆盖 ${symbols.join('、')} 等符号` : '';
  if (result.path.endsWith('_test.go')) {
    return `验证${role}${symbolText}，用于检查正常路径、边界条件和错误处理。`;
  }
  return `实现${role}${symbolText}，为 xmysql-server 的对应运行路径提供基础能力。`;
}

function fileTags(result) {
  const [, roleTag] = roleFor(result.path);
  if (result.path.endsWith('_test.go')) return uniqueTags(['test', 'go', roleTag, 'verification']);
  const tags = ['go', roleTag];
  if (result.path.includes('/manager/')) tags.push('manager');
  else if (result.path.includes('/engine/')) tags.push('execution-engine');
  else if (result.path.includes('/wrapper/')) tags.push('storage-wrapper');
  else tags.push('service');
  tags.push('source-file');
  return uniqueTags(tags);
}

function symbolAction(name) {
  const patterns = [
    [/^Test/, ['验证', 'test']],
    [/^Benchmark/, ['基准测试', 'benchmark']],
    [/^New/, ['构造并初始化', 'factory']],
    [/^(Get|Find|Lookup|Read|Fetch|Load)/, ['读取或查找', 'read']],
    [/^(Set|Update|Write|Store|Save|Persist)/, ['更新或持久化', 'write']],
    [/^(Create|Add|Append|Insert|Allocate)/, ['创建或分配', 'create']],
    [/^(Delete|Drop|Remove|Release|Free)/, ['删除或释放', 'delete']],
    [/^(Parse|Decode|Unmarshal)/, ['解析', 'parser']],
    [/^(Encode|Marshal|Serialize|Format)/, ['编码或格式化', 'serialization']],
    [/^(Validate|Check|Verify|Is|Has|Can)/, ['检查', 'validation']],
    [/^(Execute|Run|Apply|Process|Handle|Dispatch)/, ['执行', 'execution']],
    [/^(Open|Begin|Start|Init)/, ['启动或初始化', 'lifecycle']],
    [/^(Close|Commit|Rollback|End|Stop)/, ['结束或提交', 'lifecycle']],
    [/^(Optimize|Compact|Defragment|Compress)/, ['优化或压缩', 'optimization']],
    [/^(Recover|Replay|Restore)/, ['恢复', 'recovery']],
    [/^(Calculate|Compute|Evaluate|Compare)/, ['计算', 'calculation']],
  ];
  for (const [pattern, action] of patterns) {
    if (pattern.test(name)) return action;
  }
  return ['实现', 'function'];
}

function receiverQualifiedName(lines, fn, duplicate) {
  if (!duplicate) return fn.name;
  const start = Math.max(0, (fn.startLine || 1) - 1);
  const declaration = lines.slice(start, Math.min(lines.length, start + 4)).join(' ');
  const escaped = fn.name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const match = declaration.match(new RegExp(`func\\s*\\([^)]*?\\*?([A-Za-z_][A-Za-z0-9_]*)\\)\\s*${escaped}\\s*\\(`));
  return match ? `${match[1]}.${fn.name}` : `${fn.name}@${fn.startLine}`;
}

function symbolSummary(name, filePath, kind, methods = []) {
  const [role] = roleFor(filePath);
  if (kind === 'class') {
    const methodText = methods.length ? `，并通过 ${methods.slice(0, 4).join('、')} 等方法暴露行为` : '';
    return `${name} 定义${role}所需的数据结构${methodText}。`;
  }
  const baseName = name.includes('.') ? name.slice(name.lastIndexOf('.') + 1) : name.split('@')[0];
  const [verb] = symbolAction(baseName);
  return `${verb} ${baseName} 对应的${role}流程，并封装该步骤的输入、状态变化与返回结果。`;
}

function buildFragment(batch, extraction) {
  const nodes = [];
  const edges = [];
  const createdByFile = new Map();

  for (const result of extraction.results || []) {
    const fileId = `file:${result.path}`;
    nodes.push({
      id: fileId,
      type: 'file',
      name: path.basename(result.path),
      filePath: result.path,
      summary: fileSummary(result),
      tags: fileTags(result),
      complexity: complexityFor(result.nonEmptyLines ?? result.totalLines ?? 0),
      languageNotes: 'Go 文件以 package 组织类型与函数，并通过首字母大写控制包级导出。',
    });

    const sourceLines = fs.readFileSync(path.join(projectRoot, result.path), 'utf8').split(/\r?\n/);
    const exports = exportedNames(result);
    const functionCounts = new Map();
    for (const fn of result.functions || []) functionCounts.set(fn.name, (functionCounts.get(fn.name) || 0) + 1);
    const created = [];
    const nodeIds = new Set();

    for (const fn of result.functions || []) {
      const span = (fn.endLine || fn.startLine || 0) - (fn.startLine || 0) + 1;
      const isExported = exports.has(fn.name);
      if (span < 10 && !isExported) continue;
      const displayName = receiverQualifiedName(sourceLines, fn, (functionCounts.get(fn.name) || 0) > 1);
      let id = `function:${result.path}:${displayName}`;
      if (nodeIds.has(id)) id = `${id}@${fn.startLine}`;
      nodeIds.add(id);
      const [, roleTag] = roleFor(result.path);
      const [, actionTag] = symbolAction(fn.name);
      const node = {
        id,
        type: 'function',
        name: displayName,
        filePath: result.path,
        lineRange: [fn.startLine, fn.endLine],
        summary: symbolSummary(displayName, result.path, 'function'),
        tags: uniqueTags(['go', actionTag, roleTag, result.path.endsWith('_test.go') ? 'test' : 'function']),
        complexity: complexityFor(span),
      };
      nodes.push(node);
      created.push({node, baseName: fn.name, exported: isExported});
    }

    const classCounts = new Map();
    for (const cls of result.classes || []) classCounts.set(cls.name, (classCounts.get(cls.name) || 0) + 1);
    for (const cls of result.classes || []) {
      const span = (cls.endLine || cls.startLine || 0) - (cls.startLine || 0) + 1;
      const methods = cls.methods || [];
      const isExported = exports.has(cls.name);
      if (span < 20 && methods.length < 2 && !isExported) continue;
      const displayName = (classCounts.get(cls.name) || 0) > 1 ? `${cls.name}@${cls.startLine}` : cls.name;
      let id = `class:${result.path}:${displayName}`;
      if (nodeIds.has(id)) id = `${id}@${cls.startLine}`;
      nodeIds.add(id);
      const [, roleTag] = roleFor(result.path);
      const node = {
        id,
        type: 'class',
        name: displayName,
        filePath: result.path,
        lineRange: [cls.startLine, cls.endLine],
        summary: symbolSummary(displayName, result.path, 'class', methods),
        tags: uniqueTags(['go', 'data-model', roleTag, methods.length ? 'behavior' : 'type-definition']),
        complexity: complexityFor(Math.max(span, methods.length * 10)),
      };
      nodes.push(node);
      created.push({node, baseName: cls.name, exported: isExported});
    }

    createdByFile.set(result.path, created);
    for (const item of created) {
      edges.push({source: fileId, target: item.node.id, type: 'contains', direction: 'forward', weight: 1.0});
      if (item.exported) {
        edges.push({source: fileId, target: item.node.id, type: 'exports', direction: 'forward', weight: 0.8});
      }
    }
  }

  for (const file of batch.files) {
    const imports = batch.batchImportData?.[file.path] || [];
    for (const targetPath of imports) {
      edges.push({
        source: `file:${file.path}`,
        target: `file:${targetPath}`,
        type: 'imports',
        direction: 'forward',
        weight: 0.7,
      });
    }
  }

  return {nodes, edges, createdByFile};
}

function writeParts(batch, fragment) {
  const nodeCount = fragment.nodes.length;
  const edgeCount = fragment.edges.length;
  const parts = Math.ceil(Math.max(nodeCount / 60, edgeCount / 120, 1));
  const sortedFiles = [...batch.files].sort((a, b) => a.path.localeCompare(b.path));
  const chunkSize = Math.ceil(sortedFiles.length / parts);
  const written = [];

  for (let index = 0; index < parts; index += 1) {
    const filePaths = new Set(sortedFiles.slice(index * chunkSize, (index + 1) * chunkSize).map((item) => item.path));
    if (filePaths.size === 0) continue;
    const partNodes = fragment.nodes.filter((node) => filePaths.has(node.filePath));
    const partNodeIds = new Set(partNodes.map((node) => node.id));
    const partEdges = fragment.edges.filter((edge) => partNodeIds.has(edge.source));
    const output = {nodes: partNodes, edges: partEdges};
    const outputPath = path.join(intermediateDir, `batch-${batch.batchIndex}-part-${index + 1}.json`);
    fs.writeFileSync(outputPath, `${JSON.stringify(output, null, 2)}\n`);
    written.push({path: outputPath, nodes: partNodes.length, edges: partEdges.length});
  }
  return {nodeCount, edgeCount, parts: written};
}

const report = [];
for (const batchIndex of assigned) {
  const batch = batchDocument.batches.find((item) => item.batchIndex === batchIndex);
  if (!batch) throw new Error(`Missing batch ${batchIndex} in batches.json`);
  const extractionPath = path.join(tmpDir, `ua-file-extract-results-${batchIndex}.json`);
  const extraction = JSON.parse(fs.readFileSync(extractionPath, 'utf8'));
  if (!extraction.scriptCompleted) throw new Error(`Extraction did not complete for batch ${batchIndex}`);
  const fragment = buildFragment(batch, extraction);
  const written = writeParts(batch, fragment);
  report.push({
    batchIndex,
    files: batch.files.length,
    skipped: extraction.filesSkipped || [],
    expectedImports: Object.values(batch.batchImportData || {}).reduce((sum, imports) => sum + imports.length, 0),
    nodes: written.nodeCount,
    edges: written.edgeCount,
    parts: written.parts.map((item) => ({file: path.basename(item.path), nodes: item.nodes, edges: item.edges})),
  });
}

fs.writeFileSync(path.join(tmpDir, 'ua-assigned-batches-report.json'), `${JSON.stringify(report, null, 2)}\n`);
console.log(JSON.stringify(report, null, 2));
