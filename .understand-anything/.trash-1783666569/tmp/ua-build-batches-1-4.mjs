import fs from 'node:fs';
import path from 'node:path';

const projectRoot = '/Users/zhukovasky/GolandProjects/xmysql-server';
const intermediateDir = path.join(projectRoot, '.understand-anything', 'intermediate');
const tmpDir = path.join(projectRoot, '.understand-anything', 'tmp');
const batchesData = JSON.parse(fs.readFileSync(path.join(intermediateDir, 'batches.json'), 'utf8'));

const subjectRules = [
  [/auth/, 'MySQL 身份认证与权限校验'],
  [/dispatcher/, '协议消息分发与请求处理'],
  [/common\/charset/, '字符集与排序规则映射'],
  [/common\/error|errorcode|error_state|error_name/, 'MySQL 错误码、SQLSTATE 与错误消息'],
  [/common\/privs/, '数据库权限位与权限名称转换'],
  [/common\/type/, '字段标志与类型判定'],
  [/common\/page/, '存储页通用接口与索引项'],
  [/common/, '服务端通用常量与辅助逻辑'],
  [/buffer_pool/, 'InnoDB Buffer Pool 缓存、淘汰与刷盘'],
  [/bplus_tree/, 'B+Tree 索引管理'],
  [/dictionary_manager/, 'InnoDB 数据字典管理'],
  [/storage_manager|system_space_manager/, 'InnoDB 表空间与存储生命周期管理'],
  [/manager/, 'InnoDB 存储管理协调'],
  [/record/, 'InnoDB 行记录格式、编码与优化'],
  [/sqltypes/, 'SQL 聚合类型与求值'],
  [/store\/blocks/, '底层文件块存储'],
  [/store\/pages/, 'InnoDB 物理页结构与序列化'],
  [/store\/segs/, 'InnoDB 段分配与回收'],
  [/store\/table/, '表定义与 form 元数据'],
  [/wrapper\/blob/, 'BLOB 页链与大对象管理'],
  [/wrapper\/page/, 'InnoDB 页对象包装与字段访问'],
  [/wrapper\/segment/, '段包装接口与类型'],
  [/wrapper/, '存储页包装层'],
  [/cmd\//, '可执行演示与协议验证'],
];

function subjectFor(filePath) {
  for (const [rule, subject] of subjectRules) if (rule.test(filePath)) return subject;
  return 'xmysql-server 服务端逻辑';
}

function topicFor(filePath) {
  return path.basename(filePath, path.extname(filePath))
    .replace(/_test$/, '')
    .replaceAll('_', ' ');
}

function fileSummary(file) {
  const p = file.path;
  const base = path.basename(p);
  const topic = topicFor(p);
  const subject = subjectFor(p);
  if (base.endsWith('_test.go')) return `验证${subject}中 ${topic} 的正常路径、边界条件与回归行为。`;
  if (p.startsWith('cmd/') && base === 'main.go') return `提供用于验证${subject}的独立命令入口，组织连接、请求和结果检查流程。`;
  if (p.startsWith('cmd/')) return `提供${subject}的可运行演示，集中复现并检查 ${topic} 行为。`;
  if (/const|constant|types\.go$|page_types/.test(base)) return `定义${subject}使用的常量、枚举和基础类型，为同包实现提供统一语义。`;
  if (/error/.test(base)) return `定义${subject}的错误类型、错误码映射及判定辅助函数。`;
  if (/factory|constructors/.test(base)) return `集中创建${subject}对象，并按页类型或配置选择对应实现。`;
  if (/manager/.test(base)) return `协调${subject}的创建、查询、更新与资源释放，封装底层实现细节。`;
  if (/adapter/.test(base)) return `将${subject}适配为上层接口所需的数据访问与生命周期操作。`;
  if (/serializer/.test(base)) return `负责${subject}的二进制序列化、反序列化与格式校验。`;
  if (/wrapper/.test(base)) return `封装${subject}的底层字节布局，提供类型化读写和状态管理接口。`;
  if (/strategy/.test(base)) return `实现${subject}的策略选择与执行规则，支持按运行状态调整处理方式。`;
  if (/state/.test(base)) return `定义${subject}的状态模型与状态转换所需数据。`;
  if (/page/.test(base)) return `实现${subject}的页结构、字段访问和持久化操作。`;
  if (/format/.test(base)) return `实现${subject}的格式解析、长度计算与二进制编解码。`;
  return `实现${subject}中的 ${topic} 能力，并向同包或上层组件提供可复用接口。`;
}

function complexityFromLines(lines, extra = 0) {
  if (lines > 200 || extra > 25) return 'complex';
  if (lines >= 50 || extra > 8) return 'moderate';
  return 'simple';
}

function fileTags(file) {
  const p = file.path.toLowerCase();
  const tags = [];
  if (p.endsWith('_test.go')) tags.push('test', 'regression');
  else if (p.startsWith('cmd/') && p.endsWith('/main.go')) tags.push('entry-point', 'command');
  else tags.push('service');
  if (p.includes('auth')) tags.push('authentication', 'security');
  else if (p.includes('dispatcher')) tags.push('dispatcher', 'protocol');
  else if (p.includes('buffer_pool')) tags.push('buffer-pool', 'cache');
  else if (p.includes('record')) tags.push('record-format', 'serialization');
  else if (p.includes('/pages/') || p.includes('/page/')) tags.push('storage-page', 'innodb');
  else if (p.includes('manager')) tags.push('storage-manager', 'innodb');
  else if (p.includes('common')) tags.push('shared-types', 'mysql');
  else tags.push('innodb', 'storage');
  if (/wrapper/.test(p)) tags.push('adapter');
  if (/serializer|format|codec/.test(p)) tags.push('serialization');
  if (/constant|const|types\.go/.test(p)) tags.push('type-definition');
  return [...new Set(tags)].slice(0, 5);
}

function symbolAction(name) {
  if (/^Test/.test(name)) return '验证';
  if (/^(New|Create|Init|Initialize|Allocate|Open)/.test(name)) return '创建或初始化';
  if (/^(Get|Read|Load|Search|Find|Lookup|Range|Query)/.test(name)) return '读取或查询';
  if (/^(Set|Write|Put|Insert|Add|Update|Mark|Record)/.test(name)) return '写入或更新';
  if (/^(Delete|Remove|Drop|Free|Deallocate|Clear|Purge|Evict)/.test(name)) return '删除或释放';
  if (/^(Validate|Check|Verify|Is|Has|Less|Compare)/.test(name)) return '校验或判断';
  if (/^(Encode|Serialize|ToByte|Marshal)/.test(name)) return '编码或序列化';
  if (/^(Decode|Deserialize|Parse|Unmarshal)/.test(name)) return '解析或反序列化';
  if (/^(Handle|Process|Execute|Dispatch|Authenticate)/.test(name)) return '处理';
  if (/^(Start|Run|main|Trigger)/.test(name)) return '启动并协调';
  if (/^(Flush|Sync|Close|Commit|Rollback)/.test(name)) return '执行持久化或生命周期操作';
  return '实现';
}

function functionSummary(filePath, name) {
  return `${symbolAction(name)}${subjectFor(filePath)}中的 ${name} 流程，并返回调用方所需结果。`;
}

function functionTags(filePath, name) {
  const tags = ['function'];
  if (/^Test/.test(name)) tags.push('test', 'regression');
  else if (/^(New|Create|Init|Initialize)/.test(name)) tags.push('factory', 'initialization');
  else if (/^(Get|Read|Load|Search|Find|Lookup|Query)/.test(name)) tags.push('data-access', 'query');
  else if (/^(Set|Write|Put|Insert|Add|Update)/.test(name)) tags.push('mutation', 'storage');
  else if (/^(Validate|Check|Verify|Is|Has)/.test(name)) tags.push('validation', 'guard');
  else if (/^(Encode|Decode|Serialize|Deserialize|Parse|ToByte)/.test(name)) tags.push('serialization', 'binary-format');
  else tags.push('service-logic', 'workflow');
  if (filePath.includes('auth')) tags.push('authentication');
  else if (filePath.includes('/page')) tags.push('storage-page');
  else if (filePath.includes('buffer_pool')) tags.push('buffer-pool');
  return [...new Set(tags)].slice(0, 5);
}

function classSummary(filePath, cls) {
  const subject = subjectFor(filePath);
  if ((cls.methods?.length || 0) > 0 && (cls.properties?.length || 0) === 0) {
    return `定义${subject}的 ${cls.name} 接口契约，约束实现方必须提供的操作。`;
  }
  return `封装${subject}中的 ${cls.name} 状态与行为，作为相关操作的核心数据结构。`;
}

function classTags(filePath, cls) {
  const tags = [(cls.methods?.length || 0) > 0 && (cls.properties?.length || 0) === 0 ? 'interface' : 'data-model', 'go-type'];
  if ((cls.methods?.length || 0) >= 2) tags.push('service');
  else tags.push('type-definition');
  if (filePath.includes('/page')) tags.push('storage-page');
  else if (filePath.includes('auth')) tags.push('authentication');
  else if (filePath.includes('buffer_pool')) tags.push('buffer-pool');
  else tags.push('innodb');
  return [...new Set(tags)].slice(0, 5);
}

function dedupeById(nodes) {
  const seen = new Set();
  return nodes.filter((node) => {
    if (seen.has(node.id)) return false;
    seen.add(node.id);
    return true;
  });
}

function buildBatch(batchIndex) {
  const batch = batchesData.batches.find((candidate) => candidate.batchIndex === batchIndex);
  if (!batch) throw new Error(`batch ${batchIndex} 不存在`);
  const extraction = JSON.parse(fs.readFileSync(path.join(tmpDir, `ua-file-extract-results-${batchIndex}.json`), 'utf8'));
  if (!extraction.scriptCompleted) throw new Error(`batch ${batchIndex} 结构提取未完成`);
  const resultByPath = new Map(extraction.results.map((result) => [result.path, result]));
  const nodes = [];
  const edges = [];

  for (const file of batch.files) {
    const result = resultByPath.get(file.path);
    if (!result) throw new Error(`batch ${batchIndex} 缺少提取结果: ${file.path}`);
    const fileId = `file:${file.path}`;
    nodes.push({
      id: fileId,
      type: 'file',
      name: path.basename(file.path),
      filePath: file.path,
      summary: fileSummary(file),
      tags: fileTags(file),
      complexity: complexityFromLines(result.nonEmptyLines, (result.functions?.length || 0) + (result.classes?.length || 0)),
    });

    const exportedNames = new Set((result.exports || []).map((item) => item.name));
    const functions = new Map();
    for (const fn of result.functions || []) {
      const lineCount = Math.max(1, fn.endLine - fn.startLine + 1);
      if (lineCount < 10 && !exportedNames.has(fn.name)) continue;
      if (!functions.has(fn.name)) functions.set(fn.name, fn);
    }
    for (const fn of functions.values()) {
      const functionId = `function:${file.path}:${fn.name}`;
      nodes.push({
        id: functionId,
        type: 'function',
        name: fn.name,
        filePath: file.path,
        lineRange: [fn.startLine, fn.endLine],
        summary: functionSummary(file.path, fn.name),
        tags: functionTags(file.path, fn.name),
        complexity: complexityFromLines(fn.endLine - fn.startLine + 1),
      });
      edges.push({source: fileId, target: functionId, type: 'contains', direction: 'forward', weight: 1.0});
      if (exportedNames.has(fn.name)) edges.push({source: fileId, target: functionId, type: 'exports', direction: 'forward', weight: 0.8});
    }

    const classes = new Map();
    for (const cls of result.classes || []) {
      const lineCount = Math.max(1, cls.endLine - cls.startLine + 1);
      if ((cls.methods?.length || 0) < 2 && lineCount < 20 && !exportedNames.has(cls.name)) continue;
      if (!classes.has(cls.name)) classes.set(cls.name, cls);
    }
    for (const cls of classes.values()) {
      const classId = `class:${file.path}:${cls.name}`;
      nodes.push({
        id: classId,
        type: 'class',
        name: cls.name,
        filePath: file.path,
        lineRange: [cls.startLine, cls.endLine],
        summary: classSummary(file.path, cls),
        tags: classTags(file.path, cls),
        complexity: complexityFromLines(cls.endLine - cls.startLine + 1, (cls.methods?.length || 0) + (cls.properties?.length || 0)),
      });
      edges.push({source: fileId, target: classId, type: 'contains', direction: 'forward', weight: 1.0});
      if (exportedNames.has(cls.name)) edges.push({source: fileId, target: classId, type: 'exports', direction: 'forward', weight: 0.8});
    }

    for (const targetPath of batch.batchImportData[file.path] || []) {
      if (targetPath === file.path) throw new Error(`batch ${batchIndex} 包含自引用 import: ${file.path}`);
      edges.push({source: fileId, target: `file:${targetPath}`, type: 'imports', direction: 'forward', weight: 0.7});
    }
  }

  const uniqueNodes = dedupeById(nodes);
  const expectedImports = batch.files.reduce((sum, file) => sum + (batch.batchImportData[file.path] || []).length, 0);
  const actualImports = edges.filter((edge) => edge.type === 'imports').length;
  if (expectedImports !== actualImports) throw new Error(`batch ${batchIndex} import 边数量不匹配: ${actualImports}/${expectedImports}`);

  const partCount = Math.ceil(Math.max(uniqueNodes.length / 60, edges.length / 120));
  const sortedFiles = [...batch.files].sort((a, b) => a.path.localeCompare(b.path));
  const filesPerPart = Math.ceil(sortedFiles.length / partCount);
  const written = [];
  for (let part = 0; part < partCount; part += 1) {
    const partFiles = new Set(sortedFiles.slice(part * filesPerPart, (part + 1) * filesPerPart).map((file) => file.path));
    if (partFiles.size === 0) continue;
    const partNodes = uniqueNodes.filter((node) => partFiles.has(node.filePath));
    const partNodeIds = new Set(partNodes.map((node) => node.id));
    const partEdges = edges.filter((edge) => partNodeIds.has(edge.source));
    const fileName = `batch-${batchIndex}-part-${part + 1}.json`;
    const outputPath = path.join(intermediateDir, fileName);
    fs.writeFileSync(outputPath, `${JSON.stringify({nodes: partNodes, edges: partEdges}, null, 2)}\n`);
    written.push({fileName, nodes: partNodes.length, edges: partEdges.length});
  }

  const allowedFiles = new Set(batch.files.map((file) => file.path));
  for (const [source, targets] of Object.entries(batch.batchImportData)) {
    allowedFiles.add(source);
    for (const target of targets) allowedFiles.add(target);
  }
  for (const [source, neighbors] of Object.entries(batch.neighborMap || {})) {
    allowedFiles.add(source);
    for (const neighbor of neighbors) allowedFiles.add(neighbor.path);
  }
  for (const item of written) {
    const fragment = JSON.parse(fs.readFileSync(path.join(intermediateDir, item.fileName), 'utf8'));
    const ids = new Set(fragment.nodes.map((node) => node.id));
    if (!Array.isArray(fragment.nodes) || !Array.isArray(fragment.edges)) throw new Error(`${item.fileName} 缺少 nodes/edges 数组`);
    for (const edge of fragment.edges) {
      if (!ids.has(edge.source)) throw new Error(`${item.fileName} 边 source 不在分片节点中: ${edge.source}`);
      if (ids.has(edge.target)) continue;
      if (edge.target.startsWith('file:') && allowedFiles.has(edge.target.slice(5))) continue;
      throw new Error(`${item.fileName} 边 target 无法验证: ${edge.target}`);
    }
  }

  return {
    batchIndex,
    files: batch.files.length,
    nodes: uniqueNodes.length,
    edges: edges.length,
    imports: actualImports,
    skipped: extraction.filesSkipped || [],
    parts: written,
  };
}

const reports = [1, 2, 3, 4].map(buildBatch);
process.stdout.write(`${JSON.stringify(reports, null, 2)}\n`);
