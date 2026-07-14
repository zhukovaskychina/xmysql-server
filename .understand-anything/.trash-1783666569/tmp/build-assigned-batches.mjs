import fs from 'node:fs';
import path from 'node:path';

const projectRoot = process.cwd();
const uaRoot = path.join(projectRoot, '.understand-anything');
const intermediateDir = path.join(uaRoot, 'intermediate');
const tmpDir = path.join(uaRoot, 'tmp');
const assigned = [17, 18, 20, 21, 39];

const batchesData = JSON.parse(
  fs.readFileSync(path.join(intermediateDir, 'batches.json'), 'utf8'),
);

const batchesByIndex = new Map(
  batchesData.batches.map((batch) => [batch.batchIndex, batch]),
);

function isExportedName(name) {
  return /^[A-Z]/.test(name || '');
}

function complexityFromLines(nonEmptyLines) {
  if (nonEmptyLines > 200) return 'complex';
  if (nonEmptyLines >= 50) return 'moderate';
  return 'simple';
}

function domainInfo(filePath) {
  const normalized = filePath.toLowerCase();
  if (normalized.includes('/sqlparser/')) {
    return { label: 'SQL 解析与语义分析', tag: 'sql-parser' };
  }
  if (normalized.includes('/metadata/')) {
    return { label: '元数据与表结构', tag: 'metadata' };
  }
  if (normalized.includes('/plan/')) {
    return { label: '查询计划与优化器', tag: 'query-optimizer' };
  }
  if (normalized.includes('/engine/')) {
    return { label: 'SQL 执行引擎', tag: 'execution-engine' };
  }
  if (normalized.includes('/integration/')) {
    return { label: '存储与执行引擎集成', tag: 'integration' };
  }
  if (normalized.includes('/format/mvcc/')) {
    return { label: 'MVCC 版本格式', tag: 'mvcc' };
  }
  if (normalized.includes('/wrapper/mvcc/')) {
    return { label: 'MVCC 页面封装', tag: 'mvcc' };
  }
  if (normalized.includes('/store/mvcc/')) {
    return { label: 'MVCC 隔离与事务可见性', tag: 'mvcc' };
  }
  if (normalized.includes('/wrapper/record/')) {
    return { label: 'InnoDB 记录格式封装', tag: 'record-format' };
  }
  if (normalized.includes('/wrapper/extent/')) {
    return { label: 'InnoDB extent 管理', tag: 'extent-management' };
  }
  if (normalized.includes('/wrapper/space/')) {
    return { label: '表空间与位图管理', tag: 'tablespace' };
  }
  if (normalized.includes('/wrapper/page/')) {
    return { label: 'InnoDB 页面封装', tag: 'page-management' };
  }
  if (normalized.includes('/wrapper/segment/')) {
    return { label: 'InnoDB segment 管理', tag: 'segment-management' };
  }
  if (normalized.includes('/wrapper/types/')) {
    return { label: '存储层类型定义', tag: 'type-definition' };
  }
  if (normalized.includes('/store/blocks/')) {
    return { label: '块文件存储', tag: 'block-storage' };
  }
  if (normalized.includes('/store/extents/')) {
    return { label: 'extent 持久化结构', tag: 'extent-management' };
  }
  if (normalized.includes('/store/ibd/')) {
    return { label: 'IBD 表空间文件', tag: 'tablespace' };
  }
  if (normalized.includes('/record/')) {
    return { label: 'InnoDB 记录抽象', tag: 'record-format' };
  }
  if (normalized.includes('/net/')) {
    return { label: 'MySQL 网络服务', tag: 'network-server' };
  }
  return { label: 'InnoDB 存储引擎', tag: 'storage-engine' };
}

function cleanSymbol(name) {
  return String(name || '')
    .replace(/^Test/, '')
    .replace(/^Benchmark/, '')
    .replaceAll('_', ' ')
    .trim();
}

function fileSummary(result) {
  const base = path.basename(result.path);
  const domain = domainInfo(result.path);
  const names = (result.functions || []).map((item) => item.name);
  const classes = (result.classes || []).map((item) => item.name);
  if (base.endsWith('_test.go')) {
    const subjects = names
      .filter((name) => name.startsWith('Test') || name.startsWith('Benchmark'))
      .slice(0, 3)
      .map(cleanSymbol)
      .filter(Boolean);
    const detail = subjects.length > 0 ? `，重点覆盖 ${subjects.join('、')}` : '';
    return `验证${domain.label}相关行为与边界条件${detail}。`;
  }
  if (base === 'volcano_executor.go') {
    return '实现 Volcano 执行器的算子体系、物理计划构建与流式执行流程，连接查询优化结果和底层记录读取。';
  }
  if (base === 'mysql_server.go') {
    return '实现 MySQL 协议服务的监听、连接接入和生命周期管理，并将客户端会话交给服务端处理链。';
  }
  if (base.includes('statistics_collector')) {
    return `实现${domain.label}的统计信息采集与辅助计算，为基于代价的计划选择提供行数、基数和分布估计。`;
  }
  if (base.includes('optimizer')) {
    return `实现${domain.label}的优化规则与候选计划选择，负责将逻辑表达转换为可执行且代价更低的计划。`;
  }
  if (base.includes('expression')) {
    return `定义并处理${domain.label}中的表达式结构，覆盖规范化、求值、条件分解或列引用分析。`;
  }
  if (base.includes('metadata') || result.path.includes('/metadata/')) {
    return `提供${domain.label}的结构定义与转换逻辑，支撑数据库、表、列和查询结果模式的统一描述。`;
  }
  if (base.includes('integration')) {
    return `整合${domain.label}的关键组件，协调查询执行、事务、页面访问和持久化接口。`;
  }
  if (base.includes('record') || result.path.includes('/record/')) {
    return `实现${domain.label}的编码、字段访问和系统记录处理，使页面中的物理记录可被执行与事务层使用。`;
  }
  if (base.includes('page')) {
    return `实现${domain.label}的数据结构和操作接口，封装页面读取、修改、快照或版本访问。`;
  }
  if (base.includes('extent')) {
    return `实现${domain.label}的数据结构与分配操作，管理连续页面范围及其状态变化。`;
  }
  if (base.includes('space') || base.includes('bitmap')) {
    return `实现${domain.label}的空间分配与状态跟踪，通过位图或表空间结构管理页面和 extent。`;
  }
  if (base.includes('mvcc') || base.includes('version') || base.includes('read_view') || base.includes('isolation')) {
    return `实现${domain.label}的可见性判断、版本链和隔离语义，为一致性读与事务版本选择提供基础能力。`;
  }
  if (base.includes('plan')) {
    return `定义${domain.label}的计划节点及代价、行数估算和扫描接口，供优化与执行阶段复用。`;
  }
  const mainSymbols = [...classes, ...names].slice(0, 4);
  const detail = mainSymbols.length > 0 ? `，核心结构包括 ${mainSymbols.join('、')}` : '';
  return `提供${domain.label}相关的数据结构与处理逻辑${detail}。`;
}

function docSummary(result) {
  const sections = result.sections || [];
  const title = sections.find((item) => item.level === 1)?.heading || path.basename(result.path);
  const topics = sections
    .filter((item) => item.level === 2)
    .slice(0, 4)
    .map((item) => item.heading.replace(/^\d+(?:\.\d+)*[.、\s]*/, ''));
  const detail = topics.length > 0 ? `，覆盖${topics.join('、')}` : '';
  return `规划文档《${title}》共整理 ${sections.length} 个章节${detail}。`;
}

function functionSummary(name, filePath) {
  const domain = domainInfo(filePath).label;
  if (name.startsWith('Test')) return `验证 ${cleanSymbol(name)} 的正常路径、错误处理或边界行为。`;
  if (name.startsWith('Benchmark')) return `衡量 ${cleanSymbol(name)} 场景下的执行耗时与资源开销。`;
  if (name.startsWith('New')) return `创建并初始化 ${name.slice(3) || '目标对象'}，设置其运行所需的依赖和默认状态。`;
  if (name.startsWith('Get')) return `读取 ${domain} 中的 ${name.slice(3) || '目标值'} 并返回统一结果。`;
  if (name.startsWith('Set')) return `更新 ${domain} 中的 ${name.slice(3) || '目标状态'}。`;
  if (name.startsWith('Build')) return `根据输入元数据或计划节点构建 ${name.slice(5) || '目标结构'}。`;
  if (name.startsWith('Parse')) return `解析输入并生成 ${name.slice(5) || domain + '结构'}，同时处理格式与边界校验。`;
  if (name.startsWith('Execute')) return `执行 ${name.slice(7) || domain + '操作'}，协调所需的计划、存储或事务步骤。`;
  if (name.startsWith('Optimize')) return `优化 ${name.slice(8) || domain + '结构'}，在保持语义的前提下降低执行代价。`;
  if (name.startsWith('Estimate')) return `估算 ${name.slice(8) || domain + '代价或基数'}，为计划选择提供统计依据。`;
  if (name.startsWith('Collect')) return `采集 ${name.slice(7) || domain + '统计信息'} 并整理为优化器可用的数据。`;
  if (name.startsWith('Normalize')) return `规范化 ${name.slice(9) || domain + '输入'}，消除等价表示差异。`;
  if (name.startsWith('Convert') || name.startsWith('To')) return `转换 ${domain} 中的数据表示，生成调用方需要的目标格式。`;
  if (name.startsWith('Read')) return `从底层存储读取 ${name.slice(4) || '数据'}，并完成必要的解码或可见性处理。`;
  if (name.startsWith('Write')) return `将 ${name.slice(5) || '数据'} 写入底层结构，并维护相关状态。`;
  if (name.startsWith('Scan')) return `扫描 ${domain} 管理的数据范围，并按计划返回匹配记录。`;
  if (name === 'Open') return `初始化当前 ${domain} 组件的执行状态并准备后续读取。`;
  if (name === 'Next') return `推进当前 ${domain} 迭代过程并返回下一条结果。`;
  if (name === 'Close') return `释放当前 ${domain} 组件持有的资源并结束生命周期。`;
  return `实现 ${domain} 中的 ${name} 操作，封装该步骤的核心判断与状态变更。`;
}

function classSummary(name, filePath) {
  const domain = domainInfo(filePath).label;
  if (/interface/i.test(name)) return `定义 ${domain} 的 ${name} 接口，约束实现方必须提供的行为。`;
  if (/operator/i.test(name)) return `封装 ${domain} 的 ${name} 算子状态与执行行为。`;
  if (/manager/i.test(name)) return `集中管理 ${domain} 中的 ${name} 生命周期、状态与资源协调。`;
  if (/plan/i.test(name)) return `表示 ${domain} 中的 ${name} 计划节点及其估算或执行信息。`;
  return `定义 ${domain} 使用的 ${name} 结构，保存该组件的核心状态和依赖。`;
}

function functionTags(name, filePath) {
  const domain = domainInfo(filePath).tag;
  if (name.startsWith('Test')) return ['test', 'validation', domain];
  if (name.startsWith('Benchmark')) return ['benchmark', 'performance', domain];
  if (name.startsWith('New')) return ['factory', 'constructor', domain];
  if (/^(Parse|Normalize|Validate)/.test(name)) return ['validation', 'transformation', domain];
  if (/^(Build|Optimize|Estimate|Collect|Analyze)/.test(name)) return ['query-planning', 'algorithm', domain];
  if (/^(Read|Write|Scan|Insert|Update|Delete)/.test(name)) return ['data-access', 'storage', domain];
  return ['method', 'go', domain];
}

function classTags(name, filePath) {
  const domain = domainInfo(filePath).tag;
  if (/interface/i.test(name)) return ['type-definition', 'interface', domain];
  if (/operator/i.test(name)) return ['operator', 'execution', domain];
  if (/manager/i.test(name)) return ['service', 'resource-management', domain];
  return ['data-model', 'go', domain];
}

function fileTags(result) {
  const domain = domainInfo(result.path).tag;
  const base = path.basename(result.path);
  if (base.endsWith('_test.go')) {
    const tags = ['test', 'go', domain];
    if (base.includes('bench')) tags.splice(1, 0, 'benchmark');
    return tags.slice(0, 5);
  }
  const tags = ['go', domain];
  if (base === 'mysql_server.go') tags.push('network-server');
  else if (base.includes('executor')) tags.push('execution');
  else if (base.includes('optimizer')) tags.push('algorithm');
  else if (base.includes('types') || base === 'interfaces.go') tags.push('type-definition');
  else tags.push('storage-engine');
  const uniqueTags = [...new Set(tags)];
  for (const fallback of ['storage-engine', 'implementation', 'component']) {
    if (uniqueTags.length >= 3) break;
    if (!uniqueTags.includes(fallback)) uniqueTags.push(fallback);
  }
  return uniqueTags.slice(0, 5);
}

function createFileNode(result) {
  if (result.fileCategory === 'docs') {
    return {
      id: `document:${result.path}`,
      type: 'document',
      name: path.basename(result.path),
      filePath: result.path,
      summary: docSummary(result),
      tags: ['documentation', 'planning', 'production-readiness'],
      complexity: complexityFromLines(result.nonEmptyLines),
    };
  }
  return {
    id: `file:${result.path}`,
    type: 'file',
    name: path.basename(result.path),
    filePath: result.path,
    summary: fileSummary(result),
    tags: fileTags(result),
    complexity: complexityFromLines(result.nonEmptyLines),
  };
}

function uniqueByName(items) {
  const seen = new Set();
  const unique = [];
  for (const item of items) {
    if (!item?.name || seen.has(item.name)) continue;
    seen.add(item.name);
    unique.push(item);
  }
  return unique;
}

function buildCodeNodesAndEdges(result, batch) {
  const fileNode = createFileNode(result);
  const nodes = [fileNode];
  const edges = [];
  const exported = new Set((result.exports || []).map((item) => item.name));

  const functions = uniqueByName(result.functions || []).filter((item) => {
    const lineCount = item.endLine - item.startLine + 1;
    return lineCount >= 10 || exported.has(item.name) || isExportedName(item.name);
  });
  for (const item of functions) {
    const lineCount = item.endLine - item.startLine + 1;
    const id = `function:${result.path}:${item.name}`;
    nodes.push({
      id,
      type: 'function',
      name: item.name,
      filePath: result.path,
      lineRange: [item.startLine, item.endLine],
      summary: functionSummary(item.name, result.path),
      tags: functionTags(item.name, result.path),
      complexity: complexityFromLines(lineCount),
    });
    edges.push({
      source: fileNode.id,
      target: id,
      type: 'contains',
      direction: 'forward',
      weight: 1.0,
    });
    if (exported.has(item.name) || isExportedName(item.name)) {
      edges.push({
        source: fileNode.id,
        target: id,
        type: 'exports',
        direction: 'forward',
        weight: 0.8,
      });
    }
  }

  const classes = uniqueByName(result.classes || []).filter((item) => {
    const lineCount = item.endLine - item.startLine + 1;
    return lineCount >= 20 || (item.methods || []).length >= 2 || exported.has(item.name) || isExportedName(item.name);
  });
  for (const item of classes) {
    const lineCount = item.endLine - item.startLine + 1;
    const id = `class:${result.path}:${item.name}`;
    nodes.push({
      id,
      type: 'class',
      name: item.name,
      filePath: result.path,
      lineRange: [item.startLine, item.endLine],
      summary: classSummary(item.name, result.path),
      tags: classTags(item.name, result.path),
      complexity: complexityFromLines(lineCount),
    });
    edges.push({
      source: fileNode.id,
      target: id,
      type: 'contains',
      direction: 'forward',
      weight: 1.0,
    });
    if (exported.has(item.name) || isExportedName(item.name)) {
      edges.push({
        source: fileNode.id,
        target: id,
        type: 'exports',
        direction: 'forward',
        weight: 0.8,
      });
    }
  }

  for (const targetPath of batch.batchImportData[result.path] || []) {
    edges.push({
      source: fileNode.id,
      target: `file:${targetPath}`,
      type: 'imports',
      direction: 'forward',
      weight: 0.7,
    });
  }

  return { nodes, edges };
}

const docRelations = [
  ['P0_A_BASE_IMPLEMENTATION_PLAN.md', 'P0_A1_02_PAGE_SINGLE_ENTRYPOINT_MIGRATION.md'],
  ['P0_B_RECOVERY_EVIDENCE_MANIFEST.md', 'P0_PRODUCTION_DEPLOYMENT_PLAN.md'],
  ['P0_D_OBSERVABILITY_READINESS_TEMPLATE.md', 'P0_PRODUCTION_CHECKLIST.md'],
  ['P0_E_ROLLBACK_AND_CANARY_RUNBOOK.md', 'P0_E_CANARY_REHEARSAL_20260517.md'],
  ['P0_PRODUCTION_DEPLOYMENT_PLAN.md', 'P0_PRODUCTION_CHECKLIST.md'],
  ['P0_PRODUCTION_DEPLOYMENT_PLAN.md', 'P0_WEEK1_SPRINT_PLAN.md'],
  ['P0_PRODUCTION_GAP_ANALYSIS.md', 'PRODUCTION_GAP_LIST.md'],
  ['P0_PRODUCTION_GAP_ANALYSIS.md', 'PRODUCTION_GAP_EXECUTION_TABLE.md'],
  ['P0_PRODUCTION_TASKS.md', 'PRIORITY_TASK_LIST.md'],
  ['P0_PRODUCTION_TASKS.md', 'TODO_DETAILED_CHECKLIST.md'],
  ['P0_STAGE1_BASELINE_REPORT_TEMPLATE.md', 'P0_A_BASE_IMPLEMENTATION_PLAN.md'],
  ['TODO_DETAILED_CHECKLIST.md', 'TODO_EXECUTION_PLAN.md'],
  ['TODO_DETAILED_CHECKLIST.md', 'TODO_STATISTICS_REPORT.md'],
  ['TODO_DETAILED_CHECKLIST.md', 'TODO_SUMMARY.md'],
  ['UNIMPLEMENTED_CAPABILITY_BASELINE_2026-04.md', 'PRODUCTION_GAP_LIST.md'],
];

function buildBatch(batchIndex) {
  const batch = batchesByIndex.get(batchIndex);
  if (!batch) throw new Error(`Missing batch ${batchIndex} in batches.json`);
  const extraction = JSON.parse(
    fs.readFileSync(path.join(tmpDir, `ua-file-extract-results-${batchIndex}.json`), 'utf8'),
  );
  if (extraction.scriptCompleted !== true) {
    throw new Error(`Extractor did not complete for batch ${batchIndex}`);
  }
  if (extraction.results.length !== batch.files.length) {
    throw new Error(`Batch ${batchIndex} result count mismatch: ${extraction.results.length} != ${batch.files.length}`);
  }

  const nodes = [];
  const edges = [];
  for (const result of extraction.results) {
    if (result.fileCategory === 'code') {
      const fragment = buildCodeNodesAndEdges(result, batch);
      nodes.push(...fragment.nodes);
      edges.push(...fragment.edges);
    } else {
      nodes.push(createFileNode(result));
    }
  }

  if (batchIndex === 39) {
    const byBaseName = new Map(
      extraction.results.map((result) => [path.basename(result.path), result.path]),
    );
    for (const [sourceName, targetName] of docRelations) {
      const sourcePath = byBaseName.get(sourceName);
      const targetPath = byBaseName.get(targetName);
      if (!sourcePath || !targetPath) continue;
      edges.push({
        source: `document:${sourcePath}`,
        target: `document:${targetPath}`,
        type: 'related',
        direction: 'forward',
        weight: 0.5,
      });
    }
  }

  const nodeIds = new Set();
  for (const node of nodes) {
    if (nodeIds.has(node.id)) throw new Error(`Duplicate node in batch ${batchIndex}: ${node.id}`);
    nodeIds.add(node.id);
  }

  const expectedImports = Object.values(batch.batchImportData)
    .reduce((sum, targets) => sum + targets.length, 0);
  const actualImports = edges.filter((edge) => edge.type === 'imports').length;
  if (actualImports !== expectedImports) {
    throw new Error(`Batch ${batchIndex} import mismatch: ${actualImports} != ${expectedImports}`);
  }

  return {
    batch,
    extraction,
    nodes,
    edges,
    expectedImports,
  };
}

function removeOldOutputs(batchIndex) {
  const exact = new RegExp(`^batch-${batchIndex}(?:-part-\\d+)?\\.json$`);
  for (const name of fs.readdirSync(intermediateDir)) {
    if (exact.test(name)) fs.unlinkSync(path.join(intermediateDir, name));
  }
}

function writeParts(batchIndex, built) {
  removeOldOutputs(batchIndex);
  const { batch, nodes, edges } = built;
  if (nodes.length <= 60 && edges.length <= 120) {
    const outputPath = path.join(intermediateDir, `batch-${batchIndex}.json`);
    fs.writeFileSync(outputPath, `${JSON.stringify({ nodes, edges }, null, 2)}\n`);
    return [outputPath];
  }

  const requestedParts = Math.ceil(Math.max(nodes.length / 60, edges.length / 120));
  const sortedFiles = [...batch.files].sort((a, b) => a.path.localeCompare(b.path));
  const chunkSize = Math.max(1, Math.ceil(sortedFiles.length / requestedParts));
  const outputPaths = [];
  let partIndex = 1;
  for (let offset = 0; offset < sortedFiles.length; offset += chunkSize) {
    const filePaths = new Set(sortedFiles.slice(offset, offset + chunkSize).map((item) => item.path));
    const partNodes = nodes.filter((node) => filePaths.has(node.filePath));
    const partNodeIds = new Set(partNodes.map((node) => node.id));
    const partEdges = edges.filter((edge) => partNodeIds.has(edge.source));
    const outputPath = path.join(intermediateDir, `batch-${batchIndex}-part-${partIndex}.json`);
    fs.writeFileSync(outputPath, `${JSON.stringify({ nodes: partNodes, edges: partEdges }, null, 2)}\n`);
    outputPaths.push(outputPath);
    partIndex += 1;
  }
  return outputPaths;
}

function validatePart(outputPath, batch) {
  const fragment = JSON.parse(fs.readFileSync(outputPath, 'utf8'));
  if (!Array.isArray(fragment.nodes) || !Array.isArray(fragment.edges)) {
    throw new Error(`${outputPath}: nodes/edges must be arrays`);
  }
  const partNodeIds = new Set(fragment.nodes.map((node) => node.id));
  const importPaths = new Set(Object.values(batch.batchImportData).flat());
  const neighborEntries = Object.values(batch.neighborMap || {}).flat();
  const neighborPaths = new Set(neighborEntries.map((entry) => entry.path));
  const neighborSymbols = new Map(
    neighborEntries.map((entry) => [entry.path, new Set(entry.symbols || [])]),
  );
  for (const edge of fragment.edges) {
    const sourceValid = partNodeIds.has(edge.source);
    let targetValid = partNodeIds.has(edge.target);
    const fileMatch = edge.target.match(/^file:(.+)$/);
    if (fileMatch && (importPaths.has(fileMatch[1]) || neighborPaths.has(fileMatch[1]))) {
      targetValid = true;
    }
    const symbolMatch = edge.target.match(/^(?:function|class):(.+):([^:]+)$/);
    if (symbolMatch && neighborSymbols.get(symbolMatch[1])?.has(symbolMatch[2])) {
      targetValid = true;
    }
    if (!sourceValid || !targetValid || edge.source === edge.target) {
      throw new Error(`${outputPath}: invalid edge ${JSON.stringify(edge)}`);
    }
  }
  return { nodes: fragment.nodes.length, edges: fragment.edges.length };
}

const report = [];
for (const batchIndex of assigned) {
  const built = buildBatch(batchIndex);
  const outputPaths = writeParts(batchIndex, built);
  let nodeCount = 0;
  let edgeCount = 0;
  for (const outputPath of outputPaths) {
    const counts = validatePart(outputPath, built.batch);
    nodeCount += counts.nodes;
    edgeCount += counts.edges;
  }
  if (nodeCount !== built.nodes.length || edgeCount !== built.edges.length) {
    throw new Error(`Batch ${batchIndex} split totals mismatch`);
  }
  report.push({
    batchIndex,
    parts: outputPaths.map((outputPath) => path.basename(outputPath)),
    nodes: nodeCount,
    edges: edgeCount,
    imports: built.expectedImports,
    skipped: built.extraction.filesSkipped,
  });
}

process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
