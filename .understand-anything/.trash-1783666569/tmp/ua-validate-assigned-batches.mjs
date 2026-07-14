import fs from 'node:fs';
import path from 'node:path';

const root = '/Users/zhukovasky/GolandProjects/xmysql-server';
const intermediate = path.join(root, '.understand-anything', 'intermediate');
const tmp = path.join(root, '.understand-anything', 'tmp');
const assigned = [5, 8, 11, 16];
const batches = JSON.parse(fs.readFileSync(path.join(intermediate, 'batches.json'), 'utf8')).batches;
const validNodeTypes = new Set(['file', 'function', 'class', 'config', 'document', 'service', 'table', 'endpoint', 'pipeline', 'schema', 'resource']);
const validComplexity = new Set(['simple', 'moderate', 'complex']);
const validEdgeTypes = new Set(['contains', 'imports', 'calls', 'inherits', 'implements', 'exports', 'depends_on', 'tested_by', 'configures', 'documents', 'deploys', 'migrates', 'triggers', 'defines_schema', 'serves', 'provisions', 'routes', 'related']);
const report = [];
let hasErrors = false;

function externalRefAllowed(id, allowedPaths, neighborSymbols) {
  if (id.startsWith('file:')) return allowedPaths.has(id.slice(5));
  const match = id.match(/^(function|class):(.+):([^:]+)$/);
  if (!match) return false;
  return neighborSymbols.get(match[2])?.has(match[3]) || false;
}

for (const batchIndex of assigned) {
  const batch = batches.find((item) => item.batchIndex === batchIndex);
  const prefix = `batch-${batchIndex}`;
  const names = fs.readdirSync(intermediate)
    .filter((name) => new RegExp(`^${prefix}(?:-part-[0-9]+)?\\.json$`).test(name))
    .sort((a, b) => a.localeCompare(b, undefined, {numeric: true}));
  const errors = [];
  const allNodes = [];
  const allEdges = [];
  const fileOccurrences = new Map();
  const allowedPaths = new Set();
  const neighborSymbols = new Map();
  for (const imports of Object.values(batch.batchImportData || {})) {
    for (const importedPath of imports) allowedPaths.add(importedPath);
  }
  for (const neighbors of Object.values(batch.neighborMap || {})) {
    for (const neighbor of neighbors) {
      allowedPaths.add(neighbor.path);
      if (!neighborSymbols.has(neighbor.path)) neighborSymbols.set(neighbor.path, new Set());
      for (const symbol of neighbor.symbols || []) neighborSymbols.get(neighbor.path).add(symbol);
    }
  }

  if (names.length === 0) errors.push('没有找到输出文件');
  for (const name of names) {
    let fragment;
    try {
      fragment = JSON.parse(fs.readFileSync(path.join(intermediate, name), 'utf8'));
    } catch (error) {
      errors.push(`${name}: JSON 无法解析: ${error.message}`);
      continue;
    }
    if (!Array.isArray(fragment.nodes)) errors.push(`${name}: nodes 不是数组`);
    if (!Array.isArray(fragment.edges)) errors.push(`${name}: edges 不是数组`);
    if (!Array.isArray(fragment.nodes) || !Array.isArray(fragment.edges)) continue;
    const partIds = new Set(fragment.nodes.map((node) => node.id));
    for (const node of fragment.nodes) {
      allNodes.push(node);
      if (!node.id || !node.name || !node.summary) errors.push(`${name}: 节点缺少 id/name/summary`);
      if (!validNodeTypes.has(node.type)) errors.push(`${name}: 非法节点类型 ${node.type}`);
      if (!Array.isArray(node.tags) || node.tags.length < 3) errors.push(`${name}: 节点 ${node.id} tags 少于 3 个`);
      if (!validComplexity.has(node.complexity)) errors.push(`${name}: 节点 ${node.id} complexity 非法`);
      if (node.filePath) fileOccurrences.set(node.filePath, (fileOccurrences.get(node.filePath) || 0) + (node.type === 'file' ? 1 : 0));
    }
    for (const edge of fragment.edges) {
      allEdges.push(edge);
      if (!validEdgeTypes.has(edge.type)) errors.push(`${name}: 非法边类型 ${edge.type}`);
      if (edge.direction !== 'forward') errors.push(`${name}: 边方向不是 forward`);
      if (edge.source === edge.target) errors.push(`${name}: 自引用边 ${edge.source}`);
      for (const [side, ref] of [['source', edge.source], ['target', edge.target]]) {
        if (!partIds.has(ref) && !externalRefAllowed(ref, allowedPaths, neighborSymbols)) {
          errors.push(`${name}: ${side} ${ref} 不在当前 part 或批次外部上下文中`);
        }
      }
    }
  }

  const ids = new Set();
  for (const node of allNodes) {
    if (ids.has(node.id)) errors.push(`重复节点 ID: ${node.id}`);
    ids.add(node.id);
  }
  for (const file of batch.files) {
    const count = fileOccurrences.get(file.path) || 0;
    if (count !== 1) errors.push(`文件节点覆盖异常 ${file.path}: ${count}`);
  }

  const importEdges = allEdges.filter((edge) => edge.type === 'imports');
  const expectedImports = Object.values(batch.batchImportData || {}).reduce((sum, imports) => sum + imports.length, 0);
  if (importEdges.length !== expectedImports) errors.push(`imports 数量 ${importEdges.length} != ${expectedImports}`);
  for (const file of batch.files) {
    const expected = batch.batchImportData?.[file.path] || [];
    const actual = importEdges.filter((edge) => edge.source === `file:${file.path}`).map((edge) => edge.target.slice(5));
    if (JSON.stringify(actual) !== JSON.stringify(expected)) {
      errors.push(`${file.path}: imports 未按 batchImportData 逐项输出`);
    }
  }

  const extraction = JSON.parse(fs.readFileSync(path.join(tmp, `ua-file-extract-results-${batchIndex}.json`), 'utf8'));
  if (extraction.filesAnalyzed !== batch.files.length) errors.push(`提取文件数 ${extraction.filesAnalyzed} != ${batch.files.length}`);
  if ((extraction.filesSkipped || []).length > 0) errors.push(`存在跳过文件: ${(extraction.filesSkipped || []).join(', ')}`);

  report.push({
    batchIndex,
    outputFiles: names.length,
    files: batch.files.length,
    nodes: allNodes.length,
    edges: allEdges.length,
    imports: importEdges.length,
    skipped: extraction.filesSkipped || [],
    errors,
  });
  if (errors.length) hasErrors = true;
}

fs.writeFileSync(path.join(tmp, 'ua-assigned-batches-validation.json'), `${JSON.stringify(report, null, 2)}\n`);
console.log(JSON.stringify(report, null, 2));
if (hasErrors) process.exit(1);
