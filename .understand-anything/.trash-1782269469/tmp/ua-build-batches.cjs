#!/usr/bin/env node
const fs = require('fs');
const path = require('path');
const { spawnSync } = require('child_process');

const projectRoot = process.argv[2];
const skillDir = process.argv[3];
if (!projectRoot || !skillDir) {
  console.error('Usage: ua-build-batches.cjs <projectRoot> <skillDir>');
  process.exit(1);
}

const interDir = path.join(projectRoot, '.understand-anything', 'intermediate');
const tmpDir = path.join(projectRoot, '.understand-anything', 'tmp');
const scan = JSON.parse(fs.readFileSync(path.join(interDir, 'scan-result.json'), 'utf8'));
const batches = JSON.parse(fs.readFileSync(path.join(interDir, 'batches.json'), 'utf8')).batches;

const inputPath = path.join(tmpDir, 'ua-file-analyzer-input-all.json');
const extractPath = path.join(tmpDir, 'ua-file-extract-results-all.json');
fs.writeFileSync(inputPath, JSON.stringify({
  projectRoot,
  batchFiles: scan.files,
  batchImportData: scan.importMap,
}, null, 2));

const extract = spawnSync('node', [
  path.join(skillDir, 'extract-structure.mjs'),
  inputPath,
  extractPath,
], { cwd: projectRoot, encoding: 'utf8' });

if (extract.stdout) process.stdout.write(extract.stdout);
if (extract.stderr) process.stderr.write(extract.stderr);
if (extract.status !== 0) process.exit(extract.status || 1);
if (!fs.existsSync(extractPath) || fs.statSync(extractPath).size === 0) {
  console.error(`Missing extract output: ${extractPath}`);
  process.exit(1);
}

const extracted = JSON.parse(fs.readFileSync(extractPath, 'utf8'));
const byPath = new Map(extracted.results.map(r => [r.path, r]));
const fileMeta = new Map(scan.files.map(f => [f.path, f]));
const exportedByFile = new Map();

for (const result of extracted.results) {
  const exported = new Set((result.exports || []).map(e => e.name));
  exportedByFile.set(result.path, exported);
}

function idPrefix(file) {
  if (file.fileCategory === 'config') return 'config';
  if (file.fileCategory === 'docs') return 'document';
  if (file.fileCategory === 'infra') {
    if (file.path.includes('.github/workflows/') || file.path.includes('.circleci/') || file.path.endsWith('.gitlab-ci.yml') || path.basename(file.path) === 'Jenkinsfile') return 'pipeline';
    if (file.language === 'terraform' || path.basename(file.path) === 'Vagrantfile') return 'resource';
    return 'service';
  }
  if (file.fileCategory === 'data') {
    if (['graphql', 'protobuf', 'prisma'].includes(file.language)) return 'schema';
    return 'table';
  }
  return 'file';
}

function complexity(result) {
  const n = result.nonEmptyLines ?? result.totalLines ?? 0;
  const m = result.metrics || {};
  const structural = (m.functionCount || 0) + (m.classCount || 0) + (m.sectionCount || 0);
  if (n > 200 || structural > 20) return 'complex';
  if (n >= 50 || structural > 5) return 'moderate';
  return 'simple';
}

function tagsFor(file, result) {
  const tags = [];
  const base = path.basename(file.path).toLowerCase();
  if (file.fileCategory === 'docs') tags.push('documentation');
  if (file.fileCategory === 'config') tags.push('configuration');
  if (file.fileCategory === 'infra') tags.push(idPrefix(file) === 'pipeline' ? 'ci-cd' : 'infrastructure');
  if (file.fileCategory === 'script') tags.push('script');
  if (file.path.endsWith('_test.go') || base.includes('test')) tags.push('test');
  if (base === 'main.go' || file.path === 'main.go') tags.push('entry-point');
  if (file.path.includes('/protocol/')) tags.push('mysql-protocol');
  if (file.path.includes('/innodb/')) tags.push('innodb');
  if (file.path.includes('/storage/')) tags.push('storage');
  if (file.path.includes('/transaction') || file.path.includes('/txn') || base.includes('transaction')) tags.push('transaction');
  if (file.path.includes('/manager/')) tags.push('manager');
  if (file.path.includes('/engine/')) tags.push('execution-engine');
  if ((result.functions || []).length && tags.length < 3) tags.push('logic');
  if (!tags.length) tags.push(file.language || 'source');
  while (tags.length < 3) tags.push(file.fileCategory === 'code' ? 'code' : file.fileCategory);
  return [...new Set(tags)].slice(0, 5);
}

function fileSummary(file, result) {
  const rel = file.path;
  const base = path.basename(rel);
  if (file.fileCategory === 'docs') return `${base} 是项目文档，说明 ${rel.split('/').slice(0, -1).join('/') || '仓库'} 相关设计、计划或使用方式。`;
  if (file.fileCategory === 'config') return `${base} 提供项目配置，影响构建、运行参数或工具行为。`;
  if (file.fileCategory === 'infra') return `${base} 描述项目的 CI、构建或部署相关流程。`;
  if (file.fileCategory === 'script') return `${base} 是自动化脚本，用于构建、清理、验证或运维流程。`;
  if (rel === 'main.go') return '项目主入口，负责加载配置、初始化 MySQL 兼容服务并启动监听流程。';
  if (rel.startsWith('server/net/')) return `${base} 处理网络连接与 MySQL 协议请求入口，是客户端交互路径的一部分。`;
  if (rel.startsWith('server/protocol/')) return `${base} 实现 MySQL 协议编解码、包结构或命令处理相关逻辑。`;
  if (rel.startsWith('server/innodb/')) return `${base} 属于 InnoDB 风格内核实现，参与存储、事务、索引、执行器或元数据管理。`;
  if (rel.startsWith('server/dispatcher/')) return `${base} 负责 SQL 请求分发、路由或执行链路编排。`;
  if (rel.startsWith('cmd/')) return `${base} 是命令行演示或验证入口，用于验证特定模块行为。`;
  if (rel.startsWith('util/')) return `${base} 提供通用工具能力，供服务端或测试代码复用。`;
  if (rel.startsWith('client/')) return `${base} 属于客户端辅助程序，用于连接或验证服务端行为。`;
  return `${base} 是 ${file.language} 文件，属于 ${rel.split('/')[0]} 模块。`;
}

function symbolSummary(kind, name, file) {
  if (kind === 'class') return `${name} 定义在 ${file.path} 中，承载该文件的核心数据结构或接口约定。`;
  return `${name} 定义在 ${file.path} 中，封装该文件中的一段可复用逻辑。`;
}

function node(id, type, name, filePath, summary, tags, comp) {
  return { id, type, name, filePath, summary, tags, complexity: comp };
}

function edge(source, target, type, weight) {
  return { source, target, type, direction: 'forward', weight };
}

for (const batch of batches) {
  const nodes = [];
  const edges = [];
  for (const file of batch.files) {
    const result = byPath.get(file.path) || {
      path: file.path,
      language: file.language,
      fileCategory: file.fileCategory,
      totalLines: file.sizeLines,
      nonEmptyLines: file.sizeLines,
      metrics: {},
    };
    const prefix = idPrefix(file);
    const fileId = `${prefix}:${file.path}`;
    const comp = complexity(result);
    nodes.push(node(fileId, prefix, path.basename(file.path), file.path, fileSummary(file, result), tagsFor(file, result), comp));

    for (const targetPath of (scan.importMap[file.path] || [])) {
      const targetMeta = fileMeta.get(targetPath) || { path: targetPath, fileCategory: 'code' };
      edges.push(edge(fileId, `${idPrefix(targetMeta)}:${targetPath}`, 'imports', 0.7));
    }

    const exported = exportedByFile.get(file.path) || new Set();
    for (const fn of (result.functions || [])) {
      const len = Math.max(0, (fn.endLine || 0) - (fn.startLine || 0) + 1);
      const isExported = exported.has(fn.name) || /^[A-Z]/.test(fn.name);
      if (!isExported && len < 10) continue;
      const fnId = `function:${file.path}:${fn.name}`;
      nodes.push(node(fnId, 'function', fn.name, file.path, symbolSummary('function', fn.name, file), ['function', isExported ? 'exported' : 'internal', file.language || 'code'], len > 80 ? 'complex' : len >= 20 ? 'moderate' : 'simple'));
      edges.push(edge(fileId, fnId, 'contains', 1.0));
      if (isExported) edges.push(edge(fileId, fnId, 'exports', 0.8));
    }

    for (const cls of (result.classes || [])) {
      const len = Math.max(0, (cls.endLine || 0) - (cls.startLine || 0) + 1);
      const methodCount = (cls.methods || []).length;
      const isExported = exported.has(cls.name) || /^[A-Z]/.test(cls.name);
      if (!isExported && methodCount < 2 && len < 20) continue;
      const clsId = `class:${file.path}:${cls.name}`;
      nodes.push(node(clsId, 'class', cls.name, file.path, symbolSummary('class', cls.name, file), ['type', isExported ? 'exported' : 'internal', file.language || 'code'], len > 120 ? 'complex' : len >= 30 ? 'moderate' : 'simple'));
      edges.push(edge(fileId, clsId, 'contains', 1.0));
      if (isExported) edges.push(edge(fileId, clsId, 'exports', 0.8));
    }

    for (const svc of (result.services || [])) {
      const svcId = `service:${file.path}:${svc.name}`;
      nodes.push(node(svcId, 'service', svc.name, file.path, `${svc.name} 是 ${file.path} 中定义的服务或构建阶段。`, ['service', 'infrastructure', file.language || 'config'], 'simple'));
      edges.push(edge(fileId, svcId, 'contains', 1.0));
    }

    for (const res of (result.resources || [])) {
      const resId = `resource:${file.path}:${res.name}`;
      nodes.push(node(resId, 'resource', res.name, file.path, `${res.name} 是 ${file.path} 中定义的基础设施资源。`, ['resource', 'infrastructure', res.kind || 'resource'], 'simple'));
      edges.push(edge(fileId, resId, 'contains', 1.0));
    }
  }
  fs.writeFileSync(path.join(interDir, `batch-${batch.batchIndex}.json`), JSON.stringify({ nodes, edges }, null, 2));
  console.log(`batch-${batch.batchIndex}.json nodes=${nodes.length} edges=${edges.length}`);
}
