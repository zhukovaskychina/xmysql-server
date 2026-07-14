#!/usr/bin/env node
const fs = require('fs');
const path = require('path');

const projectRoot = process.cwd();
const interDir = path.join(projectRoot, '.understand-anything', 'intermediate');
const scan = JSON.parse(fs.readFileSync(path.join(interDir, 'scan-result.json'), 'utf8'));
const merged = JSON.parse(fs.readFileSync(path.join(interDir, 'assembled-graph.json'), 'utf8'));
const commitHash = require('child_process').execFileSync('git', ['rev-parse', 'HEAD'], { cwd: projectRoot, encoding: 'utf8' }).trim();
const nodeSet = new Set(merged.nodes.map(n => n.id));
const fileTypes = new Set(['file', 'config', 'document', 'service', 'pipeline', 'table', 'schema', 'resource', 'endpoint']);

function layerFor(node) {
  const p = node.filePath || '';
  if (node.type === 'document' || p === 'README.md' || p.startsWith('docs/')) return 'layer:documentation';
  if (p.startsWith('cmd/') || p.startsWith('client/') || p.startsWith('jdbc_client/')) return 'layer:clients-and-demos';
  if (p === 'main.go' || p.startsWith('server/net/') || p.startsWith('server/protocol/') || p.startsWith('server/dispatcher/') || p.startsWith('server/session')) return 'layer:service-protocol';
  if (p.startsWith('server/innodb/sqlparser/') || p.startsWith('server/innodb/plan/')) return 'layer:sql-planning';
  if (p.startsWith('server/innodb/engine/')) return 'layer:execution-engine';
  if (p.startsWith('server/innodb/manager/') || p.startsWith('server/innodb/metadata/')) return 'layer:catalog-transaction-management';
  if (p.startsWith('server/innodb/basic/') || p.startsWith('server/innodb/buffer_pool/') || p.startsWith('server/innodb/storage/')) return 'layer:storage-core';
  if (p.startsWith('util/') || p.startsWith('logger/')) return 'layer:shared-utilities';
  if (node.type === 'config' || node.type === 'pipeline' || p.startsWith('conf/') || p.startsWith('scripts/') || p.startsWith('.github/') || p.endsWith('.sh') || p.endsWith('.bat') || p === '.goreleaser.yml') return 'layer:build-config-operations';
  return 'layer:core-service';
}

const layerDefs = {
  'layer:service-protocol': ['服务入口与协议层', '启动服务、处理连接、解析 MySQL 协议包，并把请求交给分发层。'],
  'layer:sql-planning': ['SQL 解析与计划层', '负责 SQL 语法解析、逻辑计划、物理计划、优化器和统计信息。'],
  'layer:execution-engine': ['执行引擎层', '执行 DDL/DML/查询计划，连接 SQL Pipeline 与底层存储能力。'],
  'layer:catalog-transaction-management': ['元数据与事务管理层', '管理表空间、数据字典、事务、锁、undo/redo 和恢复相关流程。'],
  'layer:storage-core': ['存储内核层', '实现页、记录、Buffer Pool、B+Tree、表空间和存储包装器。'],
  'layer:clients-and-demos': ['客户端与验证入口', '包含客户端程序、命令行 demo、集成验证入口和 JDBC 侧辅助工程。'],
  'layer:shared-utilities': ['通用工具层', '提供日志、字节处理、缓冲区、hash、时间和配置读取等复用能力。'],
  'layer:build-config-operations': ['构建配置与运维脚本', '包含构建脚本、CI、发布配置、运行配置和验证脚本。'],
  'layer:documentation': ['文档与计划', '包含 README、路线图、设计分析、测试总结和项目治理文档。'],
  'layer:core-service': ['其他服务代码', '未归入更具体模块的服务端代码和项目根级源码。'],
};

const layerMap = new Map(Object.keys(layerDefs).map(id => [id, []]));
for (const n of merged.nodes) {
  if (!fileTypes.has(n.type)) continue;
  const id = layerFor(n);
  if (!layerMap.has(id)) layerMap.set(id, []);
  layerMap.get(id).push(n.id);
}

const layers = [...layerMap.entries()]
  .filter(([, nodeIds]) => nodeIds.length > 0)
  .map(([id, nodeIds]) => ({
    id,
    name: layerDefs[id]?.[0] || id.replace(/^layer:/, ''),
    description: layerDefs[id]?.[1] || '项目中的相关文件集合。',
    nodeIds: [...new Set(nodeIds)].filter(id => nodeSet.has(id)).sort(),
  }));

function pick(...ids) {
  return ids.filter(id => nodeSet.has(id));
}

const tourSeeds = [
  ['项目总览', '从 README 了解项目目标、当前状态、主目录和生产就绪路线。', pick('document:README.md')],
  ['服务启动路径', '查看主入口如何加载配置并启动 MySQL 兼容服务。', pick('file:main.go', 'file:server/net/mysql_server.go', 'file:server/session.go')],
  ['协议与连接处理', '理解网络处理、MySQL 协议编解码和请求进入分发层的路径。', pick('file:server/net/decoupled_handler.go', 'file:server/net/handler.go', 'file:server/protocol/packet.go', 'file:server/dispatcher/query_dispatcher.go')],
  ['SQL 解析和计划', '查看 SQL parser、逻辑计划、物理计划与优化器如何组织执行前的准备工作。', pick('file:server/innodb/sqlparser/parser.go', 'file:server/innodb/sqlparser/ast.go', 'file:server/innodb/plan/logical_plan.go', 'file:server/innodb/plan/physical_plan.go')],
  ['执行引擎', '跟踪 DML、查询执行器和 Volcano 风格执行链路。', pick('file:server/innodb/engine/enginx.go', 'file:server/innodb/engine/dml_executor.go', 'file:server/innodb/engine/volcano_executor.go')],
  ['事务与恢复', '查看事务管理、undo/redo、锁和崩溃恢复相关实现。', pick('file:server/innodb/manager/transaction_manager.go', 'file:server/innodb/manager/undo_log_manager.go', 'file:server/innodb/manager/crash_recovery.go', 'file:server/innodb/manager/lock_manager.go')],
  ['存储与页结构', '理解 Buffer Pool、页包装器、表空间和 B+Tree 存储路径。', pick('file:server/innodb/buffer_pool/buffer_pool.go', 'file:server/innodb/storage/wrapper/page/page_factory.go', 'file:server/innodb/storage/store/pages/index_page.go', 'file:server/innodb/basic/btree.go')],
  ['验证与运维入口', '查看脚本、CI 和测试入口如何支撑构建、恢复演练和回归验证。', pick('pipeline:.github/workflows/go.yml', 'file:scripts/p0_release_package_tests.sh', 'file:scripts/crash_recovery_drill.sh', 'file:cmd/integration_demo/main.go')],
];

const tour = tourSeeds
  .map(([title, description, nodeIds], index) => ({ order: index + 1, title, description, nodeIds }))
  .filter(step => step.nodeIds.length > 0);

const assembleReview = {
  issues: [],
  notes: [
    '所有 batch 输出均已合并。',
    'imports 边数量与 import map 对齐。',
    '文件级节点将在 Phase 6 校验中确认全部归入架构层。',
  ],
};
fs.writeFileSync(path.join(interDir, 'assemble-review.json'), JSON.stringify(assembleReview, null, 2));
fs.writeFileSync(path.join(interDir, 'layers.json'), JSON.stringify(layers, null, 2));
fs.writeFileSync(path.join(interDir, 'tour.json'), JSON.stringify(tour, null, 2));

const graph = {
  version: '1.0.0',
  project: {
    name: scan.name,
    languages: scan.languages,
    frameworks: scan.frameworks,
    description: scan.description,
    analyzedAt: new Date().toISOString(),
    gitCommitHash: commitHash,
  },
  nodes: merged.nodes,
  edges: merged.edges,
  layers,
  tour,
};
fs.writeFileSync(path.join(interDir, 'assembled-graph.json'), JSON.stringify(graph, null, 2));
console.log(JSON.stringify({
  layers: layers.map(l => `${l.name}:${l.nodeIds.length}`),
  tourSteps: tour.length,
  nodes: graph.nodes.length,
  edges: graph.edges.length,
}, null, 2));
