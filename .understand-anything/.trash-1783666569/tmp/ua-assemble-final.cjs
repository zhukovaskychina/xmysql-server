#!/usr/bin/env node
const fs = require('fs');

const [graphPath, layersPath, tourPath, scanPath, outputPath, gitCommitHash] = process.argv.slice(2);
const graph = JSON.parse(fs.readFileSync(graphPath, 'utf8'));
const layers = JSON.parse(fs.readFileSync(layersPath, 'utf8'));
const tour = JSON.parse(fs.readFileSync(tourPath, 'utf8'));
const scan = JSON.parse(fs.readFileSync(scanPath, 'utf8'));

const finalGraph = {
  version: '1.0.0',
  project: {
    name: scan.name || 'xmysql-server',
    languages: scan.languages || ['go'],
    frameworks: scan.frameworks || [],
    description: 'XMySQL Server 是一个使用 Go 实现的、面向单机场景的 MySQL 兼容数据库内核项目，重点覆盖存储、事务、索引、执行器与 MySQL 协议。',
    analyzedAt: new Date().toISOString(),
    gitCommitHash,
  },
  nodes: graph.nodes || [],
  edges: graph.edges || [],
  layers: Array.isArray(layers) ? layers : (layers.layers || []),
  tour: Array.isArray(tour) ? tour : (tour.steps || []),
};

fs.writeFileSync(outputPath, JSON.stringify(finalGraph, null, 2));
process.stdout.write(JSON.stringify({
  nodes: finalGraph.nodes.length,
  edges: finalGraph.edges.length,
  layers: finalGraph.layers.length,
  tour: finalGraph.tour.length,
}));
