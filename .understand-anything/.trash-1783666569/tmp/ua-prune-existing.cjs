#!/usr/bin/env node
const fs = require('fs');

const [graphPath, changedPath, outputPath] = process.argv.slice(2);
const graph = JSON.parse(fs.readFileSync(graphPath, 'utf8'));
const changed = new Set(
  fs.readFileSync(changedPath, 'utf8').split(/\r?\n/).map((line) => line.trim()).filter(Boolean),
);

const removedIds = new Set(
  graph.nodes.filter((node) => node.filePath && changed.has(node.filePath)).map((node) => node.id),
);
const nodes = graph.nodes.filter((node) => !removedIds.has(node.id));
const edges = graph.edges.filter(
  (edge) => !removedIds.has(edge.source) && !removedIds.has(edge.target),
);

fs.writeFileSync(outputPath, JSON.stringify({nodes, edges}, null, 2));
process.stdout.write(JSON.stringify({removedNodes: removedIds.size, keptNodes: nodes.length, keptEdges: edges.length}));
