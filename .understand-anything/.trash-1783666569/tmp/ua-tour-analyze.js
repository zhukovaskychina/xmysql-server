#!/usr/bin/env node
const fs = require('fs');
const path = require('path');

function main() {
  const inputPath = process.argv[2];
  const outputPath = process.argv[3];
  if (!inputPath || !outputPath) {
    throw new Error('Usage: node ua-tour-analyze.js <input.json> <output.json>');
  }

  const input = JSON.parse(fs.readFileSync(inputPath, 'utf8'));
  const nodes = Array.isArray(input.nodes) ? input.nodes : [];
  const edges = Array.isArray(input.edges) ? input.edges : [];
  const layers = Array.isArray(input.layers) ? input.layers : [];
  const nodeById = new Map(nodes.map((node) => [node.id, node]));

  const fanIn = new Map(nodes.map((node) => [node.id, 0]));
  const fanOut = new Map(nodes.map((node) => [node.id, 0]));
  for (const edge of edges) {
    if (fanOut.has(edge.source)) fanOut.set(edge.source, fanOut.get(edge.source) + 1);
    if (fanIn.has(edge.target)) fanIn.set(edge.target, fanIn.get(edge.target) + 1);
  }

  const rank = (counts, key) => [...counts.entries()]
    .map(([id, count]) => ({ id, [key]: count, name: nodeById.get(id)?.name || id }))
    .sort((a, b) => b[key] - a[key] || a.id.localeCompare(b.id));
  const fanInAll = rank(fanIn, 'fanIn');
  const fanOutAll = rank(fanOut, 'fanOut');
  const fanInRanking = fanInAll.slice(0, 20);
  const fanOutRanking = fanOutAll.slice(0, 20);

  const fileNodes = nodes.filter((node) => node.type === 'file');
  const fanOutValues = fileNodes.map((node) => fanOut.get(node.id) || 0).sort((a, b) => a - b);
  const fanInValues = fileNodes.map((node) => fanIn.get(node.id) || 0).sort((a, b) => a - b);
  const percentile = (values, p) => values[Math.min(values.length - 1, Math.max(0, Math.floor((values.length - 1) * p)))] || 0;
  const highFanOutThreshold = percentile(fanOutValues, 0.9);
  const lowFanInThreshold = percentile(fanInValues, 0.25);
  const codeEntryNames = new Set([
    'index.ts', 'index.js', 'main.ts', 'main.js', 'app.ts', 'app.js', 'server.ts', 'server.js',
    'mod.rs', 'main.go', 'main.py', 'main.rs', 'manage.py', 'app.py', 'wsgi.py', 'asgi.py',
    'run.py', '__main__.py', 'application.java', 'program.cs', 'config.ru', 'index.php',
    'app.swift', 'application.kt', 'main.cpp', 'main.c',
  ]);

  const entryCandidates = [];
  for (const node of nodes) {
    const filePath = node.filePath || '';
    const name = String(node.name || path.basename(filePath)).toLowerCase();
    let score = 0;
    if (node.type === 'file' && codeEntryNames.has(name)) {
      score += 3;
      const depth = filePath.split('/').filter(Boolean).length;
      if (depth <= 2) score += 1;
      if ((fanOut.get(node.id) || 0) >= highFanOutThreshold) score += 1;
      if ((fanIn.get(node.id) || 0) <= lowFanInThreshold) score += 1;
    } else if (node.type === 'document' && filePath === 'README.md') {
      score += 5;
    } else if (node.type === 'document' && filePath && !filePath.includes('/') && filePath.toLowerCase().endsWith('.md')) {
      score += 2;
    }
    if (score > 0) {
      entryCandidates.push({ id: node.id, score, name: node.name, summary: node.summary || '' });
    }
  }
  entryCandidates.sort((a, b) => {
    const aReadme = a.id === 'document:README.md' ? 1 : 0;
    const bReadme = b.id === 'document:README.md' ? 1 : 0;
    const aDepth = (nodeById.get(a.id)?.filePath || '').split('/').filter(Boolean).length;
    const bDepth = (nodeById.get(b.id)?.filePath || '').split('/').filter(Boolean).length;
    return b.score - a.score || bReadme - aReadme || aDepth - bDepth || a.id.localeCompare(b.id);
  });

  const topCodeEntry = entryCandidates.find((item) => nodeById.get(item.id)?.type === 'file');
  const adjacency = new Map();
  for (const edge of edges) {
    if (!['imports', 'calls'].includes(edge.type) || !nodeById.has(edge.source) || !nodeById.has(edge.target)) continue;
    if (!adjacency.has(edge.source)) adjacency.set(edge.source, []);
    adjacency.get(edge.source).push(edge.target);
  }
  for (const targets of adjacency.values()) targets.sort();

  const bfsTraversal = { startNode: topCodeEntry?.id || null, order: [], depthMap: {}, byDepth: {} };
  if (topCodeEntry) {
    const queue = [topCodeEntry.id];
    bfsTraversal.depthMap[topCodeEntry.id] = 0;
    for (let head = 0; head < queue.length; head += 1) {
      const current = queue[head];
      const depth = bfsTraversal.depthMap[current];
      bfsTraversal.order.push(current);
      const depthKey = String(depth);
      if (!bfsTraversal.byDepth[depthKey]) bfsTraversal.byDepth[depthKey] = [];
      bfsTraversal.byDepth[depthKey].push(current);
      for (const next of adjacency.get(current) || []) {
        if (Object.prototype.hasOwnProperty.call(bfsTraversal.depthMap, next)) continue;
        bfsTraversal.depthMap[next] = depth + 1;
        queue.push(next);
      }
    }
  }

  const compactNode = (node) => ({ id: node.id, name: node.name, type: node.type, summary: node.summary || '' });
  const nonCodeFiles = {
    documentation: nodes.filter((node) => node.type === 'document').map(compactNode),
    infrastructure: nodes.filter((node) => ['service', 'pipeline', 'resource'].includes(node.type)).map(compactNode),
    data: nodes.filter((node) => ['table', 'schema', 'endpoint'].includes(node.type)).map(compactNode),
    config: nodes.filter((node) => node.type === 'config').map(compactNode),
  };

  const structuralEdges = edges.filter((edge) => ['imports', 'calls'].includes(edge.type));
  const directedPairs = new Set(structuralEdges.map((edge) => `${edge.source}\u0000${edge.target}`));
  const undirectedAdj = new Map();
  const mutualPairs = [];
  for (const edge of structuralEdges) {
    if (!directedPairs.has(`${edge.target}\u0000${edge.source}`) || edge.source >= edge.target) continue;
    mutualPairs.push([edge.source, edge.target]);
    for (const [a, b] of [[edge.source, edge.target], [edge.target, edge.source]]) {
      if (!undirectedAdj.has(a)) undirectedAdj.set(a, new Set());
      undirectedAdj.get(a).add(b);
    }
  }

  const clusters = [];
  const seenClusterKeys = new Set();
  for (const pair of mutualPairs) {
    const cluster = new Set(pair);
    let changed = true;
    while (changed && cluster.size < 5) {
      changed = false;
      const candidateCounts = new Map();
      for (const member of cluster) {
        for (const candidate of undirectedAdj.get(member) || []) {
          if (cluster.has(candidate)) continue;
          candidateCounts.set(candidate, (candidateCounts.get(candidate) || 0) + 1);
        }
      }
      const candidate = [...candidateCounts.entries()]
        .filter(([, count]) => count >= 2)
        .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))[0];
      if (candidate) {
        cluster.add(candidate[0]);
        changed = true;
      }
    }
    const clusterNodes = [...cluster].sort();
    const key = clusterNodes.join('\u0000');
    if (seenClusterKeys.has(key)) continue;
    seenClusterKeys.add(key);
    let edgeCount = 0;
    for (const edge of structuralEdges) {
      if (cluster.has(edge.source) && cluster.has(edge.target)) edgeCount += 1;
    }
    clusters.push({ nodes: clusterNodes, edgeCount });
  }
  clusters.sort((a, b) => b.edgeCount - a.edgeCount || b.nodes.length - a.nodes.length || a.nodes[0].localeCompare(b.nodes[0]));

  const nodeSummaryIndex = {};
  for (const node of nodes) {
    nodeSummaryIndex[node.id] = { name: node.name, type: node.type, summary: node.summary || '' };
  }

  const output = {
    scriptCompleted: true,
    entryPointCandidates: entryCandidates.slice(0, 5),
    fanInRanking,
    fanOutRanking,
    bfsTraversal,
    nonCodeFiles,
    clusters: clusters.slice(0, 10),
    layers: {
      count: layers.length,
      list: layers.map(({ id, name, description }) => ({ id, name, description })),
    },
    nodeSummaryIndex,
    totalNodes: nodes.length,
    totalEdges: edges.length,
  };
  fs.writeFileSync(outputPath, `${JSON.stringify(output, null, 2)}\n`);
}

try {
  main();
} catch (error) {
  process.stderr.write(`${error.stack || error.message}\n`);
  process.exit(1);
}
