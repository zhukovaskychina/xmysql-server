import fs from 'node:fs';
import path from 'node:path';

function fail(message) {
  process.stderr.write(`${message}\n`);
  process.exit(1);
}

const [inputPath, outputPath] = process.argv.slice(2);
if (!inputPath || !outputPath) fail('Usage: node ua-arch-analyze.mjs <input.json> <output.json>');

try {
  const input = JSON.parse(fs.readFileSync(inputPath, 'utf8'));
  const fileNodes = Array.isArray(input.fileNodes) ? input.fileNodes : [];
  const importEdges = Array.isArray(input.importEdges) ? input.importEdges : [];
  const allEdges = Array.isArray(input.allEdges) ? input.allEdges : [];
  if (fileNodes.length === 0) fail('fileNodes is empty');

  const nodeById = new Map(fileNodes.map((node) => [node.id, node]));
  const pathSegments = fileNodes.map((node) => (node.filePath || '').split('/').filter(Boolean));
  let commonSegments = [...pathSegments[0].slice(0, -1)];
  for (const segments of pathSegments.slice(1)) {
    let index = 0;
    while (index < commonSegments.length && index < segments.length - 1 && commonSegments[index] === segments[index]) index += 1;
    commonSegments = commonSegments.slice(0, index);
  }
  const commonPrefix = commonSegments.length ? `${commonSegments.join('/')}/` : '';

  function directoryGroup(node) {
    const relative = commonPrefix && node.filePath.startsWith(commonPrefix)
      ? node.filePath.slice(commonPrefix.length)
      : node.filePath;
    const segments = relative.split('/').filter(Boolean);
    if (segments.length <= 1) {
      const lower = (node.filePath || '').toLowerCase();
      if (/(_test\.go|\.test\.|\.spec\.|test_.*\.py|test.*\.java)$/.test(lower)) return 'test';
      if (/\.(md|rst)$/.test(lower)) return 'documentation';
      if (/^(dockerfile|makefile)$/.test(path.basename(lower))) return 'infrastructure';
      if (/\.(ya?ml|json|toml|ini|cnf|mod|sum)$/.test(lower)) return 'config';
      return 'root';
    }
    return segments[0];
  }

  const directoryGroups = {};
  const nodeTypeGroups = {};
  const groupById = new Map();
  for (const node of fileNodes) {
    const group = directoryGroup(node);
    groupById.set(node.id, group);
    (directoryGroups[group] ||= []).push(node.id);
    (nodeTypeGroups[node.type] ||= []).push(node.id);
  }

  const fanIn = Object.fromEntries(fileNodes.map((node) => [node.id, 0]));
  const fanOut = Object.fromEntries(fileNodes.map((node) => [node.id, 0]));
  const adjacency = Object.fromEntries(fileNodes.map((node) => [node.id, []]));
  const interGroupCounter = new Map();
  const groupRelations = new Map();
  const internalByGroup = Object.fromEntries(Object.keys(directoryGroups).map((group) => [group, 0]));
  const involvingByGroup = Object.fromEntries(Object.keys(directoryGroups).map((group) => [group, 0]));

  for (const edge of importEdges) {
    if (!nodeById.has(edge.source) || !nodeById.has(edge.target)) continue;
    fanOut[edge.source] += 1;
    fanIn[edge.target] += 1;
    adjacency[edge.source].push(edge.target);
    const from = groupById.get(edge.source);
    const to = groupById.get(edge.target);
    involvingByGroup[from] += 1;
    if (to !== from) involvingByGroup[to] += 1;
    if (from === to) internalByGroup[from] += 1;
    const key = `${from}\u0000${to}`;
    interGroupCounter.set(key, (interGroupCounter.get(key) || 0) + 1);
    if (!groupRelations.has(from)) groupRelations.set(from, {importsFrom: new Set(), importedBy: new Set()});
    if (!groupRelations.has(to)) groupRelations.set(to, {importsFrom: new Set(), importedBy: new Set()});
    if (from !== to) {
      groupRelations.get(from).importsFrom.add(to);
      groupRelations.get(to).importedBy.add(from);
    }
  }

  const interGroupImports = [...interGroupCounter.entries()]
    .filter(([key]) => key.split('\u0000')[0] !== key.split('\u0000')[1])
    .map(([key, count]) => {
      const [from, to] = key.split('\u0000');
      return {from, to, count};
    })
    .sort((a, b) => b.count - a.count || a.from.localeCompare(b.from) || a.to.localeCompare(b.to));

  const intraGroupDensity = {};
  for (const group of Object.keys(directoryGroups)) {
    const totalEdges = involvingByGroup[group] || 0;
    intraGroupDensity[group] = {
      internalEdges: internalByGroup[group] || 0,
      totalEdges,
      density: totalEdges ? Number(((internalByGroup[group] || 0) / totalEdges).toFixed(4)) : 0,
    };
  }

  const patternTable = [
    [/^(routes|api|controllers|endpoints|handlers|router|routers)$/, 'api'],
    [/^(services|core|lib|domain|logic|internal)$/, 'service'],
    [/^(models|db|data|persistence|repository|entities|entity|sql|database|schema)$/, 'data'],
    [/^(middleware|plugins|interceptors|guards)$/, 'middleware'],
    [/^(utils|helpers|common|shared|tools|pkg)$/, 'utility'],
    [/^(config|constants|env|settings|conf)$/, 'config'],
    [/^(__tests__|test|tests|spec|specs)$/, 'test'],
    [/^(types|interfaces|schemas|contracts|dtos|dto|request|response)$/, 'types'],
    [/^(cmd|bin)$/, 'entry'],
    [/^(docs|documentation|wiki)$/, 'documentation'],
    [/^(deploy|deployment|infra|infrastructure|docker|k8s|kubernetes|helm|charts|terraform|tf)$/, 'infrastructure'],
    [/^(\.github|\.gitlab|\.circleci)$/, 'ci-cd'],
  ];
  const patternMatches = {};
  for (const group of Object.keys(directoryGroups)) {
    const lower = group.toLowerCase();
    patternMatches[group] = patternTable.find(([pattern]) => pattern.test(lower))?.[1] || 'unclassified';
  }
  for (const node of fileNodes) {
    const lower = (node.filePath || '').toLowerCase();
    if (/(_test\.go|\.test\.|\.spec\.|test_.*\.py|test.*\.java)$/.test(lower)) patternMatches[node.filePath] = 'test';
    else if (/^cmd\/[^/]+\/main\.go$/.test(lower) || /(^|\/)application\.java$/.test(lower)) patternMatches[node.filePath] = 'entry';
    else if (/^(go\.mod|go\.sum|pom\.xml|build\.gradle)$/.test(lower)) patternMatches[node.filePath] = 'config';
    else if (/\.(md|rst)$/.test(lower)) patternMatches[node.filePath] = 'documentation';
    else if (/(^|\/)(dockerfile|makefile)$/.test(lower)) patternMatches[node.filePath] = 'infrastructure';
    else if (/^\.github\/workflows\//.test(lower)) patternMatches[node.filePath] = 'ci-cd';
    else if (/\.(sql)$/.test(lower)) patternMatches[node.filePath] = 'data';
    else if (/\.(graphql|gql|proto)$/.test(lower)) patternMatches[node.filePath] = 'types';
  }

  const crossCategoryCounter = new Map();
  const nonCodeConnections = [];
  for (const edge of allEdges) {
    const source = nodeById.get(edge.source);
    const target = nodeById.get(edge.target);
    if (!source || !target) continue;
    const key = `${source.type}\u0000${target.type}\u0000${edge.type}`;
    crossCategoryCounter.set(key, (crossCategoryCounter.get(key) || 0) + 1);
    if (source.type !== 'file' || target.type !== 'file') {
      nonCodeConnections.push({source: edge.source, target: edge.target, type: edge.type});
    }
  }
  const crossCategoryEdges = [...crossCategoryCounter.entries()].map(([key, count]) => {
    const [fromType, toType, edgeType] = key.split('\u0000');
    return {fromType, toType, edgeType, count};
  }).sort((a, b) => b.count - a.count);

  const lowerPaths = fileNodes.map((node) => (node.filePath || '').toLowerCase());
  const infraFiles = fileNodes.filter((node) => {
    const lower = (node.filePath || '').toLowerCase();
    return ['service', 'resource', 'pipeline'].includes(node.type) || /(^|\/)(dockerfile|makefile)$/.test(lower) || /docker-compose|k8s|kubernetes|terraform|\.github\/workflows/.test(lower);
  }).map((node) => node.filePath);
  const deploymentTopology = {
    hasDockerfile: lowerPaths.some((item) => /(^|\/)dockerfile$/.test(item)),
    hasCompose: lowerPaths.some((item) => /docker-compose\.ya?ml$/.test(item)),
    hasK8s: lowerPaths.some((item) => /(^|\/)(k8s|kubernetes|helm|charts)\//.test(item)),
    hasTerraform: lowerPaths.some((item) => /\.tf(vars)?$/.test(item)),
    hasCI: lowerPaths.some((item) => /^\.github\/workflows\//.test(item) || /\.gitlab-ci\.yml$|jenkinsfile$/.test(item)),
    infraFiles,
  };

  function nodesMatching(predicate) {
    return fileNodes.filter(predicate).map((node) => node.filePath);
  }
  const dataPipeline = {
    schemaFiles: nodesMatching((node) => ['schema', 'table', 'endpoint'].includes(node.type) || /\.(sql|graphql|gql|proto)$/.test((node.filePath || '').toLowerCase())),
    migrationFiles: nodesMatching((node) => /(^|\/)(migrations?|migrate)(\/|_)/.test((node.filePath || '').toLowerCase())),
    dataModelFiles: nodesMatching((node) => /(^|\/)(model|models|metadata|schema|record|row|value)(\/|_|\.|$)/.test((node.filePath || '').toLowerCase()) || (node.tags || []).some((tag) => /data-model|数据模型|schema/.test(tag))),
    apiHandlerFiles: nodesMatching((node) => /(^|\/)(api|handler|handlers|dispatcher|server)(\/|_|\.|$)/.test((node.filePath || '').toLowerCase()) || (node.tags || []).includes('api-handler')),
  };

  const docGroups = new Set();
  for (const node of fileNodes) {
    if (node.type === 'document' || /\.(md|rst)$/.test((node.filePath || '').toLowerCase())) docGroups.add(groupById.get(node.id));
  }
  const groupNames = Object.keys(directoryGroups);
  const undocumentedGroups = groupNames.filter((group) => !docGroups.has(group));
  const docCoverage = {
    groupsWithDocs: docGroups.size,
    totalGroups: groupNames.length,
    coverageRatio: groupNames.length ? Number((docGroups.size / groupNames.length).toFixed(4)) : 0,
    undocumentedGroups,
  };

  const directionalPairs = new Map();
  for (const relation of interGroupImports) {
    const pair = [relation.from, relation.to].sort();
    const key = pair.join('\u0000');
    const entry = directionalPairs.get(key) || {a: pair[0], b: pair[1], aToB: 0, bToA: 0};
    if (relation.from === entry.a) entry.aToB += relation.count;
    else entry.bToA += relation.count;
    directionalPairs.set(key, entry);
  }
  const dependencyDirection = [...directionalPairs.values()].map((entry) => entry.aToB >= entry.bToA
    ? {dependent: entry.a, dependsOn: entry.b, count: entry.aToB, reverseCount: entry.bToA}
    : {dependent: entry.b, dependsOn: entry.a, count: entry.bToA, reverseCount: entry.aToB})
    .sort((a, b) => b.count - a.count);

  const filesPerGroup = Object.fromEntries(Object.entries(directoryGroups).map(([group, ids]) => [group, ids.length]));
  const nodeTypeCounts = Object.fromEntries(Object.entries(nodeTypeGroups).map(([type, ids]) => [type, ids.length]));
  const directoryDependencies = Object.fromEntries([...groupRelations.entries()].map(([group, value]) => [group, {
    importsFrom: [...value.importsFrom].sort(),
    importedBy: [...value.importedBy].sort(),
  }]));

  const result = {
    scriptCompleted: true,
    commonPrefix,
    directoryGroups,
    nodeTypeGroups,
    importAdjacency: adjacency,
    directoryDependencies,
    crossCategoryEdges,
    nonCodeConnections,
    interGroupImports,
    intraGroupDensity,
    patternMatches,
    deploymentTopology,
    dataPipeline,
    docCoverage,
    dependencyDirection,
    fileStats: {totalFileNodes: fileNodes.length, filesPerGroup, nodeTypeCounts},
    fileFanIn: fanIn,
    fileFanOut: fanOut,
  };
  fs.writeFileSync(outputPath, `${JSON.stringify(result, null, 2)}\n`);
} catch (error) {
  fail(error.stack || error.message);
}
