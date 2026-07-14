import fs from 'node:fs';
import path from 'node:path';

const root = process.cwd();
const intermediate = path.join(root, '.understand-anything', 'intermediate');
const tmp = path.join(root, '.understand-anything', 'tmp');
const assigned = [17, 18, 20, 21, 39];
const batches = JSON.parse(fs.readFileSync(path.join(intermediate, 'batches.json'), 'utf8')).batches;
const validNodeTypes = new Set(['file', 'function', 'class', 'config', 'document', 'service', 'table', 'endpoint', 'pipeline', 'schema', 'resource']);
const validEdgeWeights = new Map([
  ['contains', 1.0], ['imports', 0.7], ['calls', 0.8], ['inherits', 0.9],
  ['implements', 0.9], ['exports', 0.8], ['depends_on', 0.6],
  ['tested_by', 0.5], ['configures', 0.6], ['documents', 0.5],
  ['deploys', 0.7], ['migrates', 0.7], ['triggers', 0.6],
  ['defines_schema', 0.8], ['serves', 0.7], ['provisions', 0.7],
  ['routes', 0.6], ['related', 0.5],
]);

function fail(message) {
  throw new Error(message);
}

function outputFiles(batchIndex) {
  const regex = new RegExp(`^batch-${batchIndex}(?:-part-(\\d+))?\\.json$`);
  const matches = fs.readdirSync(intermediate)
    .filter((name) => regex.test(name))
    .sort((a, b) => {
      const aPart = Number(a.match(regex)?.[1] || 0);
      const bPart = Number(b.match(regex)?.[1] || 0);
      return aPart - bPart;
    });
  if (matches.length === 0) fail(`batch ${batchIndex}: no output`);
  if (matches.includes(`batch-${batchIndex}.json`) && matches.length !== 1) {
    fail(`batch ${batchIndex}: mixed single and part outputs`);
  }
  return matches;
}

const report = [];
for (const batchIndex of assigned) {
  const batch = batches.find((item) => item.batchIndex === batchIndex);
  if (!batch) fail(`batch ${batchIndex}: missing source batch`);
  const extraction = JSON.parse(fs.readFileSync(path.join(tmp, `ua-file-extract-results-${batchIndex}.json`), 'utf8'));
  if (!extraction.scriptCompleted || extraction.results.length !== batch.files.length) {
    fail(`batch ${batchIndex}: extractor result incomplete`);
  }
  const files = outputFiles(batchIndex);
  const fragments = files.map((name) => JSON.parse(fs.readFileSync(path.join(intermediate, name), 'utf8')));
  const nodes = fragments.flatMap((fragment) => fragment.nodes || []);
  const edges = fragments.flatMap((fragment) => fragment.edges || []);
  const nodeIds = new Set();
  for (const node of nodes) {
    if (!node.id || !validNodeTypes.has(node.type) || !node.name || !node.summary) {
      fail(`batch ${batchIndex}: invalid node ${JSON.stringify(node)}`);
    }
    if (!Array.isArray(node.tags) || node.tags.length < 3 || node.tags.length > 5) {
      fail(`batch ${batchIndex}: invalid tags for ${node.id}`);
    }
    if (!node.tags.every((tag) => /^[a-z0-9]+(?:-[a-z0-9]+)*$/.test(tag))) {
      fail(`batch ${batchIndex}: non-kebab tag for ${node.id}`);
    }
    if (!['simple', 'moderate', 'complex'].includes(node.complexity)) {
      fail(`batch ${batchIndex}: invalid complexity for ${node.id}`);
    }
    if (['file', 'config', 'document', 'service', 'pipeline', 'schema', 'resource'].includes(node.type) && !node.filePath) {
      fail(`batch ${batchIndex}: missing filePath for ${node.id}`);
    }
    if (['function', 'class'].includes(node.type) && (!Array.isArray(node.lineRange) || node.lineRange.length !== 2)) {
      fail(`batch ${batchIndex}: missing lineRange for ${node.id}`);
    }
    if (nodeIds.has(node.id)) fail(`batch ${batchIndex}: duplicate node ${node.id}`);
    nodeIds.add(node.id);
  }

  const expectedPaths = new Set(batch.files.map((item) => item.path));
  const fileNodes = nodes.filter((node) => node.type === 'file' || node.type === 'document');
  const outputPaths = new Set(fileNodes.map((node) => node.filePath));
  if (outputPaths.size !== expectedPaths.size || [...expectedPaths].some((filePath) => !outputPaths.has(filePath))) {
    fail(`batch ${batchIndex}: file-node coverage mismatch`);
  }

  const expectedImports = [];
  for (const [sourcePath, targets] of Object.entries(batch.batchImportData)) {
    for (const targetPath of targets) expectedImports.push(`file:${sourcePath}\u0000file:${targetPath}`);
  }
  const actualImports = edges
    .filter((edge) => edge.type === 'imports')
    .map((edge) => `${edge.source}\u0000${edge.target}`);
  expectedImports.sort();
  actualImports.sort();
  if (JSON.stringify(actualImports) !== JSON.stringify(expectedImports)) {
    fail(`batch ${batchIndex}: imports are not 1:1 with batchImportData`);
  }

  const importPaths = new Set(Object.values(batch.batchImportData).flat());
  const neighborEntries = Object.values(batch.neighborMap || {}).flat();
  const neighborPaths = new Set(neighborEntries.map((item) => item.path));
  for (const [partIndex, fragment] of fragments.entries()) {
    if (!Array.isArray(fragment.nodes) || !Array.isArray(fragment.edges)) {
      fail(`batch ${batchIndex} part ${partIndex + 1}: malformed fragment`);
    }
    const localIds = new Set(fragment.nodes.map((node) => node.id));
    for (const edge of fragment.edges) {
      if (!edge.source || !edge.target || !validEdgeWeights.has(edge.type) || edge.direction !== 'forward') {
        fail(`batch ${batchIndex}: invalid edge ${JSON.stringify(edge)}`);
      }
      if (edge.weight !== validEdgeWeights.get(edge.type)) {
        fail(`batch ${batchIndex}: wrong edge weight ${JSON.stringify(edge)}`);
      }
      if (edge.source === edge.target || !localIds.has(edge.source)) {
        fail(`batch ${batchIndex}: invalid edge source ${JSON.stringify(edge)}`);
      }
      let targetValid = localIds.has(edge.target);
      const fileMatch = edge.target.match(/^file:(.+)$/);
      if (fileMatch && (importPaths.has(fileMatch[1]) || neighborPaths.has(fileMatch[1]))) targetValid = true;
      if (!targetValid) fail(`batch ${batchIndex}: invalid edge target ${JSON.stringify(edge)}`);
    }
  }

  const extractionByPath = new Map(extraction.results.map((result) => [result.path, result]));
  for (const file of batch.files.filter((item) => item.fileCategory === 'code')) {
    const result = extractionByPath.get(file.path);
    const exported = new Set((result.exports || []).map((item) => item.name));
    const requiredFunctions = new Set((result.functions || [])
      .filter((item) => item.endLine - item.startLine + 1 >= 10 || exported.has(item.name) || /^[A-Z]/.test(item.name))
      .map((item) => item.name));
    for (const name of requiredFunctions) {
      if (!nodeIds.has(`function:${file.path}:${name}`)) {
        fail(`batch ${batchIndex}: missing significant function ${file.path}:${name}`);
      }
    }
    const requiredClasses = new Set((result.classes || [])
      .filter((item) => item.endLine - item.startLine + 1 >= 20 || (item.methods || []).length >= 2 || exported.has(item.name) || /^[A-Z]/.test(item.name))
      .map((item) => item.name));
    for (const name of requiredClasses) {
      if (!nodeIds.has(`class:${file.path}:${name}`)) {
        fail(`batch ${batchIndex}: missing significant class ${file.path}:${name}`);
      }
    }
  }

  report.push({
    batchIndex,
    outputFiles: files.length,
    files: outputPaths.size,
    nodes: nodes.length,
    edges: edges.length,
    imports: actualImports.length,
    skipped: extraction.filesSkipped || [],
  });
}

console.log(JSON.stringify({ status: 'ok', batches: report }, null, 2));
