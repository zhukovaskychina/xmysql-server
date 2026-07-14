#!/usr/bin/env node
const fs = require('fs');
const [outputPath, gitCommitHash, analyzedFiles] = process.argv.slice(2);
fs.writeFileSync(outputPath, JSON.stringify({
  lastAnalyzedAt: new Date().toISOString(),
  gitCommitHash,
  version: '1.0.0',
  analyzedFiles: Number(analyzedFiles),
}, null, 2));
