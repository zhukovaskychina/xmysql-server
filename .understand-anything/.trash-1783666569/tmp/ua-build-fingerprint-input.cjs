#!/usr/bin/env node
const fs = require('fs');
const [scanPath, outputPath, projectRoot, gitCommitHash] = process.argv.slice(2);
const scan = JSON.parse(fs.readFileSync(scanPath, 'utf8'));
const input = {
  projectRoot,
  sourceFilePaths: (scan.files || []).map((file) => file.path),
  gitCommitHash,
};
fs.writeFileSync(outputPath, JSON.stringify(input, null, 2));
process.stdout.write(String(input.sourceFilePaths.length));
