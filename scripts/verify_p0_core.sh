#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
GO_BIN="${GO_BIN:-/Users/zhukovasky/sdk/go1.24.3/bin/go}"

if [[ ! -x "${GO_BIN}" ]]; then
  GO_BIN="$(command -v go)"
fi

cd "${ROOT_DIR}"

BUFFER_POOL_P0='Test(BufferPagePinCount|OptimizedLRUCacheEvict(ReturnsOriginalPageIdentity|FallsBackToYoungList|SkipsPinnedPages))'
MANAGER_P0='Test(OptimizedBufferPoolManagerCountsLRUSetEvictions|BufferPoolManagerEvictPage(RemovesCleanUnpinnedPage|FlushesDirtyUnpinnedPage|SkipsPinnedPage)|EnhancedBTreeManager(RebuildIndex(ReloadsIndex|PreservesRecords)|DropIndex(RemovesMetadataCacheAndRootPage|FreesLoadedCachedPages)))'

GO_BIN="${GO_BIN}" scripts/verify_p0_engine_suites.sh
"${GO_BIN}" test ./server/innodb/buffer_pool -run "${BUFFER_POOL_P0}" -count=1 -v
"${GO_BIN}" test ./server/innodb/manager -run "${MANAGER_P0}" -count=1 -v
