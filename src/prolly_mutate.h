/*
** Merge-flush: apply buffered edits from MutMap into an existing prolly tree.
** This is the hardest piece — it merges sorted edits with the existing
** tree's leaf entries, then rebuilds the tree using the chunker.
*/
#ifndef SQLITE_PROLLY_MUTATE_H
#define SQLITE_PROLLY_MUTATE_H

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "prolly_node.h"
#include "prolly_cursor.h"
#include "prolly_mutmap.h"
#include "prolly_chunker.h"
#include "chunk_store.h"
#include "prolly_cache.h"

typedef struct ProllyMutator ProllyMutator;

struct ProllyMutator {
  ChunkStore *pStore;
  ProllyCache *pCache;
  ProllyHash oldRoot;       /* Root of the existing tree */
  ProllyMutMap *pEdits;     /* Pending edits */
  u8 flags;                 /* INTKEY or BLOBKEY */
  ProllyHash newRoot;       /* Result root after flush */
};

/*
** Apply all edits in pEdits to the tree rooted at oldRoot.
** Produces a new tree (newRoot) with edits applied.
** The old tree's chunks are preserved (structural sharing).
**
** Algorithm:
**   1. Open cursor on existing tree
**   2. Open iterator on edit map
**   3. Merge-walk both in sorted order:
**      - Existing entries not touched by edits pass through
**      - INSERT edits replace or add entries
**      - DELETE edits skip existing entries
**   4. Feed merged stream to chunker to build new tree
**   5. Return new root hash
**
** Returns SQLITE_OK on success with result in pMut->newRoot.
*/
int prollyMutateFlush(ProllyMutator *pMut);

/*
** Convenience: apply a single insert to a tree.
** Builds a 1-entry MutMap internally.
*/
int prollyMutateInsert(ChunkStore *pStore, ProllyCache *pCache,
                       const ProllyHash *pRoot, u8 flags,
                       const u8 *pKey, int nKey, i64 intKey,
                       const u8 *pVal, int nVal,
                       ProllyHash *pNewRoot);

/*
** Convenience: apply a single delete to a tree.
*/
int prollyMutateDelete(ChunkStore *pStore, ProllyCache *pCache,
                       const ProllyHash *pRoot, u8 flags,
                       const u8 *pKey, int nKey, i64 intKey,
                       ProllyHash *pNewRoot);

#endif /* SQLITE_PROLLY_MUTATE_H */
