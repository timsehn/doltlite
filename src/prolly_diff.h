/*
** Two-tree diff for prolly trees.
** Efficiently finds differences between two trees by exploiting
** structural sharing (identical subtrees have identical hashes).
*/
#ifndef SQLITE_PROLLY_DIFF_H
#define SQLITE_PROLLY_DIFF_H

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "prolly_node.h"
#include "prolly_cursor.h"
#include "prolly_cache.h"
#include "chunk_store.h"

/* Diff change types */
#define PROLLY_DIFF_ADD     1   /* Key exists in new, not in old */
#define PROLLY_DIFF_DELETE  2   /* Key exists in old, not in new */
#define PROLLY_DIFF_MODIFY  3   /* Key exists in both, value changed */

typedef struct ProllyDiff ProllyDiff;
typedef struct ProllyDiffChange ProllyDiffChange;

struct ProllyDiffChange {
  u8 type;              /* PROLLY_DIFF_ADD, DELETE, or MODIFY */
  const u8 *pKey;       /* Key data */
  int nKey;             /* Key size */
  i64 intKey;           /* Integer key (if INTKEY) */
  const u8 *pOldVal;   /* Old value (NULL for ADD) */
  int nOldVal;
  const u8 *pNewVal;   /* New value (NULL for DELETE) */
  int nNewVal;
};

/* Callback for each difference found */
typedef int (*ProllyDiffCallback)(void *pCtx, const ProllyDiffChange *pChange);

/*
** Compute differences between two trees.
** Calls xCallback for each difference found.
** Skips subtrees where hashes match (structural sharing optimization).
*/
int prollyDiff(ChunkStore *pStore, ProllyCache *pCache,
               const ProllyHash *pOldRoot, const ProllyHash *pNewRoot,
               u8 flags, ProllyDiffCallback xCallback, void *pCtx);

#endif /* SQLITE_PROLLY_DIFF_H */
