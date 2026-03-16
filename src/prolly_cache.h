/*
** LRU node cache with reference counting for prolly tree nodes.
** Caches deserialized ProllyNode structs keyed by content hash.
*/
#ifndef SQLITE_PROLLY_CACHE_H
#define SQLITE_PROLLY_CACHE_H

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "prolly_node.h"

typedef struct ProllyCache ProllyCache;
typedef struct ProllyCacheEntry ProllyCacheEntry;

struct ProllyCacheEntry {
  ProllyHash hash;           /* Content hash (lookup key) */
  u8 *pData;                 /* Owned copy of serialized node data */
  int nData;                 /* Size of serialized data */
  ProllyNode node;           /* Parsed node (points into pData) */
  int nRef;                  /* Reference count */
  ProllyCacheEntry *pLruNext; /* LRU doubly-linked list */
  ProllyCacheEntry *pLruPrev;
  ProllyCacheEntry *pHashNext; /* Hash bucket chain */
};

struct ProllyCache {
  int nCapacity;              /* Max entries */
  int nUsed;                  /* Current entries */
  int nBucket;                /* Hash table bucket count */
  ProllyCacheEntry **aBucket; /* Hash table */
  ProllyCacheEntry lruHead;   /* LRU sentinel (most recent) */
  ProllyCacheEntry lruTail;   /* LRU sentinel (least recent) */
};

/* Create cache with given capacity. Returns SQLITE_OK or SQLITE_NOMEM. */
int prollyCacheInit(ProllyCache *cache, int nCapacity);

/* Look up a node by hash. Increments refcount if found.
** Returns NULL if not in cache. */
ProllyCacheEntry *prollyCacheGet(ProllyCache *cache, const ProllyHash *hash);

/* Insert node data into cache. Returns the cache entry with refcount=1.
** If already present, returns existing entry with incremented refcount.
** Evicts LRU entries with refcount==0 if at capacity. */
ProllyCacheEntry *prollyCachePut(ProllyCache *cache,
                                  const ProllyHash *hash,
                                  const u8 *pData, int nData);

/* Release a reference to a cache entry */
void prollyCacheRelease(ProllyCache *cache, ProllyCacheEntry *entry);

/* Remove all unreferenced entries */
void prollyCachePurge(ProllyCache *cache);

/* Free all cache resources. Asserts all refcounts are 0. */
void prollyCacheFree(ProllyCache *cache);

#endif /* SQLITE_PROLLY_CACHE_H */
