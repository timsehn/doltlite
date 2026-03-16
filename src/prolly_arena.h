/*
** Bump allocator for prolly tree traversals.
** Allocations are freed all at once when the arena is reset.
*/
#ifndef SQLITE_PROLLY_ARENA_H
#define SQLITE_PROLLY_ARENA_H

#include "sqliteInt.h"

typedef struct ProllyArena ProllyArena;
typedef struct ProllyArenaBlock ProllyArenaBlock;

struct ProllyArenaBlock {
  ProllyArenaBlock *pNext;  /* Next block in chain */
  int sz;                   /* Usable size of this block */
  int used;                 /* Bytes consumed */
  /* data follows immediately after this struct */
};

struct ProllyArena {
  ProllyArenaBlock *pFirst;   /* First block in chain */
  ProllyArenaBlock *pCurrent; /* Current block for allocations */
  int defaultBlockSize;       /* Default size for new blocks */
};

/* Initialize arena with default block size */
void prollyArenaInit(ProllyArena *a, int defaultBlockSize);

/* Allocate n bytes from arena. Returns NULL on OOM. */
void *prollyArenaAlloc(ProllyArena *a, int n);

/* Allocate and zero n bytes */
void *prollyArenaAllocZero(ProllyArena *a, int n);

/* Reset arena: free all blocks except the first, reset usage */
void prollyArenaReset(ProllyArena *a);

/* Destroy arena: free all memory */
void prollyArenaFree(ProllyArena *a);

#endif /* SQLITE_PROLLY_ARENA_H */
