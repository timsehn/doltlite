/*
** Prolly tree hashing: SHA-512 based content addressing (first 20 bytes,
** matching Dolt) + rolling hash splitter.
** Part of the Doltite prolly tree storage engine.
*/
#ifndef SQLITE_PROLLY_HASH_H
#define SQLITE_PROLLY_HASH_H

#include "sqliteInt.h"

/* Hash output size: 20 bytes (matching Dolt's hash) */
#define PROLLY_HASH_SIZE 20

/* Hash type */
typedef struct ProllyHash ProllyHash;
struct ProllyHash {
  u8 data[PROLLY_HASH_SIZE];
};

/* Content hash: SHA1-like hash over arbitrary data */
void prollyHashCompute(const void *pData, int nData, ProllyHash *pOut);

/* Compare two hashes. Returns <0, 0, >0 like memcmp */
int prollyHashCompare(const ProllyHash *a, const ProllyHash *b);

/* Check if hash is all zeros */
int prollyHashIsEmpty(const ProllyHash *h);

/*
** Rolling hash splitter for chunk boundaries.
** Uses a window-based rolling hash to determine where to split
** nodes during tree construction.
*/
typedef struct ProllyRollingHash ProllyRollingHash;
struct ProllyRollingHash {
  u32 hash;           /* Current rolling hash value */
  int windowSize;     /* Size of the rolling window */
  int pos;            /* Current position in window buffer */
  int filled;         /* How many bytes have been fed */
  u8 *window;         /* Circular buffer [windowSize] */
};

/* Initialize rolling hash with given window size */
int prollyRollingHashInit(ProllyRollingHash *rh, int windowSize);

/* Feed one byte, returns updated hash */
u32 prollyRollingHashUpdate(ProllyRollingHash *rh, u8 byte);

/* Check if current hash crosses the boundary pattern.
** pattern is a bitmask; boundary when (hash & pattern) == pattern */
int prollyRollingHashAtBoundary(ProllyRollingHash *rh, u32 pattern);

/* Reset rolling hash state */
void prollyRollingHashReset(ProllyRollingHash *rh);

/* Free rolling hash resources */
void prollyRollingHashFree(ProllyRollingHash *rh);

#endif /* SQLITE_PROLLY_HASH_H */
