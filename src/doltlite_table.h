/*
** Shared TableEntry definition for Doltlite.
** Used by prolly_btree.c, doltlite.c, doltlite_merge.c, etc.
*/
#ifndef DOLTLITE_TABLE_H
#define DOLTLITE_TABLE_H

#include "prolly_hash.h"

typedef u32 Pgno;

struct TableEntry {
  Pgno iTable;          /* Rootpage / table number */
  ProllyHash root;      /* Root hash of the table's prolly tree */
  u8 flags;             /* BTREE_INTKEY or BTREE_BLOBKEY */
  char *zName;          /* Table name (owned, may be NULL for internal tables) */
};

#endif /* DOLTLITE_TABLE_H */
