/*
** Three-way catalog merge with conflict detection for Doltlite.
**
** Given three catalog hashes (ancestor, ours, theirs), produce a merged
** catalog. For each table:
**   - If only one side changed it, take that side's root hash.
**   - If both sides changed the same table, return SQLITE_ERROR (conflict).
**   - If a table was added on one side and doesn't exist on the other, include it.
**   - If deleted on one side and unchanged on the other, delete it.
**
** Returns the merged catalog as a serialized chunk stored in the chunk store.
*/
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "chunk_store.h"
#include "doltlite_commit.h"

#include <string.h>

/* TableEntry struct — must match prolly_btree.c definition */
struct TableEntry {
  Pgno iTable;
  ProllyHash root;
  u8 flags;
  char *zName;
};

/* Provided by prolly_btree.c */
extern ChunkStore *doltliteGetChunkStore(sqlite3 *db);
extern int doltliteLoadCatalog(sqlite3 *db, const ProllyHash *catHash,
                               struct TableEntry **ppTables, int *pnTables,
                               Pgno *piNextTable);

/*
** Find a table entry by table number in an array of entries.
** Returns a pointer to the entry, or NULL if not found.
*/
static struct TableEntry *findTableEntry(
  struct TableEntry *aEntries,
  int nEntries,
  Pgno iTable
){
  int i;
  for(i=0; i<nEntries; i++){
    if( aEntries[i].iTable==iTable ) return &aEntries[i];
  }
  return 0;
}

/*
** Find a table entry by NAME. This is the primary merge key —
** two branches may have different iTable numbers for the same table.
*/
static struct TableEntry *findTableByName(
  struct TableEntry *aEntries,
  int nEntries,
  const char *zName
){
  int i;
  if( !zName ) return 0;
  for(i=0; i<nEntries; i++){
    if( aEntries[i].zName && strcmp(aEntries[i].zName, zName)==0 ){
      return &aEntries[i];
    }
  }
  return 0;
}

/*
** Serialize a merged catalog into a chunk and store it.
**
** Format: iNextTable(4) + nTables(4) + meta(64) + entries(25 each)
**
** Meta is copied from the "ours" catalog (the current branch's metadata
** takes precedence in a merge).
*/
static int serializeMergedCatalog(
  sqlite3 *db,
  const ProllyHash *oursCatHash,     /* For copying meta header */
  struct TableEntry *aMerged,
  int nMerged,
  Pgno iNextTable,
  ProllyHash *pOutHash               /* OUT: hash of stored catalog chunk */
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  int sz = 4 + 4 + 64;
  { int j; for(j=0;j<nMerged;j++){
    int nl = aMerged[j].zName ? (int)strlen(aMerged[j].zName) : 0;
    sz += 4+1+PROLLY_HASH_SIZE+2+nl;
  }}
  u8 *buf;
  u8 *p;
  int rc;

  buf = sqlite3_malloc(sz);
  if( !buf ) return SQLITE_NOMEM;
  p = buf;

  /* Copy iNextTable and meta from ours catalog */
  {
    u8 *oursData = 0;
    int nOursData = 0;
    rc = chunkStoreGet(cs, oursCatHash, &oursData, &nOursData);
    if( rc==SQLITE_OK && nOursData>=72 ){
      memcpy(p, oursData, 72); /* iNextTable(4) + nTables(4) + meta(64) */
    }else{
      memset(p, 0, 72);
    }
    sqlite3_free(oursData);
  }

  /* Override iNextTable */
  p[0] = (u8)iNextTable;
  p[1] = (u8)(iNextTable>>8);
  p[2] = (u8)(iNextTable>>16);
  p[3] = (u8)(iNextTable>>24);

  /* Override nTables */
  p[4] = (u8)nMerged;
  p[5] = (u8)(nMerged>>8);
  p[6] = (u8)(nMerged>>16);
  p[7] = (u8)(nMerged>>24);

  p += 72;

  /* Write table entries: iTable(4) + flags(1) + root(20) + name_len(2) + name(var) */
  {
    int i;
    for(i=0; i<nMerged; i++){
      Pgno pg = aMerged[i].iTable;
      int nl = aMerged[i].zName ? (int)strlen(aMerged[i].zName) : 0;
      p[0] = (u8)pg;
      p[1] = (u8)(pg>>8);
      p[2] = (u8)(pg>>16);
      p[3] = (u8)(pg>>24);
      p += 4;
      *p++ = aMerged[i].flags;
      memcpy(p, aMerged[i].root.data, PROLLY_HASH_SIZE);
      p += PROLLY_HASH_SIZE;
      p[0]=(u8)nl; p[1]=(u8)(nl>>8); p+=2;
      if(nl>0) memcpy(p, aMerged[i].zName, nl);
      p += nl;
    }
  }

  rc = chunkStorePut(cs, buf, sz, pOutHash);
  sqlite3_free(buf);
  return rc;
}

/*
** Perform a three-way catalog merge.
**
** ancestor: the common ancestor catalog hash
** ours:     the current branch's catalog hash
** theirs:   the other branch's catalog hash
**
** On success, *pMergedHash contains the hash of the merged catalog chunk.
** Returns SQLITE_OK on success, SQLITE_ERROR on conflict.
*/
int doltliteMergeCatalogs(
  sqlite3 *db,
  const ProllyHash *ancestor,
  const ProllyHash *ours,
  const ProllyHash *theirs,
  ProllyHash *pMergedHash
){
  struct TableEntry *aAnc = 0, *aOurs = 0, *aTheirs = 0;
  int nAnc = 0, nOurs = 0, nTheirs = 0;
  Pgno iNextAnc = 2, iNextOurs = 2, iNextTheirs = 2;
  struct TableEntry *aMerged = 0;
  int nMerged = 0;
  int nMergedAlloc = 0;
  Pgno iNextMerged;
  int rc;
  int i;

  /* Load all three catalogs */
  rc = doltliteLoadCatalog(db, ancestor, &aAnc, &nAnc, &iNextAnc);
  if( rc!=SQLITE_OK ) goto merge_cleanup;

  rc = doltliteLoadCatalog(db, ours, &aOurs, &nOurs, &iNextOurs);
  if( rc!=SQLITE_OK ) goto merge_cleanup;

  rc = doltliteLoadCatalog(db, theirs, &aTheirs, &nTheirs, &iNextTheirs);
  if( rc!=SQLITE_OK ) goto merge_cleanup;

  /* Allocate merged array — worst case is all tables from both sides */
  nMergedAlloc = nOurs + nTheirs;
  if( nMergedAlloc==0 ) nMergedAlloc = 1;
  aMerged = sqlite3_malloc(nMergedAlloc * (int)sizeof(struct TableEntry));
  if( !aMerged ){
    rc = SQLITE_NOMEM;
    goto merge_cleanup;
  }

  /* Use ours' iNextTable as the starting point, take max with theirs */
  iNextMerged = iNextOurs > iNextTheirs ? iNextOurs : iNextTheirs;

  /*
  ** NAME-BASED THREE-WAY MERGE
  **
  ** Tables are matched by NAME (zName), not by iTable number.
  ** This handles the case where two branches independently CREATE TABLE
  ** and get different iTable numbers for tables with different names.
  **
  ** Output uses ours' iTable numbers. Theirs' tables that don't exist
  ** in ours get new iTable numbers from iNextMerged.
  */

  /* Pass 1: Process all tables from "ours" */
  for(i=0; i<nOurs; i++){
    const char *zName = aOurs[i].zName;
    struct TableEntry *ancEntry;
    struct TableEntry *theirsEntry;

    /* Skip internal tables (no name, or table 1 = sqlite_master) */
    if( aOurs[i].iTable<=1 || !zName ){
      aMerged[nMerged++] = aOurs[i];
      continue;
    }

    /* Find by NAME in ancestor and theirs */
    ancEntry = findTableByName(aAnc, nAnc, zName);
    theirsEntry = findTableByName(aTheirs, nTheirs, zName);

    if( !ancEntry ){
      /* Table added in ours (not in ancestor by name) */
      if( theirsEntry ){
        /* Both sides added a table with the same name */
        if( prollyHashCompare(&aOurs[i].root, &theirsEntry->root)!=0 ){
          /* Different content — conflict */
          rc = SQLITE_ERROR;
          goto merge_cleanup;
        }
        /* Identical content — include once (ours' iTable) */
      }
      aMerged[nMerged++] = aOurs[i];
    }else{
      /* Table existed in ancestor */
      int oursChanged = prollyHashCompare(&aOurs[i].root, &ancEntry->root)!=0;

      if( !theirsEntry ){
        /* Deleted in theirs (by name) */
        if( oursChanged ){
          rc = SQLITE_ERROR;  /* Modified in ours, deleted in theirs */
          goto merge_cleanup;
        }
        /* Unchanged in ours, deleted in theirs — delete (skip) */
      }else{
        int theirsChanged = prollyHashCompare(&theirsEntry->root, &ancEntry->root)!=0;
        if( oursChanged && theirsChanged ){
          rc = SQLITE_ERROR;  /* Both modified same table — conflict */
          goto merge_cleanup;
        }else if( theirsChanged ){
          /* Take theirs' root but with ours' iTable number */
          struct TableEntry merged = aOurs[i];
          memcpy(&merged.root, &theirsEntry->root, sizeof(ProllyHash));
          aMerged[nMerged++] = merged;
        }else{
          aMerged[nMerged++] = aOurs[i];
        }
      }
    }
  }

  /* Pass 2: Add tables from "theirs" that don't exist in "ours" by name */
  for(i=0; i<nTheirs; i++){
    const char *zName = aTheirs[i].zName;
    struct TableEntry *oursEntry;

    if( aTheirs[i].iTable<=1 || !zName ) continue;

    oursEntry = findTableByName(aOurs, nOurs, zName);
    if( oursEntry ) continue;  /* Already handled in pass 1 */

    {
      struct TableEntry *ancEntry = findTableByName(aAnc, nAnc, zName);
      if( !ancEntry ){
        /* Added in theirs only — assign a new iTable number */
        struct TableEntry newEntry = aTheirs[i];
        newEntry.iTable = iNextMerged++;
        /* Copy name string */
        newEntry.zName = sqlite3_mprintf("%s", zName);
        aMerged[nMerged++] = newEntry;
      }else{
        /* Was in ancestor, deleted in ours */
        int theirsChanged = prollyHashCompare(&aTheirs[i].root, &ancEntry->root)!=0;
        if( theirsChanged ){
          rc = SQLITE_ERROR;  /* Modified in theirs, deleted in ours */
          goto merge_cleanup;
        }
        /* Unchanged in theirs, deleted in ours — delete (skip) */
      }
    }
  }

  /* Serialize the merged catalog */
  rc = serializeMergedCatalog(db, ours, aMerged, nMerged, iNextMerged,
                              pMergedHash);

merge_cleanup:
  sqlite3_free(aAnc);
  sqlite3_free(aOurs);
  sqlite3_free(aTheirs);
  sqlite3_free(aMerged);
  return rc;
}

#endif /* DOLTLITE_PROLLY */
