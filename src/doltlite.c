/*
** Doltlite version control features: dolt_commit() and dolt_log.
**
** This file is the main entry point for Dolt-specific functionality.
** It registers the SQL functions and virtual tables that provide
** Git-like version control on top of the prolly tree storage engine.
*/
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "doltlite_commit.h"
#include "prolly_hash.h"
#include "chunk_store.h"

#include <string.h>
#include <time.h>

/* Provided by prolly_btree.c */
extern ChunkStore *doltliteGetChunkStore(sqlite3 *db);
extern int doltliteFlushAndSerializeCatalog(sqlite3 *db, u8 **ppOut, int *pnOut);

/* Provided by doltlite_log.c */
extern int doltliteLogRegister(sqlite3 *db);

/* --------------------------------------------------------------------------
** dolt_commit('-m', 'message' [, '--author', 'Name <email>'])
**
** Creates a versioning commit that snapshots the current database state.
** Returns the commit hash as a 40-character hex string.
**
** Usage:
**   SELECT dolt_commit('-m', 'Initial schema and data');
**   SELECT dolt_commit('-m', 'Add users', '--author', 'Tim <tim@co.com>');
** -------------------------------------------------------------------------- */

static void doltliteCommitFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  sqlite3 *db = sqlite3_context_db_handle(context);
  ChunkStore *cs = doltliteGetChunkStore(db);
  DoltliteCommit commit;
  const char *zMessage = 0;
  const char *zAuthor = 0;
  u8 *commitData = 0;
  int nCommitData = 0;
  u8 *catData = 0;
  int nCatData = 0;
  ProllyHash commitHash;
  ProllyHash catalogHash;
  ProllyHash rootHash;
  char hexBuf[PROLLY_HASH_SIZE*2+1];
  int rc;
  int i;

  if( !cs ){
    sqlite3_result_error(context, "no database open", -1);
    return;
  }

  /* Parse arguments */
  for(i=0; i<argc; i++){
    const char *arg = (const char*)sqlite3_value_text(argv[i]);
    if( !arg ) continue;
    if( strcmp(arg, "-m")==0 && i+1<argc ){
      zMessage = (const char*)sqlite3_value_text(argv[++i]);
    }else if( strcmp(arg, "--author")==0 && i+1<argc ){
      zAuthor = (const char*)sqlite3_value_text(argv[++i]);
    }
  }

  if( !zMessage || zMessage[0]==0 ){
    sqlite3_result_error(context,
      "dolt_commit requires a message: SELECT dolt_commit('-m', 'msg')", -1);
    return;
  }

  /* Flush pending mutations and serialize catalog */
  rc = doltliteFlushAndSerializeCatalog(db, &catData, &nCatData);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error(context, "failed to flush pending changes", -1);
    return;
  }

  /* Store catalog chunk */
  rc = chunkStorePut(cs, catData, nCatData, &catalogHash);
  sqlite3_free(catData);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(context, rc);
    return;
  }

  /* Build commit object */
  memset(&commit, 0, sizeof(commit));
  chunkStoreGetHeadCommit(cs, &commit.parentHash);
  chunkStoreGetRoot(cs, &rootHash);
  memcpy(&commit.rootHash, &rootHash, sizeof(ProllyHash));
  memcpy(&commit.catalogHash, &catalogHash, sizeof(ProllyHash));
  commit.timestamp = (i64)time(0);

  /* Parse author "Name <email>" */
  if( zAuthor ){
    const char *lt = strchr(zAuthor, '<');
    const char *gt = lt ? strchr(lt, '>') : 0;
    if( lt && gt ){
      int nameLen = (int)(lt - zAuthor);
      while( nameLen>0 && zAuthor[nameLen-1]==' ' ) nameLen--;
      commit.zName = sqlite3_mprintf("%.*s", nameLen, zAuthor);
      commit.zEmail = sqlite3_mprintf("%.*s", (int)(gt-lt-1), lt+1);
    }else{
      commit.zName = sqlite3_mprintf("%s", zAuthor);
      commit.zEmail = sqlite3_mprintf("");
    }
  }else{
    commit.zName = sqlite3_mprintf("doltlite");
    commit.zEmail = sqlite3_mprintf("");
  }
  commit.zMessage = sqlite3_mprintf("%s", zMessage);

  /* Serialize commit and store as chunk */
  rc = doltliteCommitSerialize(&commit, &commitData, &nCommitData);
  if( rc!=SQLITE_OK ){
    doltliteCommitClear(&commit);
    sqlite3_result_error_code(context, rc);
    return;
  }

  rc = chunkStorePut(cs, commitData, nCommitData, &commitHash);
  sqlite3_free(commitData);
  doltliteCommitClear(&commit);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(context, rc);
    return;
  }

  /* Update HEAD and persist */
  chunkStoreSetHeadCommit(cs, &commitHash);
  chunkStoreSetCatalog(cs, &catalogHash);
  chunkStoreSetRoot(cs, &rootHash);
  rc = chunkStoreCommit(cs);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(context, rc);
    return;
  }

  /* Return commit hash */
  doltliteHashToHex(&commitHash, hexBuf);
  sqlite3_result_text(context, hexBuf, -1, SQLITE_TRANSIENT);
}

/* --------------------------------------------------------------------------
** Registration: called from prolly_btree.c at database open time
** -------------------------------------------------------------------------- */

void doltliteRegister(sqlite3 *db){
  sqlite3_create_function(db, "dolt_commit", -1, SQLITE_UTF8, 0,
                          doltliteCommitFunc, 0, 0);
  doltliteLogRegister(db);
}

#endif /* DOLTLITE_PROLLY */
