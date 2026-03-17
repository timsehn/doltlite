/*
** Doltlite branch management: dolt_branch, dolt_checkout, active_branch,
** and dolt_branches virtual table.
**
** Branches are per-file (not per-connection). All connections to the same
** file share the same active branch. This matches SQLite's single-writer
** model. See README for details.
*/
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "chunk_store.h"
#include "doltlite_commit.h"

#include <string.h>

extern ChunkStore *doltliteGetChunkStore(sqlite3 *db);
extern int doltliteGetHeadCatalogHash(sqlite3 *db, ProllyHash *pCatHash);
extern int doltliteFlushAndSerializeCatalog(sqlite3 *db, u8 **ppOut, int *pnOut);
extern int doltliteHardReset(sqlite3 *db, const ProllyHash *catHash);

/* --------------------------------------------------------------------------
** active_branch() — returns current branch name
** -------------------------------------------------------------------------- */

static void activeBranchFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  ChunkStore *cs = doltliteGetChunkStore(db);
  (void)argc; (void)argv;
  if( cs ){
    sqlite3_result_text(ctx, chunkStoreGetCurrentBranch(cs), -1, SQLITE_TRANSIENT);
  }else{
    sqlite3_result_null(ctx);
  }
}

/* --------------------------------------------------------------------------
** dolt_branch('name') — create branch at HEAD
** dolt_branch('-d', 'name') — delete branch
** -------------------------------------------------------------------------- */

static void doltBranchFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  ChunkStore *cs = doltliteGetChunkStore(db);
  int rc;

  if( !cs ){ sqlite3_result_error(ctx, "no database", -1); return; }
  if( argc<1 ){ sqlite3_result_error(ctx, "usage: dolt_branch('name') or dolt_branch('-d','name')", -1); return; }

  const char *arg0 = (const char*)sqlite3_value_text(argv[0]);
  if( !arg0 ){ sqlite3_result_error(ctx, "branch name required", -1); return; }

  if( strcmp(arg0, "-d")==0 || strcmp(arg0, "--delete")==0 ){
    /* Delete branch */
    const char *zName;
    if( argc<2 ){ sqlite3_result_error(ctx, "branch name required for delete", -1); return; }
    zName = (const char*)sqlite3_value_text(argv[1]);
    if( !zName ){ sqlite3_result_error(ctx, "branch name required", -1); return; }
    if( strcmp(zName, chunkStoreGetCurrentBranch(cs))==0 ){
      sqlite3_result_error(ctx, "cannot delete the current branch", -1);
      return;
    }
    rc = chunkStoreDeleteBranch(cs, zName);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error(ctx, "branch not found", -1);
      return;
    }
  }else{
    /* Create branch at HEAD */
    ProllyHash head;
    chunkStoreGetHeadCommit(cs, &head);
    if( prollyHashIsEmpty(&head) ){
      sqlite3_result_error(ctx, "no commits yet — commit first before creating branches", -1);
      return;
    }
    rc = chunkStoreAddBranch(cs, arg0, &head);
    if( rc!=SQLITE_OK ){
      sqlite3_result_error(ctx, "branch already exists", -1);
      return;
    }
  }

  /* Persist refs */
  rc = chunkStoreSerializeRefs(cs);
  if( rc==SQLITE_OK ) rc = chunkStoreCommit(cs);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(ctx, rc);
    return;
  }
  sqlite3_result_int(ctx, 0);
}

/* --------------------------------------------------------------------------
** dolt_checkout('branch_name') — switch active branch
** -------------------------------------------------------------------------- */

static void doltCheckoutFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  ChunkStore *cs = doltliteGetChunkStore(db);
  ProllyHash targetCommit;
  ProllyHash headCatHash, workingCatHash;
  DoltliteCommit commit;
  u8 *data = 0;
  int nData = 0;
  int rc;

  if( !cs ){ sqlite3_result_error(ctx, "no database", -1); return; }
  if( argc<1 ){ sqlite3_result_error(ctx, "branch name required", -1); return; }

  const char *zBranch = (const char*)sqlite3_value_text(argv[0]);
  if( !zBranch ){ sqlite3_result_error(ctx, "branch name required", -1); return; }

  /* Already on this branch? */
  if( strcmp(zBranch, chunkStoreGetCurrentBranch(cs))==0 ){
    sqlite3_result_int(ctx, 0);
    return;
  }

  /* Find branch */
  rc = chunkStoreFindBranch(cs, zBranch, &targetCommit);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error(ctx, "branch not found", -1);
    return;
  }

  /* Check for uncommitted changes */
  {
    u8 *catData = 0; int nCatData = 0;
    rc = doltliteFlushAndSerializeCatalog(db, &catData, &nCatData);
    if( rc==SQLITE_OK ){
      prollyHashCompute(catData, nCatData, &workingCatHash);
      sqlite3_free(catData);
    }
    rc = doltliteGetHeadCatalogHash(db, &headCatHash);
    if( rc==SQLITE_OK && !prollyHashIsEmpty(&headCatHash)
     && prollyHashCompare(&workingCatHash, &headCatHash)!=0 ){
      sqlite3_result_error(ctx,
        "uncommitted changes — commit or dolt_reset('--hard') before checkout", -1);
      return;
    }
  }

  /* Load target commit's catalog */
  if( prollyHashIsEmpty(&targetCommit) ){
    sqlite3_result_error(ctx, "target branch has no commits", -1);
    return;
  }

  rc = chunkStoreGet(cs, &targetCommit, &data, &nData);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error(ctx, "failed to load target commit", -1);
    return;
  }
  rc = doltliteCommitDeserialize(data, nData, &commit);
  sqlite3_free(data);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error(ctx, "failed to parse target commit", -1);
    return;
  }

  /* Hard reset to target catalog */
  rc = doltliteHardReset(db, &commit.catalogHash);
  doltliteCommitClear(&commit);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error(ctx, "checkout failed", -1);
    return;
  }

  /* Update current branch + HEAD */
  chunkStoreSetCurrentBranch(cs, zBranch);
  memcpy(&cs->headCommit, &targetCommit, sizeof(ProllyHash));
  chunkStoreSetStagedCatalog(cs, &commit.catalogHash);

  /* Persist */
  rc = chunkStoreSerializeRefs(cs);
  if( rc==SQLITE_OK ) rc = chunkStoreCommit(cs);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(ctx, rc);
    return;
  }

  sqlite3_result_int(ctx, 0);
}

/* --------------------------------------------------------------------------
** dolt_branches virtual table
** -------------------------------------------------------------------------- */

typedef struct BranchesVtab BranchesVtab;
struct BranchesVtab { sqlite3_vtab base; sqlite3 *db; };

typedef struct BranchesCursor BranchesCursor;
struct BranchesCursor {
  sqlite3_vtab_cursor base;
  int iRow;
  int nRows;
};

static const char *branchesSchema =
  "CREATE TABLE x(name TEXT, hash TEXT, is_current INTEGER)";

static int brConnect(sqlite3 *db, void *pAux, int argc,
    const char *const*argv, sqlite3_vtab **ppVtab, char **pzErr){
  BranchesVtab *p;
  int rc;
  (void)pAux; (void)argc; (void)argv; (void)pzErr;
  rc = sqlite3_declare_vtab(db, branchesSchema);
  if( rc!=SQLITE_OK ) return rc;
  p = sqlite3_malloc(sizeof(*p));
  if( !p ) return SQLITE_NOMEM;
  memset(p, 0, sizeof(*p));
  p->db = db;
  *ppVtab = &p->base;
  return SQLITE_OK;
}

static int brDisconnect(sqlite3_vtab *pVtab){
  sqlite3_free(pVtab); return SQLITE_OK;
}

static int brOpen(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **ppCur){
  BranchesCursor *p = sqlite3_malloc(sizeof(*p));
  (void)pVtab;
  if( !p ) return SQLITE_NOMEM;
  memset(p, 0, sizeof(*p));
  *ppCur = &p->base;
  return SQLITE_OK;
}

static int brClose(sqlite3_vtab_cursor *pCur){
  sqlite3_free(pCur); return SQLITE_OK;
}

static int brFilter(sqlite3_vtab_cursor *pCur, int idxNum, const char *idxStr,
    int argc, sqlite3_value **argv){
  BranchesCursor *p = (BranchesCursor*)pCur;
  BranchesVtab *pVtab = (BranchesVtab*)pCur->pVtab;
  ChunkStore *cs = doltliteGetChunkStore(pVtab->db);
  (void)idxNum; (void)idxStr; (void)argc; (void)argv;
  p->iRow = 0;
  p->nRows = cs ? cs->nBranches : 0;
  return SQLITE_OK;
}

static int brNext(sqlite3_vtab_cursor *pCur){
  ((BranchesCursor*)pCur)->iRow++; return SQLITE_OK;
}

static int brEof(sqlite3_vtab_cursor *pCur){
  BranchesCursor *p = (BranchesCursor*)pCur;
  return p->iRow >= p->nRows;
}

static int brColumn(sqlite3_vtab_cursor *pCur, sqlite3_context *ctx, int iCol){
  BranchesCursor *p = (BranchesCursor*)pCur;
  BranchesVtab *pVtab = (BranchesVtab*)pCur->pVtab;
  ChunkStore *cs = doltliteGetChunkStore(pVtab->db);
  struct BranchRef *br;
  if( !cs || p->iRow>=cs->nBranches ) return SQLITE_OK;
  br = &cs->aBranches[p->iRow];
  switch( iCol ){
    case 0: sqlite3_result_text(ctx, br->zName, -1, SQLITE_TRANSIENT); break;
    case 1: {
      char hex[PROLLY_HASH_SIZE*2+1];
      doltliteHashToHex(&br->commitHash, hex);
      sqlite3_result_text(ctx, hex, -1, SQLITE_TRANSIENT);
      break;
    }
    case 2:
      sqlite3_result_int(ctx,
        cs->zCurrentBranch && strcmp(br->zName, cs->zCurrentBranch)==0 ? 1 : 0);
      break;
  }
  return SQLITE_OK;
}

static int brRowid(sqlite3_vtab_cursor *pCur, sqlite3_int64 *pRowid){
  *pRowid = ((BranchesCursor*)pCur)->iRow; return SQLITE_OK;
}

static int brBestIndex(sqlite3_vtab *pVtab, sqlite3_index_info *pInfo){
  (void)pVtab; pInfo->estimatedCost = 10.0; pInfo->estimatedRows = 5;
  return SQLITE_OK;
}

static sqlite3_module branchesModule = {
  0, 0, brConnect, brBestIndex, brDisconnect, 0,
  brOpen, brClose, brFilter, brNext, brEof,
  brColumn, brRowid,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
};

/* --------------------------------------------------------------------------
** Registration
** -------------------------------------------------------------------------- */

int doltliteBranchRegister(sqlite3 *db){
  int rc = SQLITE_OK;
  rc = sqlite3_create_function(db, "dolt_branch", -1, SQLITE_UTF8, 0,
                                doltBranchFunc, 0, 0);
  if( rc==SQLITE_OK )
    rc = sqlite3_create_function(db, "dolt_checkout", -1, SQLITE_UTF8, 0,
                                  doltCheckoutFunc, 0, 0);
  if( rc==SQLITE_OK )
    rc = sqlite3_create_function(db, "active_branch", 0, SQLITE_UTF8, 0,
                                  activeBranchFunc, 0, 0);
  if( rc==SQLITE_OK )
    rc = sqlite3_create_module(db, "dolt_branches", &branchesModule, 0);
  return rc;
}

#endif /* DOLTLITE_PROLLY */
