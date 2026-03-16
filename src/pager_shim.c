/*
** Pager shim implementation.
**
** When DOLTITE_PROLLY is defined, the real pager.c is not linked.
** Instead, sqlite3BtreePager() returns a PagerShim* cast to Pager*.
** This file implements the subset of sqlite3Pager* functions that are
** actually called by the 18 files which use sqlite3BtreePager().
**
** The PagerShim struct is defined in pager_shim.h. External code
** receives a Pager* which is really a PagerShim*, so every function
** here casts its Pager* argument to PagerShim* before accessing fields.
*/
#ifdef DOLTITE_PROLLY

#include "pager_shim.h"
#include "sqliteInt.h"

#include <string.h>

/*
** Cast helper — every shim function uses this to get the real struct.
*/
#define SHIM(p) ((PagerShim*)(p))

/* -----------------------------------------------------------------------
** Lifecycle: create and destroy
** ----------------------------------------------------------------------- */

/*
** Allocate a new PagerShim.
**
**   pVfs       — VFS to remember (may be NULL)
**   zFilename  — database file path; NULL and "" are both valid
**   pFd        — open file descriptor (may be NULL for :memory:)
**
** Returns NULL on OOM.
*/
PagerShim *pagerShimCreate(
  sqlite3_vfs *pVfs,
  const char *zFilename,
  sqlite3_file *pFd
){
  PagerShim *pShim;

  pShim = (PagerShim*)sqlite3_malloc(sizeof(PagerShim));
  if( pShim==0 ) return 0;
  memset(pShim, 0, sizeof(PagerShim));

  /* Copy the filename.  Treat NULL as empty string. */
  if( zFilename && zFilename[0] ){
    int n = (int)strlen(zFilename);
    pShim->zFilename = (char*)sqlite3_malloc(n + 1);
    if( pShim->zFilename==0 ){
      sqlite3_free(pShim);
      return 0;
    }
    memcpy(pShim->zFilename, zFilename, n + 1);
  }else{
    pShim->zFilename = (char*)sqlite3_malloc(1);
    if( pShim->zFilename==0 ){
      sqlite3_free(pShim);
      return 0;
    }
    pShim->zFilename[0] = '\0';
  }

  /* Journal name is always an empty string for the shim. */
  pShim->zJournal = (char*)sqlite3_malloc(1);
  if( pShim->zJournal==0 ){
    sqlite3_free(pShim->zFilename);
    sqlite3_free(pShim);
    return 0;
  }
  pShim->zJournal[0] = '\0';

  pShim->pFd          = pFd;
  pShim->pVfs         = pVfs;
  pShim->journalMode  = PAGER_JOURNALMODE_DELETE;
  pShim->eLock        = 0;
  pShim->eState       = 0;
  pShim->noSync       = 0;
  pShim->iDataVersion = 1;   /* Start at 1 so the first check sees data */
  pShim->nRef         = 0;
  pShim->nMmapSize    = 0;

  return pShim;
}

/*
** Free all memory associated with a PagerShim.
*/
void pagerShimDestroy(PagerShim *pShim){
  if( pShim==0 ) return;
  sqlite3_free(pShim->zFilename);
  sqlite3_free(pShim->zJournal);
  sqlite3_free(pShim);
}

/* -----------------------------------------------------------------------
** File descriptor
** ----------------------------------------------------------------------- */

/*
** Return the file descriptor associated with the pager.
*/
sqlite3_file *sqlite3PagerFile(Pager *pPager){
  return SHIM(pPager)->pFd;
}

/* -----------------------------------------------------------------------
** Filename queries
** ----------------------------------------------------------------------- */

/*
** Return the full pathname of the database file.
**
** The second parameter (outputFormat) controls the format of the
** returned string in the real pager.  For the shim we always return
** the stored filename regardless.
*/
const char *sqlite3PagerFilename(const Pager *pPager, int outputFormat){
  (void)outputFormat;
  return SHIM(pPager)->zFilename;
}

/*
** Return the full pathname of the journal file.
** In the shim this is always an empty string — there is no journal.
*/
const char *sqlite3PagerJournalname(Pager *pPager){
  return SHIM(pPager)->zJournal;
}

/* -----------------------------------------------------------------------
** Journal mode
** ----------------------------------------------------------------------- */

/*
** Return the current journal mode.
*/
int sqlite3PagerGetJournalMode(Pager *pPager){
  return (int)SHIM(pPager)->journalMode;
}

/*
** Return 1 — the shim always allows the journal mode to be changed.
*/
int sqlite3PagerOkToChangeJournalMode(Pager *pPager){
  (void)pPager;
  return 1;
}

/*
** Set the journal mode.  Store the value and return it.
**
** PAGER_JOURNALMODE_QUERY (-1) means "just return the current mode
** without changing it."
*/
int sqlite3PagerSetJournalMode(Pager *pPager, int eMode){
  PagerShim *pShim = SHIM(pPager);
  if( eMode!=PAGER_JOURNALMODE_QUERY ){
    pShim->journalMode = (u8)eMode;
  }
  return (int)pShim->journalMode;
}

/* -----------------------------------------------------------------------
** Lock helpers
** ----------------------------------------------------------------------- */

/*
** Attempt to acquire an exclusive lock.  The shim always succeeds.
*/
int sqlite3PagerExclusiveLock(Pager *pPager){
  (void)pPager;
  return SQLITE_OK;
}

/*
** Return non-zero if the pager is read-only.  The shim is always
** read-write, so return 0.
*/
u8 sqlite3PagerIsreadonly(Pager *pPager){
  (void)pPager;
  return 0;
}

/* -----------------------------------------------------------------------
** Reference counting
** ----------------------------------------------------------------------- */

/*
** Return the number of outstanding page references.  The shim never
** hands out pages, so this is always 0.
*/
int sqlite3PagerRefcount(Pager *pPager){
  (void)pPager;
  return 0;
}

/* -----------------------------------------------------------------------
** Data version
** ----------------------------------------------------------------------- */

/*
** Return the data-version counter.  External code uses this to detect
** when the database content has been modified by another connection.
*/
u32 sqlite3PagerDataVersion(Pager *pPager){
  return SHIM(pPager)->iDataVersion;
}

/* -----------------------------------------------------------------------
** No-op / stub functions
** ----------------------------------------------------------------------- */

/*
** Shrink the page cache.  Nothing to do in the shim.
*/
void sqlite3PagerShrink(Pager *pPager){
  (void)pPager;
}

/*
** Flush all dirty pages.  The shim has no dirty pages, so just
** return SQLITE_OK.
*/
int sqlite3PagerFlush(Pager *pPager){
  (void)pPager;
  return SQLITE_OK;
}

/*
** Return pager cache statistics.  The shim has no page cache, so
** every stat value is 0.
**
** Parameters:
**   eStat  — which statistic to return
**   reset  — if true, reset the counter after reading
**   pStat  — output pointer
*/
void sqlite3PagerCacheStat(Pager *pPager, int eStat, int reset, u64 *pStat){
  (void)pPager;
  (void)eStat;
  (void)reset;
  if( pStat ) *pStat = 0;
}

/* -----------------------------------------------------------------------
** Memory-database detection
** ----------------------------------------------------------------------- */

/*
** Return non-zero if this pager represents an in-memory database.
**
** A database is considered in-memory if its filename is either the
** empty string or the special name ":memory:".
*/
int sqlite3PagerIsMemdb(Pager *pPager){
  const char *z = SHIM(pPager)->zFilename;
  if( z==0 ) return 1;
  if( z[0]=='\0' ) return 1;
  if( strcmp(z, ":memory:")==0 ) return 1;
  return 0;
}

/* -----------------------------------------------------------------------
** Locking mode
** ----------------------------------------------------------------------- */

/*
** Get or set the locking mode.
**
** If eMode is PAGER_LOCKINGMODE_QUERY (-1), return the current mode
** without changing it.  Otherwise store eMode and return it.
**
** Valid non-query values:
**   PAGER_LOCKINGMODE_NORMAL      0
**   PAGER_LOCKINGMODE_EXCLUSIVE   1
*/
int sqlite3PagerLockingMode(Pager *pPager, int eMode){
  PagerShim *pShim = SHIM(pPager);
  if( eMode!=PAGER_LOCKINGMODE_QUERY ){
    pShim->eLock = (u8)eMode;
  }
  return (int)pShim->eLock;
}

/* -----------------------------------------------------------------------
** VFS accessor (bonus — cheap to provide, sometimes needed)
** ----------------------------------------------------------------------- */

/*
** Return the VFS associated with this pager.
*/
sqlite3_vfs *sqlite3PagerVfs(Pager *pPager){
  return SHIM(pPager)->pVfs;
}

#endif /* DOLTITE_PROLLY */
