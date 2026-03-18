/*
** prolly_diff.c — Two-tree diff for prolly trees.
**
** Computes the set of changes (ADD, DELETE, MODIFY) between two prolly
** trees by walking two cursors in parallel, comparing keys in sorted
** order.  When the root hashes are identical the diff is trivially empty.
*/
#ifdef DOLTLITE_PROLLY

#include "prolly_diff.h"

#include <string.h>  /* memcmp */

/*
** Compare two blob keys lexicographically.
** Returns <0, 0, or >0.
*/
static int diffBlobKeyCmp(
  const u8 *pA, int nA,
  const u8 *pB, int nB
){
  int n = (nA < nB) ? nA : nB;
  int c = memcmp(pA, pB, n);
  if( c ) return c;
  return nA - nB;
}

/*
** Compare the current keys of two cursors.
** Sets *pCmp to <0, 0, or >0 following the same convention as memcmp.
**
** For INTKEY trees the integer keys are compared directly.
** For BLOBKEY trees the raw key blobs are compared lexicographically.
*/
static void diffCompareKeys(
  ProllyCursor *pOld,
  ProllyCursor *pNew,
  u8 flags,
  int *pCmp
){
  if( flags & PROLLY_NODE_INTKEY ){
    i64 iOld = prollyCursorIntKey(pOld);
    i64 iNew = prollyCursorIntKey(pNew);
    if( iOld < iNew )      *pCmp = -1;
    else if( iOld > iNew )  *pCmp =  1;
    else                     *pCmp =  0;
  }else{
    const u8 *pKeyOld; int nKeyOld;
    const u8 *pKeyNew; int nKeyNew;
    prollyCursorKey(pOld, &pKeyOld, &nKeyOld);
    prollyCursorKey(pNew, &pKeyNew, &nKeyNew);
    *pCmp = diffBlobKeyCmp(pKeyOld, nKeyOld, pKeyNew, nKeyNew);
  }
}

/*
** Fill a ProllyDiffChange from a single cursor entry.
**   type     – PROLLY_DIFF_ADD, _DELETE, or _MODIFY
**   pCur     – cursor positioned at the entry
**   flags    – INTKEY / BLOBKEY
**   pChange  – [out] change record to populate
**
** Key fields are always set from pCur.  Caller is responsible for
** setting the value pointers that correspond to the "other" side
** (pOldVal for ADD, pNewVal for DELETE, or both for MODIFY).
*/
static void diffFillKey(
  ProllyDiffChange *pChange,
  ProllyCursor *pCur,
  u8 flags
){
  if( flags & PROLLY_NODE_INTKEY ){
    pChange->intKey = prollyCursorIntKey(pCur);
    pChange->pKey   = 0;
    pChange->nKey   = 0;
  }else{
    const u8 *pKey; int nKey;
    prollyCursorKey(pCur, &pKey, &nKey);
    pChange->pKey   = pKey;
    pChange->nKey   = nKey;
    pChange->intKey = 0;
  }
}

/*
** Emit a DELETE change for the current entry of curOld.
*/
static int diffEmitDelete(
  ProllyCursor *pOld,
  u8 flags,
  ProllyDiffCallback xCallback,
  void *pCtx
){
  ProllyDiffChange change;
  const u8 *pVal; int nVal;

  memset(&change, 0, sizeof(change));
  change.type = PROLLY_DIFF_DELETE;
  diffFillKey(&change, pOld, flags);
  prollyCursorValue(pOld, &pVal, &nVal);
  change.pOldVal = pVal;
  change.nOldVal = nVal;
  change.pNewVal = 0;
  change.nNewVal = 0;
  return xCallback(pCtx, &change);
}

/*
** Emit an ADD change for the current entry of curNew.
*/
static int diffEmitAdd(
  ProllyCursor *pNew,
  u8 flags,
  ProllyDiffCallback xCallback,
  void *pCtx
){
  ProllyDiffChange change;
  const u8 *pVal; int nVal;

  memset(&change, 0, sizeof(change));
  change.type = PROLLY_DIFF_ADD;
  diffFillKey(&change, pNew, flags);
  prollyCursorValue(pNew, &pVal, &nVal);
  change.pOldVal = 0;
  change.nOldVal = 0;
  change.pNewVal = pVal;
  change.nNewVal = nVal;
  return xCallback(pCtx, &change);
}

/*
** Emit a MODIFY change when the key matches but the value differs.
*/
static int diffEmitModify(
  ProllyCursor *pOld,
  ProllyCursor *pNew,
  u8 flags,
  ProllyDiffCallback xCallback,
  void *pCtx
){
  ProllyDiffChange change;
  const u8 *pOldVal; int nOldVal;
  const u8 *pNewVal; int nNewVal;

  memset(&change, 0, sizeof(change));
  change.type = PROLLY_DIFF_MODIFY;
  diffFillKey(&change, pNew, flags);
  prollyCursorValue(pOld, &pOldVal, &nOldVal);
  prollyCursorValue(pNew, &pNewVal, &nNewVal);
  change.pOldVal = pOldVal;
  change.nOldVal = nOldVal;
  change.pNewVal = pNewVal;
  change.nNewVal = nNewVal;
  return xCallback(pCtx, &change);
}

/*
** Read a big-endian SQLite varint from p. Returns bytes consumed.
*/
static int diffReadVarint(const u8 *p, const u8 *pEnd, u64 *pVal){
  u64 v = 0;
  int i;
  for(i=0; i<9 && p+i<pEnd; i++){
    if( i<8 ){
      v = (v << 7) | (p[i] & 0x7f);
      if( (p[i] & 0x80)==0 ){ *pVal = v; return i+1; }
    }else{
      v = (v << 8) | p[i];
      *pVal = v;
      return 9;
    }
  }
  *pVal = v;
  return i ? i : 1;
}

/*
** Return the data size in bytes for a given SQLite record serial type.
*/
static int diffSerialTypeLen(u64 st){
  if( st<=0 ) return 0;          /* NULL */
  if( st==1 ) return 1;
  if( st==2 ) return 2;
  if( st==3 ) return 3;
  if( st==4 ) return 4;
  if( st==5 ) return 6;
  if( st==6 ) return 8;
  if( st==7 ) return 8;          /* float */
  if( st==8 || st==9 ) return 0; /* integer 0 or 1 */
  if( st>=12 && (st&1)==0 ) return ((int)st-12)/2;  /* blob */
  if( st>=13 && (st&1)==1 ) return ((int)st-13)/2;  /* text */
  return 0;
}

/*
** Compare two SQLite record-format values field-by-field.
** Records that differ only in trailing NULL fields (e.g. after
** ALTER TABLE ADD COLUMN) are treated as equal.
**
** Returns non-zero if the records are logically identical.
*/
static int diffRecordsEqual(
  const u8 *pA, int nA,
  const u8 *pB, int nB
){
  const u8 *pEndA = pA + nA;
  const u8 *pEndB = pB + nB;
  u64 hdrSizeA, hdrSizeB;
  int hdrBytesA, hdrBytesB;
  const u8 *pHdrA, *pHdrB;       /* current position in header */
  const u8 *pHdrEndA, *pHdrEndB; /* end of header region */
  int offA, offB;                 /* current body offset */

  if( !pA || nA<1 || !pB || nB<1 ) return (!pA && !pB);

  /* Parse header sizes */
  hdrBytesA = diffReadVarint(pA, pEndA, &hdrSizeA);
  hdrBytesB = diffReadVarint(pB, pEndB, &hdrSizeB);
  pHdrA = pA + hdrBytesA;
  pHdrB = pB + hdrBytesB;
  pHdrEndA = pA + (int)hdrSizeA;
  pHdrEndB = pB + (int)hdrSizeB;
  offA = (int)hdrSizeA;
  offB = (int)hdrSizeB;

  /* Walk fields in parallel */
  while( pHdrA < pHdrEndA || pHdrB < pHdrEndB ){
    u64 stA = 0, stB = 0;   /* serial type: 0 = NULL */
    int lenA = 0, lenB = 0;

    if( pHdrA < pHdrEndA && pHdrA < pEndA ){
      int n = diffReadVarint(pHdrA, pHdrEndA, &stA);
      pHdrA += n;
      lenA = diffSerialTypeLen(stA);
    }
    if( pHdrB < pHdrEndB && pHdrB < pEndB ){
      int n = diffReadVarint(pHdrB, pHdrEndB, &stB);
      pHdrB += n;
      lenB = diffSerialTypeLen(stB);
    }

    /* Different serial types means different values, UNLESS both
    ** represent NULL (type 0 or missing field). */
    if( stA != stB ){
      /* If one side is NULL (type 0 or missing) and the other is also
      ** NULL, they match.  Otherwise they differ. */
      int nullA = (stA==0);
      int nullB = (stB==0);
      if( !nullA || !nullB ){
        return 0;  /* different */
      }
      /* Both NULL, continue */
    }else if( stA!=0 && lenA>0 ){
      /* Same serial type with data: compare the body bytes */
      if( offA+lenA > nA || offB+lenB > nB ) return 0;
      if( memcmp(pA+offA, pB+offB, lenA)!=0 ) return 0;
    }
    /* serial types 8 and 9 (integer 0 and 1) have no body bytes
    ** but are equal if stA==stB, which is already handled above. */

    offA += lenA;
    offB += lenB;
  }

  return 1;  /* all fields matched */
}

/*
** Return non-zero if the two values at the current cursor positions
** are identical.  First tries a fast byte-for-byte comparison; if sizes
** differ, falls back to field-by-field record comparison so that records
** differing only in trailing NULL columns (from ALTER TABLE ADD COLUMN)
** are treated as equal.
*/
static int diffValuesEqual(ProllyCursor *pOld, ProllyCursor *pNew){
  const u8 *pOldVal; int nOldVal;
  const u8 *pNewVal; int nNewVal;
  prollyCursorValue(pOld, &pOldVal, &nOldVal);
  prollyCursorValue(pNew, &pNewVal, &nNewVal);
  if( nOldVal==nNewVal ){
    if( nOldVal==0 ) return 1;
    if( memcmp(pOldVal, pNewVal, nOldVal)==0 ) return 1;
  }
  /* Byte comparison failed — try field-level comparison to handle
  ** schema changes (e.g. ALTER TABLE ADD COLUMN appends trailing NULLs) */
  return diffRecordsEqual(pOldVal, nOldVal, pNewVal, nNewVal);
}

/*
** Compute the diff between two prolly trees identified by their root
** hashes.  For every difference discovered, xCallback is invoked with
** a ProllyDiffChange describing the change.  If the callback returns
** anything other than SQLITE_OK the walk is abandoned and that error
** code is returned to the caller.
**
** The flags parameter carries PROLLY_NODE_INTKEY or PROLLY_NODE_BLOBKEY
** and is forwarded to the cursors so they can decode keys properly.
*/
int prollyDiff(
  ChunkStore *pStore,
  ProllyCache *pCache,
  const ProllyHash *pOldRoot,
  const ProllyHash *pNewRoot,
  u8 flags,
  ProllyDiffCallback xCallback,
  void *pCtx
){
  ProllyCursor *pCurOld = 0;
  ProllyCursor *pCurNew = 0;
  int rc = SQLITE_OK;
  int emptyOld = 0;
  int emptyNew = 0;

  /* Fast path: identical roots means no changes */
  if( prollyHashCompare(pOldRoot, pNewRoot) == 0 ){
    return SQLITE_OK;
  }

  /* Allocate cursors */
  pCurOld = (ProllyCursor*)sqlite3_malloc(sizeof(ProllyCursor));
  if( !pCurOld ){
    return SQLITE_NOMEM;
  }
  pCurNew = (ProllyCursor*)sqlite3_malloc(sizeof(ProllyCursor));
  if( !pCurNew ){
    sqlite3_free(pCurOld);
    return SQLITE_NOMEM;
  }

  /* Initialize cursors */
  prollyCursorInit(pCurOld, pStore, pCache, pOldRoot, flags);
  prollyCursorInit(pCurNew, pStore, pCache, pNewRoot, flags);

  /* Position both cursors at the first entry */
  rc = prollyCursorFirst(pCurOld, &emptyOld);
  if( rc != SQLITE_OK ) goto diff_cleanup;

  rc = prollyCursorFirst(pCurNew, &emptyNew);
  if( rc != SQLITE_OK ) goto diff_cleanup;

  /* Main merge walk */
  while( prollyCursorIsValid(pCurOld) && prollyCursorIsValid(pCurNew) ){
    int cmp;
    diffCompareKeys(pCurOld, pCurNew, flags, &cmp);

    if( cmp < 0 ){
      /* Old key < new key → entry was deleted */
      rc = diffEmitDelete(pCurOld, flags, xCallback, pCtx);
      if( rc != SQLITE_OK ) goto diff_cleanup;
      rc = prollyCursorNext(pCurOld);
      if( rc != SQLITE_OK ) goto diff_cleanup;
    }else if( cmp > 0 ){
      /* Old key > new key → entry was added */
      rc = diffEmitAdd(pCurNew, flags, xCallback, pCtx);
      if( rc != SQLITE_OK ) goto diff_cleanup;
      rc = prollyCursorNext(pCurNew);
      if( rc != SQLITE_OK ) goto diff_cleanup;
    }else{
      /* Keys equal — check values */
      if( !diffValuesEqual(pCurOld, pCurNew) ){
        rc = diffEmitModify(pCurOld, pCurNew, flags, xCallback, pCtx);
        if( rc != SQLITE_OK ) goto diff_cleanup;
      }
      rc = prollyCursorNext(pCurOld);
      if( rc != SQLITE_OK ) goto diff_cleanup;
      rc = prollyCursorNext(pCurNew);
      if( rc != SQLITE_OK ) goto diff_cleanup;
    }
  }

  /* Drain remaining entries in old cursor → DELETEs */
  while( prollyCursorIsValid(pCurOld) ){
    rc = diffEmitDelete(pCurOld, flags, xCallback, pCtx);
    if( rc != SQLITE_OK ) goto diff_cleanup;
    rc = prollyCursorNext(pCurOld);
    if( rc != SQLITE_OK ) goto diff_cleanup;
  }

  /* Drain remaining entries in new cursor → ADDs */
  while( prollyCursorIsValid(pCurNew) ){
    rc = diffEmitAdd(pCurNew, flags, xCallback, pCtx);
    if( rc != SQLITE_OK ) goto diff_cleanup;
    rc = prollyCursorNext(pCurNew);
    if( rc != SQLITE_OK ) goto diff_cleanup;
  }

diff_cleanup:
  prollyCursorClose(pCurOld);
  prollyCursorClose(pCurNew);
  sqlite3_free(pCurOld);
  sqlite3_free(pCurNew);
  return rc;
}

#endif /* DOLTLITE_PROLLY */
