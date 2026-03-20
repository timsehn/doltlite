/*
** Merge-flush: apply buffered edits from MutMap into an existing prolly tree.
** This is the core mutation algorithm — it merges sorted edits with the
** existing tree's leaf entries, then rebuilds the tree using the chunker.
**
** The algorithm performs a single sorted merge-walk over two streams:
**   1. The existing tree's leaf entries (via cursor)
**   2. The pending edits (via MutMap iterator)
**
** The merged output is fed to the chunker, which produces a new set of
** content-addressed nodes forming the updated tree. Unchanged subtrees
** are structurally shared with the old tree.
*/
#ifdef DOLTLITE_PROLLY

#include "prolly_mutate.h"
#include <string.h>

/*
** Compare two keys according to the node flags.
**
** For INTKEY tables: compare the 64-bit integer keys.
** For BLOBKEY tables: memcmp on the shorter length, then compare lengths.
**
** Returns negative if key1 < key2, zero if equal, positive if key1 > key2.
*/
/*
** Compare two SQLite serialized record keys using sqlite3VdbeRecordCompare.
** This is necessary because raw memcmp does NOT produce the correct sort
** order for SQLite record format (serial type encodings in the header
** cause byte-order divergence from logical key order).
**
** We allocate a temporary KeyInfo with BINARY collation, unpack pKey2
** into an UnpackedRecord, and compare pKey1 against it.
*/
/*
** Compare two serialized SQLite record keys by parsing the record
** headers and comparing field by field. This avoids needing KeyInfo
** or the full VdbeRecordCompare machinery.
**
** Returns <0, 0, >0 like strcmp.
*/
int compareBlobKeys(const u8 *pKey1, int nKey1,
                    const u8 *pKey2, int nKey2){
  u32 hdr1, hdr2;
  u32 off1, off2;  /* Offset within header */
  u32 d1, d2;      /* Offset within data */

  if( nKey1==0 && nKey2==0 ) return 0;
  if( nKey1==0 ) return -1;
  if( nKey2==0 ) return 1;

  /* Parse header sizes */
  off1 = getVarint32(pKey1, hdr1);
  off2 = getVarint32(pKey2, hdr2);
  if( hdr1 > (u32)nKey1 || hdr2 > (u32)nKey2 ){
    /* Malformed header — fall back to raw comparison */
    if( nKey1<nKey2 ) return -1;
    if( nKey1>nKey2 ) return 1;
    return memcmp(pKey1, pKey2, nKey1);
  }
  d1 = hdr1;
  d2 = hdr2;

  /* Compare fields one by one */
  while( off1 < hdr1 && off2 < hdr2 ){
    u32 type1, type2;
    u32 len1, len2;

    off1 += getVarint32(pKey1+off1, type1);
    off2 += getVarint32(pKey2+off2, type2);

    /* Compute field lengths from serial types */
    if( type1<=6 ){
      /* Integer types: 0=NULL(0 bytes), 1=1byte, 2=2bytes, 3=3bytes, 4=4bytes, 5=6bytes, 6=8bytes */
      static const u8 aLen[] = {0, 1, 2, 3, 4, 6, 8};
      len1 = aLen[type1];
    }else if( type1==7 ){
      len1 = 8; /* float */
    }else if( type1>=12 ){
      len1 = (type1-12)/2;
    }else{
      len1 = 0; /* types 8,9,10,11 = constants */
    }

    if( type2<=6 ){
      static const u8 aLen[] = {0, 1, 2, 3, 4, 6, 8};
      len2 = aLen[type2];
    }else if( type2==7 ){
      len2 = 8;
    }else if( type2>=12 ){
      len2 = (type2-12)/2;
    }else{
      len2 = 0;
    }

    /* Bounds check: ensure field data doesn't exceed key buffer */
    if( d1+len1 > (u32)nKey1 || d2+len2 > (u32)nKey2 ){
      /* Malformed key — fall back to raw length comparison */
      if( nKey1<nKey2 ) return -1;
      if( nKey1>nKey2 ) return 1;
      return 0;
    }

    /* NULL handling: NULL sorts before everything */
    if( type1==0 && type2==0 ){ d1+=len1; d2+=len2; continue; }
    if( type1==0 ) return -1;
    if( type2==0 ) return 1;

    /* Integer types (1-6, 8, 9) */
    if( (type1>=1 && type1<=6) || type1==8 || type1==9 ){
      i64 v1, v2;
      if( type1==8 ) v1 = 0;
      else if( type1==9 ) v1 = 1;
      else{
        v1 = (pKey1[d1] & 0x80) ? -1 : 0;
        for(u32 i=0; i<len1; i++) v1 = (v1<<8) | pKey1[d1+i];
      }

      /* type2 might be different type; compare by type class */
      if( (type2>=1 && type2<=6) || type2==8 || type2==9 ){
        if( type2==8 ) v2 = 0;
        else if( type2==9 ) v2 = 1;
        else{
          v2 = (pKey2[d2] & 0x80) ? -1 : 0;
          for(u32 i=0; i<len2; i++) v2 = (v2<<8) | pKey2[d2+i];
        }
        if( v1<v2 ){ return -1; }
        if( v1>v2 ){ return 1; }
      }else if( type2==7 ){
        /* Integer vs float: promote to float */
        return -1; /* integers sort before floats with same value; simplified */
      }else{
        /* Integer vs text/blob: integer sorts first */
        return -1;
      }
      d1 += len1; d2 += len2;
      continue;
    }

    /* Float (type 7) */
    if( type1==7 ){
      if( type2==7 ){
        /* Compare 8-byte big-endian floats */
        int c = memcmp(pKey1+d1, pKey2+d2, 8);
        if( c!=0 ) return c;
      }else if( (type2>=1 && type2<=6) || type2==8 || type2==9 ){
        return 1; /* float after integer */
      }else{
        return -1; /* float before text/blob */
      }
      d1 += len1; d2 += len2;
      continue;
    }

    /* Text types (odd serial type >= 13) */
    if( type1>=13 && (type1&1) ){
      if( type2>=13 && (type2&1) ){
        /* Both text: compare as strings (BINARY collation) */
        u32 n = len1 < len2 ? len1 : len2;
        int c = memcmp(pKey1+d1, pKey2+d2, n);
        if( c!=0 ) return c;
        if( len1<len2 ) return -1;
        if( len1>len2 ) return 1;
      }else if( type2>=12 && !(type2&1) ){
        /* Text vs blob: text sorts before blob */
        return -1;
      }else{
        /* Text vs numeric: text sorts after numeric */
        return 1;
      }
      d1 += len1; d2 += len2;
      continue;
    }

    /* Blob types (even serial type >= 12) */
    if( type1>=12 && !(type1&1) ){
      if( type2>=12 && !(type2&1) ){
        u32 n = len1 < len2 ? len1 : len2;
        int c = memcmp(pKey1+d1, pKey2+d2, n);
        if( c!=0 ) return c;
        if( len1<len2 ) return -1;
        if( len1>len2 ) return 1;
      }else{
        return 1; /* blob sorts last */
      }
      d1 += len1; d2 += len2;
      continue;
    }

    /* Fallback: compare raw bytes */
    {
      u32 n = len1 < len2 ? len1 : len2;
      int c = memcmp(pKey1+d1, pKey2+d2, n);
      if( c!=0 ) return c;
      if( len1<len2 ) return -1;
      if( len1>len2 ) return 1;
    }
    d1 += len1; d2 += len2;
  }

  /* All compared fields are equal. Compare by number of remaining fields. */
  if( off1 < hdr1 ) return 1;  /* key1 has more fields */
  if( off2 < hdr2 ) return -1; /* key2 has more fields */
  return 0;
}

static int compareKeys(
  u8 flags,
  const u8 *pKey1, int nKey1, i64 iKey1,
  const u8 *pKey2, int nKey2, i64 iKey2
){
  if( flags & PROLLY_NODE_INTKEY ){
    if( iKey1 < iKey2 ) return -1;
    if( iKey1 > iKey2 ) return +1;
    return 0;
  }else{
    return compareBlobKeys(pKey1, nKey1, pKey2, nKey2);
  }
}

/*
** Feed a single key-value pair into the chunker.
**
** For INTKEY tables the key is serialized as an 8-byte little-endian i64
** so that the chunker (which works on raw bytes) receives a sortable key
** representation.  For BLOBKEY tables the key bytes are passed through
** directly.
*/
static int feedChunker(
  ProllyChunker *pCh,
  u8 flags,
  const u8 *pKey, int nKey, i64 intKey,
  const u8 *pVal, int nVal
){
  if( flags & PROLLY_NODE_INTKEY ){
    /* Encode integer key as 8-byte little-endian for the chunker. */
    u8 aKeyBuf[8];
    aKeyBuf[0] = (u8)(intKey);
    aKeyBuf[1] = (u8)(intKey >> 8);
    aKeyBuf[2] = (u8)(intKey >> 16);
    aKeyBuf[3] = (u8)(intKey >> 24);
    aKeyBuf[4] = (u8)(intKey >> 32);
    aKeyBuf[5] = (u8)(intKey >> 40);
    aKeyBuf[6] = (u8)(intKey >> 48);
    aKeyBuf[7] = (u8)(intKey >> 56);
    return prollyChunkerAdd(pCh, aKeyBuf, 8, pVal, nVal);
  }else{
    return prollyChunkerAdd(pCh, pKey, nKey, pVal, nVal);
  }
}

/*
** Build a new tree from scratch using only the edits in the MutMap.
** This is used when the old tree is empty (oldRoot is all-zeros).
**
** We simply iterate over all edits in sorted order. INSERT edits are
** fed to the chunker; DELETE edits are silently ignored (there is
** nothing to delete in an empty tree).
*/
static int buildFromEdits(
  ProllyMutator *pMut
){
  ProllyChunker chunker;
  ProllyMutMapIter iter;
  int rc;

  rc = prollyChunkerInit(&chunker, pMut->pStore, pMut->flags);
  if( rc!=SQLITE_OK ) return rc;

  prollyMutMapIterFirst(&iter, pMut->pEdits);
  while( prollyMutMapIterValid(&iter) ){
    ProllyMutMapEntry *pEntry = prollyMutMapIterEntry(&iter);
    if( pEntry->op==PROLLY_EDIT_INSERT ){
      rc = feedChunker(&chunker, pMut->flags,
                       pEntry->pKey, pEntry->nKey, pEntry->intKey,
                       pEntry->pVal, pEntry->nVal);
      if( rc!=SQLITE_OK ){
        prollyChunkerFree(&chunker);
        return rc;
      }
    }
    /* DELETE edits on an empty tree: nothing to do, skip */
    prollyMutMapIterNext(&iter);
  }

  rc = prollyChunkerFinish(&chunker);
  if( rc==SQLITE_OK ){
    prollyChunkerGetRoot(&chunker, &pMut->newRoot);
  }

  prollyChunkerFree(&chunker);
  return rc;
}

/*
** Helper: get the last key from a leaf node in chunker-compatible format.
** For INTKEY, encodes as 8-byte LE into aKeyBuf (must be >= 8 bytes).
** Sets *ppKey/*pnKey to the key bytes.
*/
static void leafLastKey(
  const ProllyNode *pLeaf, u8 flags,
  u8 *aKeyBuf, const u8 **ppKey, int *pnKey
){
  if( flags & PROLLY_NODE_INTKEY ){
    i64 ik = prollyNodeIntKey(pLeaf, pLeaf->nItems - 1);
    aKeyBuf[0] = (u8)(ik);      aKeyBuf[1] = (u8)(ik >> 8);
    aKeyBuf[2] = (u8)(ik >> 16); aKeyBuf[3] = (u8)(ik >> 24);
    aKeyBuf[4] = (u8)(ik >> 32); aKeyBuf[5] = (u8)(ik >> 40);
    aKeyBuf[6] = (u8)(ik >> 48); aKeyBuf[7] = (u8)(ik >> 56);
    *ppKey = aKeyBuf;
    *pnKey = 8;
  }else{
    prollyNodeKey(pLeaf, pLeaf->nItems - 1, ppKey, pnKey);
  }
}

/*
** Merge one leaf's entries with edits and feed to chunker level 0.
** Consumes edits from *pIter that fall within this leaf's key range.
*/
static int mergeLeafWithEdits(
  ProllyMutator *pMut,
  const ProllyNode *pLeaf,
  ProllyMutMapIter *pIter,
  ProllyChunker *pCh
){
  int rc = SQLITE_OK;
  int j = 0;

  while( j < pLeaf->nItems || prollyMutMapIterValid(pIter) ){
    int haveOld = (j < pLeaf->nItems);
    int haveEdit = prollyMutMapIterValid(pIter);
    ProllyMutMapEntry *pEd = haveEdit ? prollyMutMapIterEntry(pIter) : 0;

    if( haveOld && haveEdit ){
      /* Check if edit is still within this leaf's range */
      const u8 *pLK; int nLK; i64 iLK = 0;
      if( pMut->flags & PROLLY_NODE_INTKEY ){
        iLK = prollyNodeIntKey(pLeaf, pLeaf->nItems - 1);
        pLK = 0; nLK = 0;
      }else{
        prollyNodeKey(pLeaf, pLeaf->nItems - 1, &pLK, &nLK);
      }
      if( compareKeys(pMut->flags, pLK, nLK, iLK,
                      pEd->pKey, pEd->nKey, pEd->intKey) < 0 ){
        /* Edit is past this leaf — copy remaining old entries */
        const u8 *pOK; int nOK; i64 iOK = 0;
        const u8 *pV; int nV;
        if( pMut->flags & PROLLY_NODE_INTKEY ){
          iOK = prollyNodeIntKey(pLeaf, j); pOK = 0; nOK = 0;
        }else{
          prollyNodeKey(pLeaf, j, &pOK, &nOK);
        }
        prollyNodeValue(pLeaf, j, &pV, &nV);
        rc = feedChunker(pCh, pMut->flags, pOK, nOK, iOK, pV, nV);
        if( rc!=SQLITE_OK ) return rc;
        j++;
        continue;
      }

      const u8 *pOK; int nOK; i64 iOK = 0;
      if( pMut->flags & PROLLY_NODE_INTKEY ){
        iOK = prollyNodeIntKey(pLeaf, j); pOK = 0; nOK = 0;
      }else{
        prollyNodeKey(pLeaf, j, &pOK, &nOK);
      }
      int cmp = compareKeys(pMut->flags, pOK, nOK, iOK,
                            pEd->pKey, pEd->nKey, pEd->intKey);
      if( cmp < 0 ){
        const u8 *pV; int nV;
        prollyNodeValue(pLeaf, j, &pV, &nV);
        rc = feedChunker(pCh, pMut->flags, pOK, nOK, iOK, pV, nV);
        if( rc!=SQLITE_OK ) return rc;
        j++;
      }else if( cmp == 0 ){
        if( pEd->op==PROLLY_EDIT_INSERT ){
          rc = feedChunker(pCh, pMut->flags, pEd->pKey, pEd->nKey,
                           pEd->intKey, pEd->pVal, pEd->nVal);
          if( rc!=SQLITE_OK ) return rc;
        }
        j++;
        prollyMutMapIterNext(pIter);
      }else{
        if( pEd->op==PROLLY_EDIT_INSERT ){
          rc = feedChunker(pCh, pMut->flags, pEd->pKey, pEd->nKey,
                           pEd->intKey, pEd->pVal, pEd->nVal);
          if( rc!=SQLITE_OK ) return rc;
        }
        prollyMutMapIterNext(pIter);
      }
    }else if( haveOld ){
      const u8 *pOK; int nOK; i64 iOK = 0;
      const u8 *pV; int nV;
      if( pMut->flags & PROLLY_NODE_INTKEY ){
        iOK = prollyNodeIntKey(pLeaf, j); pOK = 0; nOK = 0;
      }else{
        prollyNodeKey(pLeaf, j, &pOK, &nOK);
      }
      prollyNodeValue(pLeaf, j, &pV, &nV);
      rc = feedChunker(pCh, pMut->flags, pOK, nOK, iOK, pV, nV);
      if( rc!=SQLITE_OK ) return rc;
      j++;
    }else{
      /* Edits past this leaf — break, caller handles them */
      break;
    }
  }
  return SQLITE_OK;
}

/*
** Apply edits using targeted leaf modification with subtree skipping.
**
** Algorithm:
**   1. Walk edits in sorted order.
**   2. For each edit, seek cursor to the target leaf (O(log N)).
**   3. Emit all unchanged leaves between the previous position and
**      the current leaf directly at chunker level 1 (O(1) per leaf).
**   4. Merge the target leaf's entries with edits through level 0.
**   5. After all edits, emit remaining unchanged leaves at level 1.
**   6. Finalize the chunker to get the new root.
**
** Total cost: O(M × log N) where M = edits, N = total entries.
*/
static int applyEdits(
  ProllyMutator *pMut
){
  ProllyCursor cur;
  ProllyMutMapIter iter;
  ProllyChunker chunker;
  int rc;
  int curEmpty = 0;

  prollyCursorInit(&cur, pMut->pStore, pMut->pCache,
                   &pMut->oldRoot, pMut->flags);
  rc = prollyCursorFirst(&cur, &curEmpty);
  if( rc!=SQLITE_OK ){
    prollyCursorClose(&cur);
    return rc;
  }
  if( curEmpty ){
    prollyCursorClose(&cur);
    return buildFromEdits(pMut);
  }

  prollyMutMapIterFirst(&iter, pMut->pEdits);

  int treeHeight = cur.nLevel;
  int leafLevel = treeHeight - 1;

  rc = prollyChunkerInit(&chunker, pMut->pStore, pMut->flags);
  if( rc!=SQLITE_OK ){
    prollyCursorClose(&cur);
    return rc;
  }

  /* For single-level trees (one leaf), just merge and rebuild.
  ** No subtree skipping possible. */
  if( treeHeight <= 1 ){
    ProllyNode *pLeaf = &cur.aLevel[0].pEntry->node;
    rc = mergeLeafWithEdits(pMut, pLeaf, &iter, &chunker);
    if( rc!=SQLITE_OK ) goto apply_cleanup;

    /* Trailing inserts */
    while( prollyMutMapIterValid(&iter) ){
      ProllyMutMapEntry *pEd = prollyMutMapIterEntry(&iter);
      if( pEd->op==PROLLY_EDIT_INSERT ){
        rc = feedChunker(&chunker, pMut->flags, pEd->pKey, pEd->nKey,
                         pEd->intKey, pEd->pVal, pEd->nVal);
        if( rc!=SQLITE_OK ) goto apply_cleanup;
      }
      prollyMutMapIterNext(&iter);
    }
    goto apply_finish;
  }

  /*
  ** Multi-level tree.  Walk the lowest internal level (parent of
  ** leaves).  For each internal node, use its keys to determine
  ** which children contain edits WITHOUT loading unchanged children.
  **
  ** Internal node keys are the LAST key of each child subtree.
  ** So child[i] covers keys from (key[i-1], key[i]].  We can
  ** binary search to find which child an edit targets.
  **
  ** For ranges of children between edits, we inject their
  ** (key, hash) directly at level 1 without loading them.
  */
  {
    int parentLevel = leafLevel - 1;

    while( prollyCursorIsValid(&cur) ){
      ProllyCacheEntry *pParentEntry = cur.aLevel[parentLevel].pEntry;
      ProllyNode *pParent = &pParentEntry->node;
      int startChild = cur.aLevel[parentLevel].idx;
      int nChildren = pParent->nItems;

      /* Find the range of children that contain edits.
      ** Process children from startChild to nChildren-1.
      ** For each child, check if the next edit falls within it
      ** by comparing edit key against the parent's key for that child.
      **
      ** Parent key[i] = last key of child[i].
      ** Edit is in child[i] if editKey <= parentKey[i] and
      ** (i==0 or editKey > parentKey[i-1]).
      */
      int ci = startChild;
      while( ci < nChildren ){
        /* If no more edits, all remaining children are unchanged */
        if( !prollyMutMapIterValid(&iter) ){
          /* Inject remaining children directly at level 1 */
          for(; ci < nChildren; ci++){
            const u8 *pPK; int nPK;
            ProllyHash childHash;
            prollyNodeKey(pParent, ci, &pPK, &nPK);
            prollyNodeChildHash(pParent, ci, &childHash);

            rc = prollyChunkerFlushLevel(&chunker, 0);
            if( rc!=SQLITE_OK ) goto apply_cleanup;
            rc = prollyChunkerAddAtLevel(&chunker, 1,
                   pPK, nPK, childHash.data, PROLLY_HASH_SIZE);
            if( rc!=SQLITE_OK ) goto apply_cleanup;
          }
          break;
        }

        /* Check: does the next edit fall in child[ci]?
        ** Parent key[ci] >= editKey means yes (or ci is the last child). */
        ProllyMutMapEntry *pEd = prollyMutMapIterEntry(&iter);
        const u8 *pPK; int nPK;
        if( pMut->flags & PROLLY_NODE_INTKEY ){
          i64 parentIntKey = prollyNodeIntKey(pParent, ci);
          int cmp;
          if( parentIntKey < pEd->intKey ) cmp = -1;
          else if( parentIntKey > pEd->intKey ) cmp = 1;
          else cmp = 0;
          if( cmp < 0 && ci < nChildren - 1 ){
            /* Edit is past this child — inject and skip */
            ProllyHash childHash;
            u8 aKeyBuf[8];
            aKeyBuf[0] = (u8)(parentIntKey);
            aKeyBuf[1] = (u8)(parentIntKey >> 8);
            aKeyBuf[2] = (u8)(parentIntKey >> 16);
            aKeyBuf[3] = (u8)(parentIntKey >> 24);
            aKeyBuf[4] = (u8)(parentIntKey >> 32);
            aKeyBuf[5] = (u8)(parentIntKey >> 40);
            aKeyBuf[6] = (u8)(parentIntKey >> 48);
            aKeyBuf[7] = (u8)(parentIntKey >> 56);
            prollyNodeChildHash(pParent, ci, &childHash);

            rc = prollyChunkerFlushLevel(&chunker, 0);
            if( rc!=SQLITE_OK ) goto apply_cleanup;
            rc = prollyChunkerAddAtLevel(&chunker, 1,
                   aKeyBuf, 8, childHash.data, PROLLY_HASH_SIZE);
            if( rc!=SQLITE_OK ) goto apply_cleanup;
            ci++;
            continue;
          }
        }else{
          prollyNodeKey(pParent, ci, &pPK, &nPK);
          int cmp = compareKeys(pMut->flags, pPK, nPK, 0,
                                pEd->pKey, pEd->nKey, pEd->intKey);
          if( cmp < 0 && ci < nChildren - 1 ){
            /* Edit is past this child — inject and skip */
            ProllyHash childHash;
            prollyNodeChildHash(pParent, ci, &childHash);

            rc = prollyChunkerFlushLevel(&chunker, 0);
            if( rc!=SQLITE_OK ) goto apply_cleanup;
            rc = prollyChunkerAddAtLevel(&chunker, 1,
                   pPK, nPK, childHash.data, PROLLY_HASH_SIZE);
            if( rc!=SQLITE_OK ) goto apply_cleanup;
            ci++;
            continue;
          }
        }

        /* Edit targets this child (or this is the last child).
        ** Load the leaf and merge. */
        {
          ProllyHash childHash;
          prollyNodeChildHash(pParent, ci, &childHash);
          ProllyCacheEntry *pChildEntry = 0;
          rc = prollyCursorLoadNode(&cur, &childHash, &pChildEntry);
          if( rc!=SQLITE_OK ) goto apply_cleanup;

          rc = mergeLeafWithEdits(pMut, &pChildEntry->node, &iter, &chunker);
          prollyCacheRelease(cur.pCache, pChildEntry);
          if( rc!=SQLITE_OK ) goto apply_cleanup;
          ci++;
        }
      }

      /* Advance cursor past this parent to the next one */
      {
        ProllyHash lastChildHash;
        prollyNodeChildHash(pParent, nChildren - 1, &lastChildHash);
        ProllyCacheEntry *pLastChild = 0;
        rc = prollyCursorLoadNode(&cur, &lastChildHash, &pLastChild);
        if( rc!=SQLITE_OK ) goto apply_cleanup;

        if( cur.aLevel[leafLevel].pEntry ){
          prollyCacheRelease(cur.pCache, cur.aLevel[leafLevel].pEntry);
        }
        cur.aLevel[leafLevel].pEntry = pLastChild;
        cur.aLevel[leafLevel].idx = pLastChild->node.nItems - 1;
        cur.aLevel[parentLevel].idx = nChildren - 1;

        rc = prollyCursorNext(&cur);
        if( rc!=SQLITE_OK ) goto apply_cleanup;
      }
    }
  }

  /* Handle trailing inserts past end of tree */
  while( prollyMutMapIterValid(&iter) ){
    ProllyMutMapEntry *pEd = prollyMutMapIterEntry(&iter);
    if( pEd->op==PROLLY_EDIT_INSERT ){
      rc = feedChunker(&chunker, pMut->flags, pEd->pKey, pEd->nKey,
                       pEd->intKey, pEd->pVal, pEd->nVal);
      if( rc!=SQLITE_OK ) goto apply_cleanup;
    }
    prollyMutMapIterNext(&iter);
  }

apply_finish:
  rc = prollyChunkerFinish(&chunker);
  if( rc==SQLITE_OK ){
    prollyChunkerGetRoot(&chunker, &pMut->newRoot);
  }

apply_cleanup:
  prollyChunkerFree(&chunker);
  prollyCursorClose(&cur);
  return rc;
}

/*
** Apply all edits in pMut->pEdits to the tree rooted at pMut->oldRoot.
** Produces a new tree whose root hash is stored in pMut->newRoot.
**
** Three cases:
**   1. No edits: newRoot = oldRoot (no work).
**   2. Old tree is empty: build from edits alone.
**   3. Otherwise: merge-walk old tree + edits.
**
** Returns SQLITE_OK on success with result in pMut->newRoot.
*/
int prollyMutateFlush(ProllyMutator *pMut){
  /* Case 1: no edits — the tree is unchanged */
  if( prollyMutMapIsEmpty(pMut->pEdits) ){
    memcpy(&pMut->newRoot, &pMut->oldRoot, sizeof(ProllyHash));
    return SQLITE_OK;
  }

  /* Case 2: old tree is empty — build entirely from edits */
  if( prollyHashIsEmpty(&pMut->oldRoot) ){
    return buildFromEdits(pMut);
  }

  /* Case 3: targeted leaf edits */
  return applyEdits(pMut);
}

/*
** Convenience function: apply a single INSERT to a tree.
**
** Creates a temporary 1-entry MutMap, wraps it in a ProllyMutator,
** calls prollyMutateFlush, and returns the new root hash.
*/
int prollyMutateInsert(
  ChunkStore *pStore,
  ProllyCache *pCache,
  const ProllyHash *pRoot,
  u8 flags,
  const u8 *pKey, int nKey, i64 intKey,
  const u8 *pVal, int nVal,
  ProllyHash *pNewRoot
){
  ProllyMutMap mm;
  ProllyMutator mut;
  int rc;
  u8 isIntKey = (flags & PROLLY_NODE_INTKEY) ? 1 : 0;

  /* Initialize a 1-entry edit map with the INSERT */
  rc = prollyMutMapInit(&mm, isIntKey);
  if( rc!=SQLITE_OK ) return rc;

  rc = prollyMutMapInsert(&mm, pKey, nKey, intKey, pVal, nVal);
  if( rc!=SQLITE_OK ){
    prollyMutMapFree(&mm);
    return rc;
  }

  /* Set up the mutator */
  memset(&mut, 0, sizeof(mut));
  mut.pStore = pStore;
  mut.pCache = pCache;
  memcpy(&mut.oldRoot, pRoot, sizeof(ProllyHash));
  mut.pEdits = &mm;
  mut.flags = flags;

  /* Flush */
  rc = prollyMutateFlush(&mut);
  if( rc==SQLITE_OK ){
    memcpy(pNewRoot, &mut.newRoot, sizeof(ProllyHash));
  }

  prollyMutMapFree(&mm);
  return rc;
}

/*
** Convenience function: apply a single DELETE to a tree.
**
** Creates a temporary 1-entry MutMap with a DELETE edit, wraps it in a
** ProllyMutator, calls prollyMutateFlush, and returns the new root hash.
*/
int prollyMutateDelete(
  ChunkStore *pStore,
  ProllyCache *pCache,
  const ProllyHash *pRoot,
  u8 flags,
  const u8 *pKey, int nKey, i64 intKey,
  ProllyHash *pNewRoot
){
  ProllyMutMap mm;
  ProllyMutator mut;
  int rc;
  u8 isIntKey = (flags & PROLLY_NODE_INTKEY) ? 1 : 0;

  /* Initialize a 1-entry edit map with the DELETE */
  rc = prollyMutMapInit(&mm, isIntKey);
  if( rc!=SQLITE_OK ) return rc;

  rc = prollyMutMapDelete(&mm, pKey, nKey, intKey);
  if( rc!=SQLITE_OK ){
    prollyMutMapFree(&mm);
    return rc;
  }

  /* Set up the mutator */
  memset(&mut, 0, sizeof(mut));
  mut.pStore = pStore;
  mut.pCache = pCache;
  memcpy(&mut.oldRoot, pRoot, sizeof(ProllyHash));
  mut.pEdits = &mm;
  mut.flags = flags;

  /* Flush */
  rc = prollyMutateFlush(&mut);
  if( rc==SQLITE_OK ){
    memcpy(pNewRoot, &mut.newRoot, sizeof(ProllyHash));
  }

  prollyMutMapFree(&mm);
  return rc;
}

#endif /* DOLTLITE_PROLLY */
