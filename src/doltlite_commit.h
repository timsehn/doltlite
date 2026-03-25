/*
** Doltlite commit objects: version control for SQLite.
**
** A commit is a content-addressed snapshot of the database state.
** Commits form a DAG via parent hashes (merge commits have 2+ parents).
**
** V1 format (legacy, single parent):
**   version(1=0x01) + parent_hash(20) + root_hash(20) + catalog_hash(20) +
**   timestamp(8) + name_len(2) + name(...) + email_len(2) + email(...) +
**   message_len(2) + message(...)
**
** V2 format (multi-parent, no redundant rootHash):
**   version(1=0x02) + nParents(1) + parent_hashes(20*nParents) +
**   catalog_hash(20) + timestamp(8) + name_len(2) + name(...) +
**   email_len(2) + email(...) + message_len(2) + message(...)
*/
#ifndef DOLTLITE_COMMIT_H
#define DOLTLITE_COMMIT_H

#include "sqliteInt.h"
#include "prolly_hash.h"

#define DOLTLITE_COMMIT_V1 1
#define DOLTLITE_COMMIT_V2 2
#define DOLTLITE_COMMIT_VERSION DOLTLITE_COMMIT_V2

/* Maximum parents (merge commits typically have 2) */
#define DOLTLITE_MAX_PARENTS 8

typedef struct DoltliteCommit DoltliteCommit;
struct DoltliteCommit {
  ProllyHash parentHash;     /* First parent (for backward compat / convenience) */
  ProllyHash rootHash;       /* V1 only: data tree root (unused in V2) */
  ProllyHash catalogHash;    /* Catalog (table registry) at commit time */
  i64 timestamp;             /* Unix seconds */
  char *zName;               /* Author name (owned, sqlite3_free) */
  char *zEmail;              /* Author email (owned) */
  char *zMessage;            /* Commit message (owned) */
  /* Multi-parent support (V2+) */
  ProllyHash aParents[DOLTLITE_MAX_PARENTS];  /* Parent hashes */
  int nParents;              /* Number of parents (0 for initial commit) */
};

/* Serialize a commit to a byte buffer. Caller must sqlite3_free(*ppOut). */
int doltliteCommitSerialize(const DoltliteCommit *c, u8 **ppOut, int *pnOut);

/* Deserialize a commit from a byte buffer. Strings are copied (owned). */
int doltliteCommitDeserialize(const u8 *data, int nData, DoltliteCommit *c);

/* Free owned strings inside a commit (does NOT free the struct itself). */
void doltliteCommitClear(DoltliteCommit *c);

/* Format a ProllyHash as a 40-character hex string. buf must be >= 41 bytes. */
void doltliteHashToHex(const ProllyHash *h, char *buf);

/* Parse a 40-character hex string into a ProllyHash. */
int doltliteHexToHash(const char *hex, ProllyHash *h);

#endif /* DOLTLITE_COMMIT_H */
