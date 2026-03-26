/*
** Minimal HTTP/1.1 server for doltlite remote access.
** Serves a .doltlite file over HTTP, enabling push/fetch/clone
** from remote clients.
**
** Uses POSIX sockets, no external dependencies.
*/
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "chunk_store.h"
#include "prolly_hash.h"
#include "doltlite_remotesrv.h"

#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <pthread.h>
#include <poll.h>
#include <string.h>
#include <stdio.h>

struct DoltliteServer {
  int listenFd;          /* Listening socket */
  int port;              /* Actual port (may differ from requested if 0) */
  volatile int running;  /* Set to 0 to stop */
  ChunkStore store;      /* The chunk store being served */
  pthread_t thread;      /* Background thread (for async mode) */
};

/* ----------------------------------------------------------------
** Hex utilities
** ---------------------------------------------------------------- */

static int hexVal(char c){
  if( c>='0' && c<='9' ) return c - '0';
  if( c>='a' && c<='f' ) return c - 'a' + 10;
  if( c>='A' && c<='F' ) return c - 'A' + 10;
  return -1;
}

static int hexToHash(const char *zHex, ProllyHash *pHash){
  int i;
  for(i=0; i<PROLLY_HASH_SIZE; i++){
    int hi = hexVal(zHex[i*2]);
    int lo = hexVal(zHex[i*2+1]);
    if( hi<0 || lo<0 ) return SQLITE_ERROR;
    pHash->data[i] = (u8)((hi<<4)|lo);
  }
  return SQLITE_OK;
}

/* ----------------------------------------------------------------
** HTTP response helpers
** ---------------------------------------------------------------- */

static void sendResponse(int fd, int status, const char *zStatus,
                         const u8 *pBody, int nBody){
  char zHeader[256];
  int nHeader;
  sqlite3_snprintf(sizeof(zHeader), zHeader,
    "HTTP/1.1 %d %s\r\n"
    "Content-Length: %d\r\n"
    "Connection: close\r\n"
    "\r\n",
    status, zStatus, nBody);
  nHeader = (int)strlen(zHeader);
  write(fd, zHeader, nHeader);
  if( pBody && nBody>0 ){
    write(fd, pBody, nBody);
  }
}

static void sendOk(int fd, const u8 *pBody, int nBody){
  sendResponse(fd, 200, "OK", pBody, nBody);
}

static void sendNotFound(int fd){
  sendResponse(fd, 404, "Not Found", (const u8*)"Not Found", 9);
}

static void sendBadRequest(int fd){
  sendResponse(fd, 400, "Bad Request", (const u8*)"Bad Request", 11);
}

static void sendError(int fd){
  sendResponse(fd, 500, "Internal Server Error",
               (const u8*)"Internal Server Error", 21);
}

/* ----------------------------------------------------------------
** Minimal HTTP request parser
** ---------------------------------------------------------------- */

/* Maximum header size we'll accept */
#define MAX_HEADER_SIZE 4096

/* Read exactly nBytes from fd into pBuf. Returns 0 on success. */
static int readExact(int fd, u8 *pBuf, int nBytes){
  int nRead = 0;
  while( nRead < nBytes ){
    int n = (int)read(fd, pBuf + nRead, nBytes - nRead);
    if( n <= 0 ) return -1;
    nRead += n;
  }
  return 0;
}

/*
** Parse an HTTP request from fd.
** Extracts method, path, content-length, and reads the body if present.
** Returns 0 on success, -1 on error.
*/
static int parseRequest(
  int fd,
  char *zMethod, int nMethodMax,    /* OUT: method (GET/POST/PUT) */
  char *zPath, int nPathMax,        /* OUT: request path */
  u8 **ppBody, int *pnBody          /* OUT: body (caller frees) */
){
  char aBuf[MAX_HEADER_SIZE];
  int nBuf = 0;
  int headerEnd = 0;
  int contentLength = 0;
  char *p;

  *ppBody = 0;
  *pnBody = 0;

  /* Read headers byte by byte until we find \r\n\r\n */
  while( nBuf < MAX_HEADER_SIZE-1 ){
    int n = (int)read(fd, &aBuf[nBuf], 1);
    if( n <= 0 ) return -1;
    nBuf++;
    if( nBuf>=4
     && aBuf[nBuf-4]=='\r' && aBuf[nBuf-3]=='\n'
     && aBuf[nBuf-2]=='\r' && aBuf[nBuf-1]=='\n' ){
      headerEnd = 1;
      break;
    }
  }
  if( !headerEnd ) return -1;
  aBuf[nBuf] = '\0';

  /* Parse request line: METHOD /path HTTP/1.x\r\n */
  p = aBuf;
  {
    char *pSpace = strchr(p, ' ');
    int len;
    if( !pSpace ) return -1;
    len = (int)(pSpace - p);
    if( len >= nMethodMax ) len = nMethodMax - 1;
    memcpy(zMethod, p, len);
    zMethod[len] = '\0';
    p = pSpace + 1;
  }
  {
    char *pSpace = strchr(p, ' ');
    int len;
    if( !pSpace ) return -1;
    len = (int)(pSpace - p);
    if( len >= nPathMax ) len = nPathMax - 1;
    memcpy(zPath, p, len);
    zPath[len] = '\0';
    p = pSpace + 1;
  }

  /* Scan headers for Content-Length */
  {
    const char *zCL = "Content-Length:";
    int nCL = (int)strlen(zCL);
    char *pLine = strstr(aBuf, zCL);
    if( !pLine ){
      /* Try lowercase */
      zCL = "content-length:";
      pLine = strstr(aBuf, zCL);
    }
    if( pLine ){
      pLine += nCL;
      while( *pLine==' ' || *pLine=='\t' ) pLine++;
      contentLength = atoi(pLine);
    }
  }

  /* Read body if present */
  if( contentLength > 0 ){
    u8 *pBody = (u8*)sqlite3_malloc(contentLength);
    if( !pBody ) return -1;
    if( readExact(fd, pBody, contentLength)!=0 ){
      sqlite3_free(pBody);
      return -1;
    }
    *ppBody = pBody;
    *pnBody = contentLength;
  }

  return 0;
}

/* ----------------------------------------------------------------
** Request handlers
** ---------------------------------------------------------------- */

/* GET /v1/root — Return 20-byte raw root hash */
static void handleGetRoot(DoltliteServer *pSrv, int fd){
  ProllyHash root;
  chunkStoreGetRoot(&pSrv->store, &root);
  sendOk(fd, root.data, PROLLY_HASH_SIZE);
}

/* POST /v1/has-chunks — Batch existence check */
static void handleHasChunks(DoltliteServer *pSrv, int fd,
                            const u8 *pBody, int nBody){
  int nHashes;
  u8 *aResult;
  int rc;

  if( nBody % PROLLY_HASH_SIZE != 0 ){
    sendBadRequest(fd);
    return;
  }
  nHashes = nBody / PROLLY_HASH_SIZE;
  if( nHashes == 0 ){
    sendOk(fd, 0, 0);
    return;
  }

  aResult = (u8*)sqlite3_malloc(nHashes);
  if( !aResult ){
    sendError(fd);
    return;
  }
  memset(aResult, 0, nHashes);

  rc = chunkStoreHasMany(&pSrv->store, (const ProllyHash*)pBody,
                         nHashes, aResult);
  if( rc!=SQLITE_OK ){
    sqlite3_free(aResult);
    sendError(fd);
    return;
  }
  sendOk(fd, aResult, nHashes);
  sqlite3_free(aResult);
}

/* GET /v1/chunk/{40-char-hex-hash} — Download one chunk */
static void handleGetChunk(DoltliteServer *pSrv, int fd, const char *zPath){
  const char *zHex;
  ProllyHash hash;
  u8 *pData = 0;
  int nData = 0;
  int rc;

  /* Path is /v1/chunk/XXXX...  (prefix is 10 chars: "/v1/chunk/") */
  if( (int)strlen(zPath) < 10 + PROLLY_HASH_SIZE*2 ){
    sendBadRequest(fd);
    return;
  }
  zHex = zPath + 10;

  if( hexToHash(zHex, &hash)!=SQLITE_OK ){
    sendBadRequest(fd);
    return;
  }

  rc = chunkStoreGet(&pSrv->store, &hash, &pData, &nData);
  if( rc==SQLITE_NOTFOUND ){
    sendNotFound(fd);
    return;
  }
  if( rc!=SQLITE_OK ){
    sendError(fd);
    return;
  }

  sendOk(fd, pData, nData);
  sqlite3_free(pData);
}

/* POST /v1/chunks — Upload batch of chunks */
static void handlePostChunks(DoltliteServer *pSrv, int fd,
                             const u8 *pBody, int nBody){
  int offset = 0;

  while( offset + PROLLY_HASH_SIZE + 4 <= nBody ){
    u32 len;
    ProllyHash hash;
    int rc;

    /* Skip the client-provided hash (20 bytes) — chunkStorePut computes it */
    offset += PROLLY_HASH_SIZE;

    /* Read 4-byte little-endian length */
    len = (u32)pBody[offset]
        | ((u32)pBody[offset+1] << 8)
        | ((u32)pBody[offset+2] << 16)
        | ((u32)pBody[offset+3] << 24);
    offset += 4;

    if( offset + (int)len > nBody ){
      sendBadRequest(fd);
      return;
    }

    rc = chunkStorePut(&pSrv->store, pBody + offset, (int)len, &hash);
    if( rc!=SQLITE_OK ){
      sendError(fd);
      return;
    }
    offset += (int)len;
  }

  sendOk(fd, 0, 0);
}

/* GET /v1/refs — Get serialized refs blob */
static void handleGetRefs(DoltliteServer *pSrv, int fd){
  u8 *pData = 0;
  int nData = 0;
  int rc;

  if( prollyHashIsEmpty(&pSrv->store.refsHash) ){
    sendNotFound(fd);
    return;
  }

  rc = chunkStoreGet(&pSrv->store, &pSrv->store.refsHash, &pData, &nData);
  if( rc==SQLITE_NOTFOUND ){
    sendNotFound(fd);
    return;
  }
  if( rc!=SQLITE_OK ){
    sendError(fd);
    return;
  }

  sendOk(fd, pData, nData);
  sqlite3_free(pData);
}

/* PUT /v1/refs — Update refs blob */
static void handlePutRefs(DoltliteServer *pSrv, int fd,
                          const u8 *pBody, int nBody){
  ProllyHash hash;
  int rc;

  if( nBody<=0 ){
    sendBadRequest(fd);
    return;
  }

  rc = chunkStorePut(&pSrv->store, pBody, nBody, &hash);
  if( rc!=SQLITE_OK ){
    sendError(fd);
    return;
  }

  pSrv->store.refsHash = hash;

  rc = chunkStoreReloadRefs(&pSrv->store);
  if( rc!=SQLITE_OK ){
    sendError(fd);
    return;
  }

  sendOk(fd, 0, 0);
}

/* POST /v1/commit — Commit pending writes */
static void handleCommit(DoltliteServer *pSrv, int fd){
  int rc;

  rc = chunkStoreSerializeRefs(&pSrv->store);
  if( rc!=SQLITE_OK ){
    sendError(fd);
    return;
  }

  rc = chunkStoreCommit(&pSrv->store);
  if( rc!=SQLITE_OK ){
    sendError(fd);
    return;
  }

  sendOk(fd, 0, 0);
}

/* ----------------------------------------------------------------
** Request routing
** ---------------------------------------------------------------- */

static void handleRequest(DoltliteServer *pSrv, int fd){
  char zMethod[16];
  char zPath[512];
  u8 *pBody = 0;
  int nBody = 0;

  if( parseRequest(fd, zMethod, sizeof(zMethod),
                   zPath, sizeof(zPath), &pBody, &nBody)!=0 ){
    sendBadRequest(fd);
    return;
  }

  if( strcmp(zMethod, "GET")==0 ){
    if( strcmp(zPath, "/v1/root")==0 ){
      handleGetRoot(pSrv, fd);
    }else if( strncmp(zPath, "/v1/chunk/", 10)==0 ){
      handleGetChunk(pSrv, fd, zPath);
    }else if( strcmp(zPath, "/v1/refs")==0 ){
      handleGetRefs(pSrv, fd);
    }else{
      sendNotFound(fd);
    }
  }else if( strcmp(zMethod, "POST")==0 ){
    if( strcmp(zPath, "/v1/has-chunks")==0 ){
      handleHasChunks(pSrv, fd, pBody, nBody);
    }else if( strcmp(zPath, "/v1/chunks")==0 ){
      handlePostChunks(pSrv, fd, pBody, nBody);
    }else if( strcmp(zPath, "/v1/commit")==0 ){
      handleCommit(pSrv, fd);
    }else{
      sendNotFound(fd);
    }
  }else if( strcmp(zMethod, "PUT")==0 ){
    if( strcmp(zPath, "/v1/refs")==0 ){
      handlePutRefs(pSrv, fd, pBody, nBody);
    }else{
      sendNotFound(fd);
    }
  }else{
    sendBadRequest(fd);
  }

  sqlite3_free(pBody);
}

/* ----------------------------------------------------------------
** Server lifecycle
** ---------------------------------------------------------------- */

static int serverInit(DoltliteServer *pSrv, const char *zDbPath, int port){
  struct sockaddr_in addr;
  socklen_t addrLen;
  int opt = 1;
  int rc;
  int flags;

  memset(pSrv, 0, sizeof(*pSrv));
  pSrv->listenFd = -1;

  /* Open the chunk store */
  flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_MAIN_DB;
  rc = chunkStoreOpen(&pSrv->store, sqlite3_vfs_find(0), zDbPath, flags);
  if( rc!=SQLITE_OK ) return rc;

  /* Create listening socket */
  pSrv->listenFd = socket(AF_INET, SOCK_STREAM, 0);
  if( pSrv->listenFd < 0 ){
    chunkStoreClose(&pSrv->store);
    return SQLITE_ERROR;
  }

  setsockopt(pSrv->listenFd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_ANY);
  addr.sin_port = htons((u16)port);

  if( bind(pSrv->listenFd, (struct sockaddr*)&addr, sizeof(addr)) < 0 ){
    close(pSrv->listenFd);
    chunkStoreClose(&pSrv->store);
    return SQLITE_ERROR;
  }

  if( listen(pSrv->listenFd, 5) < 0 ){
    close(pSrv->listenFd);
    chunkStoreClose(&pSrv->store);
    return SQLITE_ERROR;
  }

  /* Retrieve actual port (important when port==0) */
  addrLen = sizeof(addr);
  if( getsockname(pSrv->listenFd, (struct sockaddr*)&addr, &addrLen)==0 ){
    pSrv->port = ntohs(addr.sin_port);
  }else{
    pSrv->port = port;
  }

  pSrv->running = 1;
  return SQLITE_OK;
}

static void serverLoop(DoltliteServer *pSrv){
  while( pSrv->running ){
    struct pollfd pfd;
    int clientFd;

    pfd.fd = pSrv->listenFd;
    pfd.events = POLLIN;
    pfd.revents = 0;

    /* Poll with 1-second timeout so we can check the running flag */
    if( poll(&pfd, 1, 1000) <= 0 ) continue;

    clientFd = accept(pSrv->listenFd, NULL, NULL);
    if( clientFd < 0 ) continue;

    handleRequest(pSrv, clientFd);
    close(clientFd);
  }
}

static void serverCleanup(DoltliteServer *pSrv){
  if( pSrv->listenFd >= 0 ){
    close(pSrv->listenFd);
    pSrv->listenFd = -1;
  }
  chunkStoreClose(&pSrv->store);
}

/*
** Start serving a doltlite database over HTTP.
** Blocks until stopped. Returns SQLITE_OK on clean shutdown.
*/
int doltliteServe(const char *zDbPath, int port){
  DoltliteServer server;
  int rc;

  rc = serverInit(&server, zDbPath, port);
  if( rc!=SQLITE_OK ) return rc;

  serverLoop(&server);
  serverCleanup(&server);
  return SQLITE_OK;
}

/* Thread entry point for async mode */
static void *serverThreadEntry(void *pArg){
  DoltliteServer *pSrv = (DoltliteServer*)pArg;
  serverLoop(pSrv);
  serverCleanup(pSrv);
  return 0;
}

/*
** Non-blocking: start server in background thread.
*/
DoltliteServer *doltliteServeAsync(const char *zDbPath, int port){
  DoltliteServer *pSrv;
  int rc;

  pSrv = (DoltliteServer*)sqlite3_malloc(sizeof(DoltliteServer));
  if( !pSrv ) return 0;

  rc = serverInit(pSrv, zDbPath, port);
  if( rc!=SQLITE_OK ){
    sqlite3_free(pSrv);
    return 0;
  }

  if( pthread_create(&pSrv->thread, 0, serverThreadEntry, pSrv)!=0 ){
    serverCleanup(pSrv);
    sqlite3_free(pSrv);
    return 0;
  }

  return pSrv;
}

/*
** Stop a running async server and free resources.
*/
void doltliteServerStop(DoltliteServer *pServer){
  if( !pServer ) return;
  pServer->running = 0;
  /* Close the listen socket to unblock accept() */
  if( pServer->listenFd >= 0 ){
    close(pServer->listenFd);
    pServer->listenFd = -1;
  }
  pthread_join(pServer->thread, 0);
  sqlite3_free(pServer);
}

/*
** Return the actual port the server is listening on.
*/
int doltliteServerPort(DoltliteServer *pServer){
  return pServer ? pServer->port : 0;
}

#endif /* DOLTLITE_PROLLY */
