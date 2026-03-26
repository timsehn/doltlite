#ifndef DOLTLITE_REMOTESRV_H
#define DOLTLITE_REMOTESRV_H

typedef struct DoltliteServer DoltliteServer;

/*
** Start serving a doltlite database over HTTP.
** Blocks until stopped. Returns SQLITE_OK on clean shutdown.
*/
int doltliteServe(const char *zDbPath, int port);

/*
** Non-blocking: start server in background thread.
** Use doltliteServerStop() to shut down.
*/
DoltliteServer *doltliteServeAsync(const char *zDbPath, int port);
void doltliteServerStop(DoltliteServer *pServer);
int doltliteServerPort(DoltliteServer *pServer);

#endif
