/*
** Shell wrapper: runs the SQLite shell main() on a thread with a large stack.
**
** Prolly tree operations (especially serializeCatalog during COMMIT
** and deep VDBE call chains) need more stack than the default 512KB
** on macOS secondary threads or the default 8MB on Linux.
**
** This wrapper spawns a shell on a 64MB stack thread, matching what
** the C benchmark does.
*/
#include <pthread.h>
#include <stdlib.h>

/* Forward declare the real shell main (defined in shell.c) */
extern int sqlite3_shell_main(int argc, char **argv);

struct ShellArgs {
  int argc;
  char **argv;
  int rc;
};

static void *shell_thread(void *arg){
  struct ShellArgs *sa = (struct ShellArgs *)arg;
  sa->rc = sqlite3_shell_main(sa->argc, sa->argv);
  return NULL;
}

int main(int argc, char **argv){
  pthread_t th;
  pthread_attr_t attr;
  struct ShellArgs sa;

  sa.argc = argc;
  sa.argv = argv;
  sa.rc = 0;

  pthread_attr_init(&attr);
  pthread_attr_setstacksize(&attr, 256 * 1024 * 1024);  /* 256MB stack */
  pthread_create(&th, &attr, shell_thread, &sa);
  pthread_join(th, NULL);
  pthread_attr_destroy(&attr);

  return sa.rc;
}
