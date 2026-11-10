/**
 * Mathis experiment RECEIVER (CP2 report tooling).
 *
 * Listens, reads the entire transfer until the sender is silent for ~10 s,
 * optionally writes received bytes out, and exits.
 *
 * Env: serverport15441=<port>  MATHIS_OUT=<path> (optional)
 *      MATHIS_EXPECT=<bytes> (optional): if set, exit as soon as this many
 *      bytes have been received (avoids waiting out the idle timeout and the
 *      race where the final segment arrives just after we give up).
 * Output: MATHIS_RECV,bytes=<N>
 */

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "cmu_tcp.h"

#define CHUNK 65536

int main() {
  char *serverport = getenv("serverport15441");
  if (!serverport) serverport = "15441";
  int portno = (uint16_t)atoi(serverport);
  char *outpath = getenv("MATHIS_OUT");
  char *expectstr = getenv("MATHIS_EXPECT");
  long expect = expectstr ? atol(expectstr) : 0;

  cmu_socket_t socket;
  if (cmu_socket(&socket, TCP_LISTENER, portno, NULL) < 0) {
    return EXIT_FAILURE;
  }

  FILE *out = outpath ? fopen(outpath, "wb") : NULL;
  uint8_t *buf = malloc(CHUNK);
  long total = 0;

  // Drain aggressively so the advertised window never collapses. Exit when we
  // have the full expected transfer (if known), or after a long idle period.
  int idle = 0;
  while (idle < 300) {  // up to ~30 s of silence as a fallback
    int got_any = 0;
    int n;
    while ((n = cmu_read(&socket, buf, CHUNK, NO_WAIT)) > 0) {
      total += n;
      if (out) fwrite(buf, 1, n, out);
      got_any = 1;
    }
    if (expect > 0 && total >= expect) {
      break;  // got the whole file; no need to wait out the idle timer
    }
    if (got_any) {
      idle = 0;
    } else {
      idle++;
      usleep(20000);  // 20 ms
    }
  }
  if (out) fclose(out);
  printf("MATHIS_RECV,bytes=%ld\n", total);
  if (cmu_close(&socket) < 0) {
    return EXIT_FAILURE;
  }
  free(buf);
  return EXIT_SUCCESS;
}
