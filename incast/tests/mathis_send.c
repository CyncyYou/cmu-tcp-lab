/**
 * Mathis experiment SENDER (CP2 report tooling).
 *
 * Connects to the receiver, transfers a fixed file, and prints the transfer
 * duration and throughput. Timing spans from just before the first byte is
 * queued to just after cmu_close() returns (cmu_close blocks until all
 * outstanding data has been acknowledged).
 *
 * Env: server15441=<recv ip>  serverport15441=<port>  MATHIS_FILE=<path>
 * Output: MATHIS,bytes=<N>,seconds=<T>,throughput_bps=<B>
 */

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include "cmu_tcp.h"

int main() {
  char *serverip = getenv("server15441");
  if (!serverip) serverip = "10.0.1.1";
  char *serverport = getenv("serverport15441");
  if (!serverport) serverport = "15441";
  int portno = (uint16_t)atoi(serverport);

  char *path = getenv("MATHIS_FILE");
  if (!path) path = "/vagrant/project-2_15-441/tests/mathis.input";

  FILE *fp = fopen(path, "rb");
  if (!fp) {
    fprintf(stderr, "cannot open %s\n", path);
    return EXIT_FAILURE;
  }
  fseek(fp, 0, SEEK_END);
  long fsize = ftell(fp);
  fseek(fp, 0, SEEK_SET);
  uint8_t *data = malloc(fsize);
  if (!data || fread(data, 1, fsize, fp) != (size_t)fsize) {
    fprintf(stderr, "read error\n");
    return EXIT_FAILURE;
  }
  fclose(fp);

  cmu_socket_t socket;
  if (cmu_socket(&socket, TCP_INITIATOR, portno, serverip) < 0) {
    return EXIT_FAILURE;
  }

  struct timespec t0, t1;
  clock_gettime(CLOCK_MONOTONIC, &t0);

  long off = 0;
  while (off < fsize) {
    int chunk = (fsize - off) > 2000 ? 2000 : (int)(fsize - off);
    cmu_write(&socket, data + off, chunk);
    off += chunk;
  }

  if (cmu_close(&socket) < 0) {
    return EXIT_FAILURE;
  }

  clock_gettime(CLOCK_MONOTONIC, &t1);
  double secs = (t1.tv_sec - t0.tv_sec) + (t1.tv_nsec - t0.tv_nsec) / 1e9;
  double bps = (secs > 0) ? (fsize * 8.0 / secs) : 0.0;
  printf("MATHIS,bytes=%ld,seconds=%.6f,throughput_bps=%.2f\n", fsize, secs,
         bps);
  free(data);
  return EXIT_SUCCESS;
}
