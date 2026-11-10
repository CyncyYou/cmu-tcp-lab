/**
 * Copyright (C) 2026 Carnegie Mellon University
 *
 * This file is part of the TCP in the Wild course project developed for the
 * Computer Networks course (15-441/641) taught at Carnegie Mellon University.
 *
 * Implementation of lab_host.h.
 *
 * In the Incast tests every receiving connection runs in its own process, so
 * the host-wide table lives in shared memory: the lab tools create a file and
 * pass its path in LAB_HOST_SHM, and every process maps it. A small spinlock
 * in that memory keeps the table consistent across processes. Without
 * LAB_HOST_SHM (e.g. your own tests), the table is ordinary process memory.
 *
 * The link rate and base RTT come from the LAB_HOST_LINK_BPS and
 * LAB_HOST_BASE_RTT_US environment variables.
 */

#include "lab_host.h"

#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <time.h>
#include <unistd.h>

#define LAB_HOST_MAX_CONNS 1024
#define LAB_HOST_RATE_WINDOW_US 100000ull
#define LAB_HOST_RATE_SLOTS 4096

typedef struct {
  int pid;              // process that owns the connection
  uintptr_t sock;       // its socket (unique within that process)
  uint32_t expected;
  uint32_t received;
  int active;
} lab_conn_t;

typedef struct {
  volatile int lock;
  int nconns;
  unsigned version;
  lab_conn_t conns[LAB_HOST_MAX_CONNS];
  uint64_t arr_t[LAB_HOST_RATE_SLOTS];
  uint32_t arr_b[LAB_HOST_RATE_SLOTS];
  int arr_head, arr_count;
} lab_table_t;

static lab_table_t local_table;
static lab_table_t* table = NULL;

static lab_table_t* get_table(void) {
  if (table != NULL) return table;
  lab_table_t* t = &local_table;
  const char* path = getenv("LAB_HOST_SHM");
  if (path != NULL) {
    int fd = open(path, O_RDWR);
    if (fd >= 0) {
      if (ftruncate(fd, sizeof(lab_table_t)) == 0) {
        void* m = mmap(NULL, sizeof(lab_table_t), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        if (m != MAP_FAILED) t = (lab_table_t*)m;
      }
      close(fd);
    }
  }
  // Benign race: every thread computes the same mapping target.
  table = t;
  return table;
}

static void lock(lab_table_t* t) {
  while (__atomic_exchange_n(&t->lock, 1, __ATOMIC_ACQUIRE)) {
    while (__atomic_load_n(&t->lock, __ATOMIC_RELAXED)) usleep(1);
  }
}

static void unlock(lab_table_t* t) { __atomic_store_n(&t->lock, 0, __ATOMIC_RELEASE); }

static uint64_t mono_us(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (uint64_t)ts.tv_sec * 1000000ull + (uint64_t)ts.tv_nsec / 1000ull;
}

static lab_conn_t* find(lab_table_t* t, cmu_socket_t* s) {
  int pid = (int)getpid();
  for (int i = 0; i < t->nconns; i++)
    if (t->conns[i].pid == pid && t->conns[i].sock == (uintptr_t)s) return &t->conns[i];
  return NULL;
}

void lab_host_join(cmu_socket_t* s, uint32_t expected_bytes) {
  lab_table_t* t = get_table();
  lock(t);
  lab_conn_t* c = find(t, s);
  if (c == NULL && t->nconns < LAB_HOST_MAX_CONNS) c = &t->conns[t->nconns++];
  if (c != NULL) {
    c->pid = (int)getpid();
    c->sock = (uintptr_t)s;
    c->expected = expected_bytes;
    c->received = 0;
    c->active = 1;
    t->version++;
  }
  unlock(t);
}

void lab_host_on_data(cmu_socket_t* s, uint32_t bytes) {
  lab_table_t* t = get_table();
  lock(t);
  lab_conn_t* c = find(t, s);
  if (c != NULL) c->received += bytes;
  int slot = (t->arr_head + t->arr_count) % LAB_HOST_RATE_SLOTS;
  if (t->arr_count == LAB_HOST_RATE_SLOTS)
    t->arr_head = (t->arr_head + 1) % LAB_HOST_RATE_SLOTS;
  else
    t->arr_count++;
  t->arr_t[slot] = mono_us();
  t->arr_b[slot] = bytes;
  unlock(t);
}

void lab_host_leave(cmu_socket_t* s) {
  lab_table_t* t = get_table();
  lock(t);
  lab_conn_t* c = find(t, s);
  if (c != NULL && c->active) {
    c->active = 0;
    t->version++;
  }
  unlock(t);
}

int lab_host_active(void) {
  lab_table_t* t = get_table();
  lock(t);
  int n = 0;
  for (int i = 0; i < t->nconns; i++) n += t->conns[i].active;
  unlock(t);
  return n;
}

int lab_host_rank(cmu_socket_t* s) {
  lab_table_t* t = get_table();
  int pid = (int)getpid();
  lock(t);
  int rank = -1, pos = 0;
  for (int i = 0; i < t->nconns; i++) {
    if (!t->conns[i].active) continue;
    if (t->conns[i].pid == pid && t->conns[i].sock == (uintptr_t)s) {
      rank = pos;
      break;
    }
    pos++;
  }
  unlock(t);
  return rank;
}

uint32_t lab_host_remaining(cmu_socket_t* s) {
  lab_table_t* t = get_table();
  lock(t);
  lab_conn_t* c = find(t, s);
  uint32_t rem = 0;
  if (c != NULL && c->expected > c->received) rem = c->expected - c->received;
  unlock(t);
  return rem;
}

double lab_host_link_bps(void) {
  const char* v = getenv("LAB_HOST_LINK_BPS");
  return v ? atof(v) : 0.0;
}

uint32_t lab_host_base_rtt_us(void) {
  const char* v = getenv("LAB_HOST_BASE_RTT_US");
  return v ? (uint32_t)strtoul(v, NULL, 10) : 0;
}

double lab_host_arrival_bps(void) {
  lab_table_t* t = get_table();
  lock(t);
  uint64_t now = mono_us();
  uint64_t bytes = 0;
  for (int i = 0; i < t->arr_count; i++) {
    int slot = (t->arr_head + i) % LAB_HOST_RATE_SLOTS;
    if (now - t->arr_t[slot] <= LAB_HOST_RATE_WINDOW_US) bytes += t->arr_b[slot];
  }
  unlock(t);
  return (double)bytes * 8.0 * 1e6 / (double)LAB_HOST_RATE_WINDOW_US;
}

unsigned lab_host_version(void) {
  lab_table_t* t = get_table();
  lock(t);
  unsigned v = t->version;
  unlock(t);
  return v;
}
