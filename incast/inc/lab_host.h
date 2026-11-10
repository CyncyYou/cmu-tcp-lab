/**
 * Copyright (C) 2026 Carnegie Mellon University
 *
 * This file is part of the TCP in the Wild course project developed for the
 * Computer Networks course (15-441/641) taught at Carnegie Mellon University.
 *
 * lab_host: host-level information for the Incast challenge.
 *
 * `./labtest.py init` puts this file in incast/inc/ and lab_host.c in
 * incast/src/, so you can `#include "lab_host.h"` and call it directly. (If you
 * build with your own Makefile, add $(BUILD_DIR)/lab_host.o to OBJS.) The lab
 * tests always link the official lab_host.c.
 *
 * In the Incast tests the aggregator runs one receiving process per worker
 * (so each of your sockets has a process to itself). Our application
 * registers every receiving connection here and reports the bytes it reads,
 * and the information is shared by all of those processes: lab_host_active()
 * counts the connections in every process. Call these functions from any
 * thread; you never need a lock.
 */

#ifndef PROJECT_2_15_441_INC_LAB_HOST_H_
#define PROJECT_2_15_441_INC_LAB_HOST_H_

#include <stdint.h>

#include "cmu_tcp.h"

/* ---- Called by our incast application. You never call these. ---- */

/** Registers a receiving connection that expects `expected_bytes` in total. */
void lab_host_join(cmu_socket_t* s, uint32_t expected_bytes);

/** Reports that the application read `bytes` more bytes from `s`. */
void lab_host_on_data(cmu_socket_t* s, uint32_t bytes);

/** Unregisters `s` (it has delivered everything it was asked for). */
void lab_host_leave(cmu_socket_t* s);

/* ---- For your design. ---- */

/** Number of connections on this host (in all processes) still receiving. */
int lab_host_active(void);

/**
 * Position of `s` among the active connections, in the order they joined
 * (0 = joined first). Returns -1 if `s` has not joined or has already left.
 */
int lab_host_rank(cmu_socket_t* s);

/** Bytes `s` still has to deliver (0 if `s` is unknown). */
uint32_t lab_host_remaining(cmu_socket_t* s);

/** Rate of this host's link, in bits per second (0 if unknown). */
double lab_host_link_bps(void);

/** Round-trip time with empty queues, in microseconds (0 if unknown). */
uint32_t lab_host_base_rtt_us(void);

/** Total arrival rate over all connections during the last ~100 ms, bits/s. */
double lab_host_arrival_bps(void);

/** A counter that changes whenever a connection joins or leaves. */
unsigned lab_host_version(void);

#endif  // PROJECT_2_15_441_INC_LAB_HOST_H_
