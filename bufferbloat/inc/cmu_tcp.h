/**
 * Copyright (C) 2022 Carnegie Mellon University
 *
 * This file is part of the TCP in the Wild course project developed for the
 * Computer Networks course (15-441/641) taught at Carnegie Mellon University.
 *
 * No part of the project may be copied and/or distributed without the express
 * permission of the 15-441/641 course staff.
 *
 *
 * This file defines the API for the CMU TCP implementation.
 */

#ifndef PROJECT_2_15_441_INC_CMU_TCP_H_
#define PROJECT_2_15_441_INC_CMU_TCP_H_

#include <netinet/in.h>
#include <pthread.h>
#include <stdint.h>
#include <sys/socket.h>
#include <sys/types.h>

#include "cmu_packet.h"
#include "grading.h"

#define EXIT_SUCCESS 0
#define EXIT_ERROR -1
#define EXIT_FAILURE 1

typedef struct {
  uint32_t next_seq_expected;
  uint32_t last_ack_received;
  uint32_t next_seq_to_send;
  // Flow control: the most recent advertised window received from the peer.
  // This is the amount of data (in bytes) the sender is allowed to have
  // outstanding, as reported by the receiver. Protected by window_lock.
  uint16_t adv_window_from_receiver;
} window_t;

/**
 * TCP Reno congestion-control stages.
 */
typedef enum {
  CC_SLOW_START = 0,
  CC_CONGESTION_AVOIDANCE,
  CC_FAST_RECOVERY,
} cc_state_t;

/**
 * Connection states for TCP handshake and data transfer.
 */
typedef enum {
  CLOSED = 0,
  LISTEN,
  SYN_SENT,
  SYN_RECEIVED,
  ESTABLISHED,
  CLOSING
} tcp_state_t;

/**
 * Structure to hold packets for retransmission.
 */
typedef struct packet_buf {
  uint8_t *packet;
  uint16_t packet_len;
  uint32_t seq_num;
  struct packet_buf *next;
} packet_buf_t;

/**
 * Structure to hold a received out-of-order data segment, pending the arrival
 * of the missing earlier bytes. Kept in a list sorted by seq_num.
 */
typedef struct ofo_buf {
  uint8_t *payload;
  uint16_t len;
  uint32_t seq_num;
  struct ofo_buf *next;
} ofo_buf_t;

/**
 * CMU-TCP socket types. (DO NOT CHANGE.)
 */
typedef enum {
  TCP_INITIATOR = 0,
  TCP_LISTENER = 1,
} cmu_socket_type_t;

/**
 * This structure holds the state of a socket. You may modify this structure as
 * you see fit to include any additional state you need for your implementation.
 */
typedef struct {
  int socket;
  pthread_t thread_id;
  uint16_t my_port;
  struct sockaddr_in conn;
  uint8_t* received_buf;
  int received_len;
  pthread_mutex_t recv_lock;
  pthread_cond_t wait_cond;
  uint8_t* sending_buf;
  int sending_len;
  cmu_socket_type_t type;
  pthread_mutex_t send_lock;
  int dying;
  pthread_mutex_t death_lock;
  window_t window;
  tcp_state_t state;
  packet_buf_t* packet_buffer_head;
  pthread_mutex_t window_lock;
  // Out-of-order receive buffer: data segments received ahead of a gap,
  // held (sorted by seq_num) until the missing earlier bytes arrive. Protected
  // by recv_lock (same as received_buf). ofo_bytes tracks total buffered here.
  ofo_buf_t* ofo_head;
  int ofo_bytes;
  // Congestion control (TCP Reno). All protected by window_lock.
  uint32_t cwnd;            // Congestion window, in bytes.
  uint32_t ssthresh;        // Slow-start threshold, in bytes.
  uint32_t dup_ack_count;   // Count of consecutive identical ACKs (1 == first).
  cc_state_t cc_state;      // Current congestion-control stage.
  // Retransmission timer. timer_running indicates whether a timer is active for
  // the oldest outstanding segment; timer_start_ms is when it was (re)started,
  // in milliseconds (CLOCK_MONOTONIC). A timeout fires when the elapsed time
  // since timer_start_ms reaches DEFAULT_TIMEOUT, independent of dup ACKs.
  int timer_running;
  uint64_t timer_start_ms;
  // --- TCP Vegas (delay-based) congestion control. Protected by window_lock.
  // Vegas keeps the bottleneck queue short by reacting to RTT inflation rather
  // than waiting for loss, which both lowers latency (good for the game flow in
  // Bufferbloat) and avoids the loss-driven cwnd collapse that wastes goodput.
  //
  // RTT is sampled once per round trip: when no sample is outstanding, the next
  // segment sent is "timed" by recording its end sequence number (rtt_seq) and
  // send time (rtt_send_ms). When a cumulative ACK first covers rtt_seq, the
  // elapsed time is one RTT sample.
  uint32_t base_rtt_ms;   // Minimum RTT ever seen (empty-queue propagation).
  uint32_t rtt_sample_ms; // Most recent RTT sample.
  int rtt_pending;        // 1 while a timed segment is outstanding.
  uint32_t rtt_seq;       // End sequence number of the timed segment.
  uint64_t rtt_send_ms;   // Time the timed segment was sent (CLOCK_MONOTONIC).
  // Vegas updates cwnd once per RTT. round_end_seq marks the send-sequence
  // boundary of the current round; when last_ack_received passes it, a round
  // has completed and the Vegas control law runs once.
  uint32_t round_end_seq;
  int vegas_inited;       // 1 once base_rtt has a real sample.
} cmu_socket_t;

/*
 * DO NOT CHANGE THE DECLARATIONS BELOW
 */

/**
 * Read mode flags supported by a CMU-TCP socket.
 */
typedef enum {
  NO_FLAG = 0,  // Default behavior: block indefinitely until data is available.
  NO_WAIT,      // Return immediately if no data is available.
  TIMEOUT,      // Block until data is available or the timeout is reached.
} cmu_read_mode_t;

/**
 * Constructs a CMU-TCP socket.
 *
 * An Initiator socket is used to connect to a Listener socket.
 *
 * @param sock The structure with the socket state. It will be initialized by
 *             this function.
 * @param socket_type Indicates the type of socket: Listener or Initiator.
 * @param port Port to either connect to, or bind to. (Based on socket_type.)
 * @param server_ip IP address of the server to connect to. (Only used if the
 *                 socket is an initiator.)
 *
 * @return 0 on success, -1 on error.
 */
int cmu_socket(cmu_socket_t* sock, const cmu_socket_type_t socket_type,
               const int port, const char* server_ip);

/**
 * Closes a CMU-TCP socket.
 *
 * @param sock The socket to close.
 *
 * @return 0 on success, -1 on error.
 */
int cmu_close(cmu_socket_t* sock);

/**
 * Reads data from a CMU-TCP socket.
 *
 * If there is data available in the socket buffer, it is placed in the
 * destination buffer.
 *
 * @param sock The socket to read from.
 * @param buf The buffer to read into.
 * @param length The maximum number of bytes to read.
 * @param flags Flags that determine how the socket should wait for data. Check
 *             `cmu_read_mode_t` for more information. `TIMEOUT` is not
 *             implemented for CMU-TCP.
 *
 * @return The number of bytes read on success, -1 on error.
 */
int cmu_read(cmu_socket_t* sock, void* buf, const int length,
             cmu_read_mode_t flags);

/**
 * Writes data to a CMU-TCP socket.
 *
 * @param sock The socket to write to.
 * @param buf The data to write.
 * @param length The number of bytes to write.
 *
 * @return 0 on success, -1 on error.
 */
int cmu_write(cmu_socket_t* sock, const void* buf, int length);

/*
 * You can declare more functions after this point if you need to.
 */

#endif  // PROJECT_2_15_441_INC_CMU_TCP_H_
