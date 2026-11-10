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
 * This file implements the CMU-TCP backend. The backend runs in a different
 * thread and handles all the socket operations separately from the application.
 *
 * This is where most of your code should go. Feel free to modify any function
 * in this file.
 */

#include "backend.h"

#include <poll.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <time.h>

#include "cmu_packet.h"
#include "cmu_tcp.h"

#define MIN(X, Y) (((X) < (Y)) ? (X) : (Y))
#define MAX(X, Y) (((X) > (Y)) ? (X) : (Y))

// --- TCP Vegas tuning (delay-based congestion control) ---------------------
// Vegas keeps the estimated number of queued bytes (diff) in [ALPHA, BETA].
// Below ALPHA the path is under-utilised, so cwnd grows; above BETA the queue
// is building, so cwnd shrinks; in between it holds. GAMMA governs the slow
// -start -> congestion-avoidance transition. Values are in bytes, expressed as
// small multiples of MSS. With the local test's BDP (~7 MSS), holding ~2-4 MSS
// of queue keeps the bottleneck busy without filling the 30-packet buffer.
#define VEGAS_ALPHA ((uint32_t)MSS / 2u)
#define VEGAS_BETA (1u * (uint32_t)MSS)
#define VEGAS_GAMMA (1u * (uint32_t)MSS)

/**
 * Computes the advertised window to report to the peer: the amount of free
 * space remaining in the receive buffer. This is MAX_NETWORK_BUFFER minus the
 * bytes currently buffered but not yet consumed by the application.
 *
 * Caller may hold recv_lock; this only reads received_len.
 */
static uint16_t compute_adv_window(cmu_socket_t *sock) {
  int buffered = sock->received_len + sock->ofo_bytes;
  int free_space = MAX_NETWORK_BUFFER - buffered;
  if (free_space < 0) {
    free_space = 0;
  }
  if (free_space > MAX_NETWORK_BUFFER) {
    free_space = MAX_NETWORK_BUFFER;
  }
  return (uint16_t)free_space;
}

/**
 * Appends payload to the in-order received buffer and advances
 * next_seq_expected. Caller holds recv_lock (received_buf) and will handle
 * window_lock for next_seq_expected. Returns new next_seq_expected.
 */
static void deliver_in_order(cmu_socket_t *sock, uint8_t *payload,
                             uint16_t len) {
  sock->received_buf = realloc(sock->received_buf, sock->received_len + len);
  memcpy(sock->received_buf + sock->received_len, payload, len);
  sock->received_len += len;
  pthread_mutex_lock(&(sock->window_lock));
  sock->window.next_seq_expected += len;
  pthread_mutex_unlock(&(sock->window_lock));
}

/**
 * After delivering in-order data (next_seq_expected advanced), drain any
 * out-of-order segments that are now contiguous: while the lowest buffered
 * segment starts exactly at next_seq_expected, splice it into received_buf,
 * advance next_seq_expected, and remove it from the OOO list.
 *
 * Assumes the OOO list is sorted ascending by seq_num and caller holds
 * recv_lock.
 */
static void drain_ofo(cmu_socket_t *sock) {
  for (;;) {
    pthread_mutex_lock(&(sock->window_lock));
    uint32_t expected = sock->window.next_seq_expected;
    pthread_mutex_unlock(&(sock->window_lock));

    ofo_buf_t *head = sock->ofo_head;
    if (head == NULL || head->seq_num != expected) {
      break;
    }
    // Splice this segment into the in-order buffer.
    deliver_in_order(sock, head->payload, head->len);
    sock->ofo_head = head->next;
    sock->ofo_bytes -= head->len;
    free(head->payload);
    free(head);
  }
}

/**
 * Inserts an out-of-order segment into the sorted OOO list. Ignores duplicates
 * (same seq already buffered) and drops the segment if buffering it would
 * exceed MAX_NETWORK_BUFFER (ordered + unordered bytes). Caller holds
 * recv_lock. Returns 1 if buffered, 0 if ignored/dropped.
 */
static int insert_ofo(cmu_socket_t *sock, uint32_t seq, uint8_t *payload,
                      uint16_t len) {
  // Bound: ordered + unordered must stay under MAX_NETWORK_BUFFER.
  if (sock->received_len + sock->ofo_bytes + len > MAX_NETWORK_BUFFER) {
    return 0;  // no room; drop (sender will retransmit)
  }
  // Find insertion point (sorted ascending by seq); dedup exact seq match.
  ofo_buf_t *prev = NULL;
  ofo_buf_t *cur = sock->ofo_head;
  while (cur != NULL && before(cur->seq_num, seq)) {
    prev = cur;
    cur = cur->next;
  }
  if (cur != NULL && cur->seq_num == seq) {
    return 0;  // duplicate already buffered
  }
  ofo_buf_t *node = malloc(sizeof(ofo_buf_t));
  node->payload = malloc(len);
  memcpy(node->payload, payload, len);
  node->len = len;
  node->seq_num = seq;
  node->next = cur;
  if (prev == NULL) {
    sock->ofo_head = node;
  } else {
    prev->next = node;
  }
  sock->ofo_bytes += len;
  return 1;
}

/**
 * Computes the effective sending window: the minimum of the congestion window
 * (cwnd) and the receiver's advertised window. The sender may not have more
 * than this many bytes outstanding.
 *
 * Assumes the caller holds window_lock.
 */
static uint32_t effective_send_window(cmu_socket_t *sock) {
  uint32_t adv = sock->window.adv_window_from_receiver;
  uint32_t cwnd = sock->cwnd;
  return MIN(cwnd, adv);
}

/**
 * Adds a packet to the retransmission buffer.
 */
void add_packet_to_buffer(cmu_socket_t *sock, uint8_t *packet,
                          uint16_t packet_len, uint32_t seq_num) {
  packet_buf_t *new_packet = malloc(sizeof(packet_buf_t));
  new_packet->packet = malloc(packet_len);
  memcpy(new_packet->packet, packet, packet_len);
  new_packet->packet_len = packet_len;
  new_packet->seq_num = seq_num;
  new_packet->next = NULL;

  pthread_mutex_lock(&(sock->window_lock));
  if (sock->packet_buffer_head == NULL) {
    sock->packet_buffer_head = new_packet;
  } else {
    packet_buf_t *current = sock->packet_buffer_head;
    while (current->next != NULL) {
      current = current->next;
    }
    current->next = new_packet;
  }
  pthread_mutex_unlock(&(sock->window_lock));
}

/**
 * Removes acknowledged packets from the retransmission buffer.
 */
void remove_acked_packets(cmu_socket_t *sock, uint32_t ack_num) {
  pthread_mutex_lock(&(sock->window_lock));
  packet_buf_t *current = sock->packet_buffer_head;
  packet_buf_t *prev = NULL;

  while (current != NULL) {
    // A packet is fully acknowledged when the cumulative ACK covers its last
    // byte, i.e. ack_num >= seq_num + payload_len. Using the packet END (not
    // its start) ensures a partial ACK only frees packets that are completely
    // acknowledged, while still releasing window space mid-flight.
    uint16_t payload_len =
        current->packet_len - (uint16_t)sizeof(cmu_tcp_header_t);
    // For control packets (SYN) that consume one sequence number, payload_len
    // is 0; treat them as covering one byte of sequence space.
    uint32_t consumed = payload_len > 0 ? payload_len : 1;
    uint32_t pkt_end = current->seq_num + consumed;

    if (!after(pkt_end, ack_num)) {  // pkt_end <= ack_num  => fully acked
      packet_buf_t *to_free = current;
      if (prev == NULL) {
        sock->packet_buffer_head = current->next;
      } else {
        prev->next = current->next;
      }
      current = current->next;
      free(to_free->packet);
      free(to_free);
    } else {
      prev = current;
      current = current->next;
    }
  }
  pthread_mutex_unlock(&(sock->window_lock));
}

/**
 * Retransmits all packets in the buffer starting from last_ack_received.
 */
void retransmit_packets(cmu_socket_t *sock) {
  pthread_mutex_lock(&(sock->window_lock));
  packet_buf_t *current = sock->packet_buffer_head;
  socklen_t conn_len = sizeof(sock->conn);

  while (current != NULL) {
    sendto(sock->socket, current->packet, current->packet_len, 0,
           (struct sockaddr *)&(sock->conn), conn_len);
    current = current->next;
  }
  pthread_mutex_unlock(&(sock->window_lock));
}

/**
 * Retransmits the single "missing" segment: the one the receiver is waiting
 * for, i.e. the buffered packet whose sequence number equals the current
 * cumulative ACK point (last_ack_received). Used by fast retransmit and by
 * timeout recovery, per the student's design (single-segment retransmit
 * rather than Go-Back-N).
 *
 * Assumes the caller holds window_lock.
 */
static void retransmit_missing_segment(cmu_socket_t *sock) {
  socklen_t conn_len = sizeof(sock->conn);
  packet_buf_t *current = sock->packet_buffer_head;
  while (current != NULL) {
    if (current->seq_num == sock->window.last_ack_received) {
      sendto(sock->socket, current->packet, current->packet_len, 0,
             (struct sockaddr *)&(sock->conn), conn_len);
      return;
    }
    current = current->next;
  }
  // Fallback: if the exact segment isn't found (e.g. right at the window edge),
  // retransmit the earliest buffered packet so progress can still be made.
  if (sock->packet_buffer_head != NULL) {
    sendto(sock->socket, sock->packet_buffer_head->packet,
           sock->packet_buffer_head->packet_len, 0,
           (struct sockaddr *)&(sock->conn), conn_len);
  }
}

/**
 * Returns the current monotonic time in milliseconds.
 */
static uint64_t now_ms(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (uint64_t)ts.tv_sec * 1000ULL + (uint64_t)ts.tv_nsec / 1000000ULL;
}

/**
 * Returns the current monotonic time in microseconds. Used for Vegas RTT
 * sampling: at a 20 ms RTT, millisecond resolution is too coarse to detect a
 * 1-2 packet standing queue (which adds only a few hundred microseconds to a
 * few milliseconds), so RTT and base RTT are tracked in microseconds.
 */
static uint64_t now_us(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (uint64_t)ts.tv_sec * 1000000ULL + (uint64_t)ts.tv_nsec / 1000ULL;
}

static void log_cwnd(cmu_socket_t *sock);  // forward declaration
/**
 * TCP Vegas per-round congestion-window update. Called once per RTT, after a
 * fresh RTT sample (sock->rtt_sample_ms) and base RTT (sock->base_rtt_ms) are
 * available. Assumes the caller holds window_lock.
 * Vegas estimates the number of bytes queued at the bottleneck as
 *
 *   diff = (expected_throughput - actual_throughput) * base_rtt
 *        = cwnd - cwnd * base_rtt / rtt
 *        = cwnd * (rtt - base_rtt) / rtt
 *
 * and steers cwnd so diff stays in [ALPHA, BETA]: grow when the queue is too
 * small (link under-used), shrink when it is too large (queue building toward
 * overflow), hold otherwise. In slow start it doubles cwnd each round until
 * diff exceeds GAMMA, then switches to the linear congestion-avoidance regime.
 */
static void vegas_update(cmu_socket_t *sock) {
  uint32_t rtt = sock->rtt_sample_ms;
  uint32_t base = sock->base_rtt_ms;
  if (rtt == 0 || base == 0 || base == UINT32_MAX) {
    return;  // not enough information yet
  }

  // Estimated queued bytes. Guard against rtt < base (clock jitter) -> 0.
  uint32_t diff = 0;
  if (rtt > base) {
    diff = (uint32_t)(((uint64_t)sock->cwnd * (rtt - base)) / rtt);
  }

  if (sock->cc_state == CC_SLOW_START) {
    if (diff > VEGAS_GAMMA) {
      // Too much queue to keep doubling: enter congestion avoidance and pull
      // cwnd back to the target occupancy.
      sock->cc_state = CC_CONGESTION_AVOIDANCE;
      if (sock->cwnd > diff) {
        sock->cwnd -= (diff - VEGAS_ALPHA > 0 ? (diff - VEGAS_ALPHA) : 0);
      }
    } else {
      // Exponential growth, one MSS per ACKed segment this round (~doubling),
      // but capped so a single round cannot overshoot wildly.
      sock->cwnd += MSS;
    }
  } else {  // CC_CONGESTION_AVOIDANCE (and treat FAST_RECOVERY like CA here)
    sock->cc_state = CC_CONGESTION_AVOIDANCE;
    // Proportional convergence instead of a fixed +/- MSS step. We want the
    // estimated queue (diff) to equal a small target (~1 packet). The queue
    // error is (diff - target); correcting cwnd by a damped fraction of that
    // error drives the operating point toward the target without the limit
    // -cycle overshoot that a full-MSS step per round produces. This keeps the
    // standing queue near one packet, minimising the game flow's added delay
    // while leaving just enough in flight to keep the link busy.
    int32_t target = (int32_t)((uint32_t)MSS / 2u);  // desired queue occupancy
    int32_t err = (int32_t)diff - target;         // >0: too much queue
    int32_t step = err / 4;                       // gentle gain -> less ringing
    if (step == 0) {
      step = (err > 0) ? 1 : (err < 0 ? -1 : 0);  // always nudge toward target
    }
    // Clamp the per-round change so a single noisy sample cannot swing cwnd far
    // and set up a large oscillation (the limit cycle that spikes p95 lag).
    int32_t cap = (int32_t)((uint32_t)MSS / 2u);
    if (step > cap) step = cap;
    if (step < -cap) step = -cap;
    int32_t new_cwnd = (int32_t)sock->cwnd - step;
    if (new_cwnd < (int32_t)MSS) {
      new_cwnd = (int32_t)MSS;
    }
    sock->cwnd = (uint32_t)new_cwnd;
  }

  // Never let cwnd fall below one segment.
  if (sock->cwnd < (uint32_t)MSS) {
    sock->cwnd = (uint32_t)MSS;
  }
  log_cwnd(sock);
}

/**
 * (Re)starts the retransmission timer. Assumes the caller holds window_lock.
 */
static void timer_start(cmu_socket_t *sock) {
  sock->timer_running = 1;
  sock->timer_start_ms = now_ms();
}

/**
 * Stops the retransmission timer. Assumes the caller holds window_lock.
 */
static void timer_stop(cmu_socket_t *sock) { sock->timer_running = 0; }

/**
 * Returns 1 if the retransmission timer is running and has expired (elapsed
 * >= DEFAULT_TIMEOUT). Assumes the caller holds window_lock.
 */
static int timer_expired(cmu_socket_t *sock) {
  if (!sock->timer_running) {
    return 0;
  }
  return (now_ms() - sock->timer_start_ms) >= (uint64_t)DEFAULT_TIMEOUT;
}

/**
 * Logs the congestion window state whenever it changes, for the CP2 report.
 * Format: timestamp_ms,cwnd,ssthresh,stage
 *
 * Assumes the caller holds window_lock.
 */
static void log_cwnd(cmu_socket_t *sock) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  long ms = ts.tv_sec * 1000L + ts.tv_nsec / 1000000L;
  const char *stage = sock->cc_state == CC_SLOW_START          ? "SLOW_START"
                      : sock->cc_state == CC_CONGESTION_AVOIDANCE
                          ? "CONGESTION_AVOIDANCE"
                          : "FAST_RECOVERY";
  fprintf(stderr, "CWND,%ld,%u,%u,%s\n", ms, sock->cwnd, sock->ssthresh, stage);
}

/**
 * TCP Reno: handle a new (window-advancing) ACK.
 * Assumes the caller holds window_lock.
 */
static void on_new_ack(cmu_socket_t *sock) {
  switch (sock->cc_state) {
    case CC_SLOW_START:
      sock->cwnd += MSS;
      sock->dup_ack_count = 0;
      if (sock->cwnd >= sock->ssthresh) {
        sock->cc_state = CC_CONGESTION_AVOIDANCE;
      }
      break;
    case CC_CONGESTION_AVOIDANCE:
      // cwnd += MSS * (MSS / cwnd). Compute with integer math carefully so the
      // increment isn't truncated to zero: (MSS*MSS)/cwnd.
      sock->cwnd += (uint32_t)MSS * (uint32_t)MSS / sock->cwnd;
      sock->dup_ack_count = 0;
      break;
    case CC_FAST_RECOVERY:
      // A new ACK ends fast recovery: deflate cwnd to ssthresh and resume
      // congestion avoidance.
      sock->cwnd = sock->ssthresh;
      sock->dup_ack_count = 0;
      sock->cc_state = CC_CONGESTION_AVOIDANCE;
      break;
  }
  log_cwnd(sock);
}

/**
 * TCP Reno: handle a duplicate ACK.
 * Assumes the caller holds window_lock.
 */
static void on_dup_ack(cmu_socket_t *sock) {
  if (sock->cc_state == CC_FAST_RECOVERY) {
    // Already in fast recovery: inflate the window by one MSS for each dup ACK
    // (each dup implies a packet left the network) and allow a new segment.
    sock->cwnd += MSS;
    log_cwnd(sock);
    return;
  }

  // Slow start or congestion avoidance: count the duplicate.
  sock->dup_ack_count++;
  if (sock->dup_ack_count == 3) {  // 3rd duplicate ACK
    sock->ssthresh = sock->cwnd / 2;
    sock->cwnd = sock->ssthresh + 3 * MSS;
    retransmit_missing_segment(sock);
    // Karn's algorithm: do not measure RTT across a retransmission. Discard any
    // in-flight Vegas RTT sample so the next fresh segment restarts timing.
    sock->rtt_pending = 0;
    sock->dup_ack_count = 0;
    sock->cc_state = CC_FAST_RECOVERY;
    log_cwnd(sock);
  }
}

/**
 * TCP Reno: handle a retransmission timeout.
 * Assumes the caller holds window_lock.
 */
static void on_timeout(cmu_socket_t *sock) {
  // ssthresh = cwnd/2, but never below 2*MSS. Flooring prevents repeated
  // timeouts (during a long silence) from collapsing ssthresh so low that the
  // connection cannot stay in slow start when ACKs resume.
  uint32_t half = sock->cwnd / 2;
  uint32_t floor = 2 * (uint32_t)MSS;
  sock->ssthresh = half > floor ? half : floor;
  sock->cwnd = WINDOW_INITIAL_WINDOW_SIZE;  // 1 MSS
  sock->dup_ack_count = 0;
  // EXPERIMENT BUILD: retransmit the entire outstanding window (Go-Back-N) on
  // timeout. A timeout signals heavy loss; resending only one segment recovers
  // just one packet per timeout and cannot recover a whole window of dropped
  // segments (there is no out-of-order buffering), which stalls bulk transfers.
  // Whole-window retransmit recovers multi-segment loss so transfers complete.
  retransmit_packets(sock);
  sock->cc_state = CC_SLOW_START;
  // Karn's algorithm: a retransmission makes the outstanding RTT sample
  // ambiguous, so discard it. Restart the Vegas round boundary at the current
  // send sequence so the control law resumes cleanly once ACKs return.
  sock->rtt_pending = 0;
  sock->round_end_seq = sock->window.next_seq_to_send;
  // Restart the timer so a lost retransmission triggers another timeout.
  timer_start(sock);
  log_cwnd(sock);
}

/**
 * Tells if a given sequence number has been acknowledged by the socket.
 *
 * @param sock The socket to check for acknowledgements.
 * @param seq Sequence number to check.
 *
 * @return 1 if the sequence number has been acknowledged, 0 otherwise.
 */
int has_been_acked(cmu_socket_t *sock, uint32_t seq) {
  int result;
  pthread_mutex_lock(&(sock->window_lock));
  result = after(sock->window.last_ack_received, seq);
  pthread_mutex_unlock(&(sock->window_lock));
  return result;
}

/**
 * Updates the socket information to represent the newly received packet.
 *
 * Handles SYN, SYN-ACK, ACK, and data packets based on connection state.
 *
 * @param sock The socket used for handling packets received.
 * @param pkt The packet data received by the socket.
 */
void handle_message(cmu_socket_t *sock, uint8_t *pkt) {
  cmu_tcp_header_t *hdr = (cmu_tcp_header_t *)pkt;
  uint8_t flags = get_flags(hdr);
  uint32_t seq = get_seq(hdr);
  uint32_t ack = get_ack(hdr);
  socklen_t conn_len = sizeof(sock->conn);

  // Validate identifier
  if (hdr->identifier != htonl(IDENTIFIER)) {
    return;
  }

  // Handle based on connection state
  switch (sock->state) {
    case LISTEN: {
      // Listener waiting for SYN
      if (flags == SYN_FLAG_MASK) {
        // Received SYN, send SYN-ACK
        pthread_mutex_lock(&(sock->window_lock));
        sock->window.next_seq_expected = seq + 1;

        uint16_t src = sock->my_port;
        uint16_t dst = ntohs(sock->conn.sin_port);
        uint32_t syn_ack_seq = sock->window.next_seq_to_send;
        uint32_t syn_ack_ack = sock->window.next_seq_expected;
        uint16_t hlen = sizeof(cmu_tcp_header_t);
        uint16_t plen = hlen;
        uint8_t syn_ack_flags = SYN_FLAG_MASK | ACK_FLAG_MASK;
        uint16_t adv_window = compute_adv_window(sock);
        uint16_t ext_len = 0;

        uint8_t *syn_ack_packet =
            create_packet(src, dst, syn_ack_seq, syn_ack_ack, hlen, plen,
                          syn_ack_flags, adv_window, ext_len, NULL, NULL, 0);

        sendto(sock->socket, syn_ack_packet, plen, 0,
               (struct sockaddr *)&(sock->conn), conn_len);

        // Store SYN-ACK for retransmission
        add_packet_to_buffer(sock, syn_ack_packet, plen, syn_ack_seq);
        free(syn_ack_packet);

        sock->window.next_seq_to_send += 1;
        sock->state = SYN_RECEIVED;
        pthread_mutex_unlock(&(sock->window_lock));
      }
      break;
    }

    case SYN_RECEIVED: {
      // Listener waiting for ACK
      if (flags == ACK_FLAG_MASK) {
        pthread_mutex_lock(&(sock->window_lock));
        if (ack == sock->window.next_seq_to_send) {
          // Valid ACK, connection established
          sock->window.last_ack_received = ack;
          remove_acked_packets(sock, ack);
          sock->state = ESTABLISHED;
        }
        pthread_mutex_unlock(&(sock->window_lock));
      } else if (flags == SYN_FLAG_MASK) {
        // Duplicate SYN, resend SYN-ACK
        pthread_mutex_lock(&(sock->window_lock));
        sock->window.next_seq_expected = seq + 1;

        uint16_t src = sock->my_port;
        uint16_t dst = ntohs(sock->conn.sin_port);
        uint32_t syn_ack_seq = sock->window.last_ack_received;
        uint32_t syn_ack_ack = sock->window.next_seq_expected;
        uint16_t hlen = sizeof(cmu_tcp_header_t);
        uint16_t plen = hlen;
        uint8_t syn_ack_flags = SYN_FLAG_MASK | ACK_FLAG_MASK;
        uint16_t adv_window = compute_adv_window(sock);
        uint16_t ext_len = 0;

        uint8_t *syn_ack_packet =
            create_packet(src, dst, syn_ack_seq, syn_ack_ack, hlen, plen,
                          syn_ack_flags, adv_window, ext_len, NULL, NULL, 0);

        sendto(sock->socket, syn_ack_packet, plen, 0,
               (struct sockaddr *)&(sock->conn), conn_len);
        free(syn_ack_packet);
        pthread_mutex_unlock(&(sock->window_lock));
      }
      break;
    }

    case CLOSED: {
      // Initiator: expecting SYN-ACK in response to SYN
      if ((flags & (SYN_FLAG_MASK | ACK_FLAG_MASK)) ==
          (SYN_FLAG_MASK | ACK_FLAG_MASK)) {
        pthread_mutex_lock(&(sock->window_lock));
        // Validate ACK number
        if (ack == sock->window.next_seq_to_send) {
          // Valid SYN-ACK
          sock->window.next_seq_expected = seq + 1;
          sock->window.last_ack_received = ack;

          // Remove SYN from buffer
          remove_acked_packets(sock, ack);

          // Send ACK
          uint16_t src = sock->my_port;
          uint16_t dst = ntohs(sock->conn.sin_port);
          uint32_t ack_seq = sock->window.next_seq_to_send;
          uint32_t ack_ack = sock->window.next_seq_expected;
          uint16_t hlen = sizeof(cmu_tcp_header_t);
          uint16_t plen = hlen;
          uint8_t ack_flags = ACK_FLAG_MASK;
          uint16_t adv_window = compute_adv_window(sock);
          uint16_t ext_len = 0;

          uint8_t *ack_packet =
              create_packet(src, dst, ack_seq, ack_ack, hlen, plen, ack_flags,
                            adv_window, ext_len, NULL, NULL, 0);

          sendto(sock->socket, ack_packet, plen, 0,
                 (struct sockaddr *)&(sock->conn), conn_len);
          free(ack_packet);

          sock->state = ESTABLISHED;
        }
        pthread_mutex_unlock(&(sock->window_lock));
      }
      break;
    }

    case ESTABLISHED: {
      // Handle ACKs
      if (flags & ACK_FLAG_MASK) {
        pthread_mutex_lock(&(sock->window_lock));
        // Flow control: record the peer's latest advertised window so the
        // send loop knows how much data it may keep outstanding.
        sock->window.adv_window_from_receiver = get_advertised_window(hdr);
        if (after(ack, sock->window.last_ack_received)) {
          // New ACK, slide window forward and drive congestion control.
          sock->window.last_ack_received = ack;
          remove_acked_packets(sock, ack);

          // --- Vegas RTT sampling. If the timed segment is now acknowledged,
          // record one RTT sample and maintain a WINDOWED minimum base RTT.
          if (sock->rtt_pending && !after(sock->rtt_seq, ack)) {
            uint64_t now = now_us();
            uint64_t sample = now - sock->rtt_send_ms;  // microseconds
            if (sample == 0) {
              sample = 1;  // clamp to avoid divide-by-zero
            }
            sock->rtt_sample_ms = (uint32_t)sample;

            // Track the minimum within the current observation window.
            if ((uint32_t)sample < sock->win_min_us) {
              sock->win_min_us = (uint32_t)sample;
            }

            if (!sock->vegas_inited) {
              // First sample: seed everything.
              sock->base_rtt_ms = (uint32_t)sample;
              sock->base_rtt_stamp_us = now;
              sock->win_min_us = (uint32_t)sample;
              sock->win_start_us = now;
            } else if ((uint32_t)sample < sock->base_rtt_ms) {
              // base_rtt can DROP immediately (we just saw an emptier queue).
              sock->base_rtt_ms = (uint32_t)sample;
              sock->base_rtt_stamp_us = now;
            } else {
              // base_rtt can only RISE after a full window confirms the old
              // minimum is stale. The window is ~10 base RTTs (bounded), long
              // enough to ignore transient queueing but short enough to notice
              // a real propagation-delay increase within a handful of RTTs.
              uint64_t win_len = (uint64_t)sock->base_rtt_ms * 5ULL;
              if (win_len < 30000ULL) win_len = 30000ULL;      // >= 30 ms
              if (win_len > 200000ULL) win_len = 200000ULL;    // <= 200 ms
              if (now - sock->win_start_us >= win_len) {
                // Adopt the window's minimum as the new base (it may be larger
                // than the old base: that is the delay-increase case).
                sock->base_rtt_ms = sock->win_min_us;
                sock->base_rtt_stamp_us = now;
                sock->win_min_us = (uint32_t)sample;  // restart window
                sock->win_start_us = now;
              }
            }

            sock->rtt_pending = 0;
            sock->vegas_inited = 1;
          }

          // --- Vegas per-round control. Run the cwnd update once per RTT:
          // when this ACK passes the sequence that ended the previous round.
          if (sock->vegas_inited && !after(sock->round_end_seq, ack)) {
            vegas_update(sock);
            // The next round covers everything currently in flight.
            sock->round_end_seq = sock->window.next_seq_to_send;
          }
          sock->dup_ack_count = 0;

          // Restart the retransmission timer for the next-oldest outstanding
          // segment, or stop it if everything is acknowledged.
          if (sock->packet_buffer_head != NULL) {
            timer_start(sock);
          } else {
            timer_stop(sock);
          }
        } else if (ack == sock->window.last_ack_received) {
          // Duplicate ACK. Only treat it as a loss signal (count it) when there
          // is outstanding unacknowledged data it could be signaling loss for.
          // Repeated ACKs with nothing in flight (e.g. the peer piggybacking
          // ACKs on its own data) are not congestion signals and must be
          // ignored, otherwise we spuriously enter fast recovery.
          if (sock->window.next_seq_to_send != sock->window.last_ack_received) {
            on_dup_ack(sock);
          }
        }
        pthread_mutex_unlock(&(sock->window_lock));
      }

      // Handle data packets
      if (get_payload_len(pkt) > 0) {
        pthread_mutex_lock(&(sock->window_lock));
        uint32_t expected_seq = sock->window.next_seq_expected;
        pthread_mutex_unlock(&(sock->window_lock));

        uint16_t payload_len = get_payload_len(pkt);
        uint8_t *payload = get_payload(pkt);

        // NOTE: recv_lock is already held by check_for_data(), the only caller
        // of handle_message(), so received_buf / ofo list are accessed safely.
        if (seq == expected_seq) {
          // In-order: deliver, then drain any now-contiguous buffered segments.
          deliver_in_order(sock, payload, payload_len);
          drain_ofo(sock);
        } else if (after(seq, expected_seq)) {
          // Future data (a gap exists): buffer it out of order so that after
          // the missing segment is retransmitted we can acknowledge everything
          // at once. Duplicates and over-buffer are handled inside insert_ofo.
          insert_ofo(sock, seq, payload, payload_len);
        }
        // else seq < expected: already-received old data -> drop.

        // Send an ACK reflecting the current cumulative point. For in-order
        // data this acknowledges new bytes; for out-of-order/duplicate data it
        // is a duplicate ACK, which drives the sender's fast retransmit.
        pthread_mutex_lock(&(sock->window_lock));
        uint16_t src = sock->my_port;
        uint16_t dst = ntohs(sock->conn.sin_port);
        uint32_t ack_seq = sock->window.next_seq_to_send;
        uint32_t ack_ack = sock->window.next_seq_expected;
        pthread_mutex_unlock(&(sock->window_lock));

        uint16_t hlen = sizeof(cmu_tcp_header_t);
        uint16_t plen = hlen;
        uint8_t ack_flags = ACK_FLAG_MASK;
        uint16_t adv_window = compute_adv_window(sock);
        uint16_t ext_len = 0;

        uint8_t *ack_packet =
            create_packet(src, dst, ack_seq, ack_ack, hlen, plen, ack_flags,
                          adv_window, ext_len, NULL, NULL, 0);

        sendto(sock->socket, ack_packet, plen, 0,
               (struct sockaddr *)&(sock->conn), conn_len);
        free(ack_packet);
      }
      break;
    }

    default:
      break;
  }
}

/**
 * Checks if the socket received any data.
 *
 * It first peeks at the header to figure out the length of the packet and then
 * reads the entire packet.
 *
 * @param sock The socket used for receiving data on the connection.
 * @param flags Flags that determine how the socket should wait for data. Check
 *             `cmu_read_mode_t` for more information.
 */
void check_for_data(cmu_socket_t *sock, cmu_read_mode_t flags) {
  cmu_tcp_header_t hdr;
  uint8_t *pkt;
  socklen_t conn_len = sizeof(sock->conn);
  ssize_t len = 0;
  uint32_t plen = 0, buf_size = 0, n = 0;

  while (pthread_mutex_lock(&(sock->recv_lock)) != 0) {
  }
  switch (flags) {
    case NO_FLAG:
      len = recvfrom(sock->socket, &hdr, sizeof(cmu_tcp_header_t), MSG_PEEK,
                     (struct sockaddr *)&(sock->conn), &conn_len);
      break;
    case TIMEOUT: {
      // Using `poll` here so that we can specify a timeout.
      struct pollfd ack_fd;
      ack_fd.fd = sock->socket;
      ack_fd.events = POLLIN;
      // Timeout after DEFAULT_TIMEOUT.
      if (poll(&ack_fd, 1, DEFAULT_TIMEOUT) <= 0) {
        break;
      }
    }
    // Fallthrough.
    case NO_WAIT:
      len = recvfrom(sock->socket, &hdr, sizeof(cmu_tcp_header_t),
                     MSG_DONTWAIT | MSG_PEEK, (struct sockaddr *)&(sock->conn),
                     &conn_len);
      break;
    default:
      perror("ERROR unknown flag");
  }
  if (len >= (ssize_t)sizeof(cmu_tcp_header_t)) {
    plen = get_plen(&hdr);
    pkt = malloc(plen);
    while (buf_size < plen) {
      n = recvfrom(sock->socket, pkt + buf_size, plen - buf_size, 0,
                   (struct sockaddr *)&(sock->conn), &conn_len);
      buf_size = buf_size + n;
    }
    handle_message(sock, pkt);
    free(pkt);
  }
  pthread_mutex_unlock(&(sock->recv_lock));
}

/**
 * Breaks up the data into packets and sends with windowing.
 *
 * Implements windowing with Go-Back-N retransmission.
 *
 * @param sock The socket to use for sending data.
 * @param data The data to be sent.
 * @param buf_len The length of the data being sent.
 */
void single_send(cmu_socket_t *sock, uint8_t *data, int buf_len) {
  uint8_t *msg;
  uint8_t *data_offset = data;
  size_t conn_len = sizeof(sock->conn);
  int sockfd = sock->socket;

  if (buf_len <= 0) {
    return;
  }

  while (buf_len > 0) {
    // Determine how much data we may have outstanding: the receiver's
    // advertised window (flow control). Compute current usage too.
    pthread_mutex_lock(&(sock->window_lock));
    uint32_t window_used =
        sock->window.next_seq_to_send - sock->window.last_ack_received;
    uint32_t effective_window = effective_send_window(sock);
    pthread_mutex_unlock(&(sock->window_lock));

    // How much room is left in the current send window.
    uint32_t room = (effective_window > window_used)
                        ? (effective_window - window_used)
                        : 0;
    // Smallest amount we could usefully send next: a full MSS, or the tail of
    // the application data if less than MSS remains.
    uint32_t need = ((uint32_t)buf_len < (uint32_t)MSS) ? (uint32_t)buf_len
                                                        : (uint32_t)MSS;

    // Wait until there is room for the next send. We do NOT inject any probe
    // packets here: injecting a data byte without advancing next_seq_to_send
    // desynchronizes the byte stream. Instead we rely on (a) ordinary ACKs from
    // the receiver freeing window space, and (b) the receiver proactively
    // sending a window-update ACK from cmu_read() when the application drains
    // its buffer (which reopens a collapsed window). A genuine retransmission
    // timeout is still handled so we recover from loss.
    while (room < need && buf_len > 0) {
      check_for_data(sock, TIMEOUT);

      pthread_mutex_lock(&(sock->window_lock));
      window_used =
          sock->window.next_seq_to_send - sock->window.last_ack_received;
      effective_window = effective_send_window(sock);
      if (timer_expired(sock) && sock->packet_buffer_head != NULL) {
        on_timeout(sock);
        effective_window = effective_send_window(sock);
      }
      pthread_mutex_unlock(&(sock->window_lock));

      room = (effective_window > window_used)
                 ? (effective_window - window_used)
                 : 0;
    }

    if (buf_len <= 0) {
      break;
    }

    // At this point room >= need: there is space for the next full segment (or
    // the final partial tail). Fall through to the send logic below.

    // Case B: the window is open but already fully used. Wait for ACKs to free
    // space. Fire a timeout only when the retransmission timer actually
    // expires (time-based), independent of duplicate ACKs; dup ACKs are
    // handled separately by fast retransmit.
    while (effective_window > 0 && window_used >= effective_window) {
      check_for_data(sock, TIMEOUT);

      pthread_mutex_lock(&(sock->window_lock));
      window_used =
          sock->window.next_seq_to_send - sock->window.last_ack_received;
      if (timer_expired(sock) && sock->packet_buffer_head != NULL) {
        on_timeout(sock);
      }
      effective_window = effective_send_window(sock);
      pthread_mutex_unlock(&(sock->window_lock));

      // If the window closed entirely while waiting, hand off to the
      // zero-window probe logic on the next outer-loop iteration.
      if (effective_window == 0) {
        break;
      }
    }

    // If the window is now zero, restart the outer loop to probe.
    if (effective_window == 0) {
      continue;
    }

    // Send next packet.
    //
    // Chunking rule (alignment-proof): send a FULL MSS whenever a full MSS fits
    // within the window (inflight + MSS <= effective_window). Only send a
    // partial segment when it is the genuine tail of the application data
    // (buf_len < MSS). We never emit a sub-MSS "runt" merely to fill the exact
    // remaining window space, because the congestion window's fractional
    // congestion-avoidance increments would otherwise produce misaligned runt
    // segments and break clean window growth.
    uint32_t inflight = window_used;
    if ((uint32_t)buf_len >= (uint32_t)MSS) {
      // A full segment is available from the application. Only send it if a
      // full MSS fits in the window; otherwise wait for ACKs.
      if (inflight + (uint32_t)MSS > effective_window) {
        check_for_data(sock, TIMEOUT);
        pthread_mutex_lock(&(sock->window_lock));
        if (timer_expired(sock) && sock->packet_buffer_head != NULL) {
          on_timeout(sock);
        }
        pthread_mutex_unlock(&(sock->window_lock));
        continue;
      }
    } else {
      // Tail of the data (less than a full MSS). Send it only if it fits in the
      // window.
      if (inflight + (uint32_t)buf_len > effective_window) {
        check_for_data(sock, TIMEOUT);
        pthread_mutex_lock(&(sock->window_lock));
        if (timer_expired(sock) && sock->packet_buffer_head != NULL) {
          on_timeout(sock);
        }
        pthread_mutex_unlock(&(sock->window_lock));
        continue;
      }
    }

    uint16_t payload_len = (uint16_t)MIN((uint32_t)buf_len, (uint32_t)MSS);

    pthread_mutex_lock(&(sock->window_lock));
    uint16_t src = sock->my_port;
    uint16_t dst = ntohs(sock->conn.sin_port);
    uint32_t seq = sock->window.next_seq_to_send;
    uint32_t ack = sock->window.next_seq_expected;
    uint16_t hlen = sizeof(cmu_tcp_header_t);
    uint16_t plen = hlen + payload_len;
    uint8_t flags = ACK_FLAG_MASK;
    uint16_t adv_window = compute_adv_window(sock);
    uint16_t ext_len = 0;
    uint8_t *ext_data = NULL;
    uint8_t *payload = data_offset;

    msg = create_packet(src, dst, seq, ack, hlen, plen, flags, adv_window,
                        ext_len, ext_data, payload, payload_len);

    sendto(sockfd, msg, plen, 0, (struct sockaddr *)&(sock->conn), conn_len);

    // Store packet for retransmission
    add_packet_to_buffer(sock, msg, plen, seq);

    sock->window.next_seq_to_send += payload_len;
    // Vegas RTT sampling: if no segment is currently being timed, time this
    // one. We record its END sequence number so the sample completes when a
    // cumulative ACK first covers the whole segment. One sample per RTT is
    // enough for the per-round control law.
    if (!sock->rtt_pending) {
      sock->rtt_pending = 1;
      sock->rtt_seq = sock->window.next_seq_to_send;  // end seq of this segment
      sock->rtt_send_ms = now_us();  // stored in microseconds
    }
    // Sending new data marks forward progress, so any prior run of duplicate
    // ACKs is no longer a loss signal for the current flight. Reset the
    // duplicate counter so stale/early duplicate ACKs cannot spuriously
    // accumulate to 3 and trigger fast retransmit.
    sock->dup_ack_count = 0;
    // Start the retransmission timer for the oldest outstanding segment if it
    // is not already running.
    if (!sock->timer_running) {
      timer_start(sock);
    }
    pthread_mutex_unlock(&(sock->window_lock));

    free(msg);

    buf_len -= payload_len;
    data_offset += payload_len;

    // Check for ACKs while sending
    check_for_data(sock, NO_WAIT);

    // --- Pacing. Spread this window's packets evenly across the RTT instead of
    // releasing them as one burst. A burst of a whole window (~BDP worth of
    // packets) transiently fills the bottleneck queue even when the average
    // rate is correct, which is exactly what inflates the game flow's p95
    // latency. Pacing holds the instantaneous queue near the Vegas target.
    //
    // Inter-packet gap = time to drain one segment at the path rate
    //   = base_rtt / (cwnd / MSS) = base_rtt_us * MSS / cwnd  (microseconds).
    // Pacing off base_rtt (the empty-queue propagation time) spreads each
    // window across one RTT, smoothing the burst that would otherwise spike the
    // bottleneck queue. The 7/8 factor leaves headroom so pacing never becomes
    // the bottleneck and goodput stays at the link rate.
    pthread_mutex_lock(&(sock->window_lock));
    uint32_t pace_cwnd = sock->cwnd;
    uint32_t pace_base_us = sock->base_rtt_ms;  // stored in microseconds
    pthread_mutex_unlock(&(sock->window_lock));
    if (pace_base_us != UINT32_MAX && pace_cwnd > (uint32_t)MSS) {
      uint64_t gap_us = (uint64_t)pace_base_us * (uint32_t)MSS / pace_cwnd;
      gap_us = gap_us * 7 / 8;        // leave headroom so the link stays full
      if (gap_us > 5000) {
        gap_us = 5000;                // never stall more than 5 ms per packet
      }
      if (gap_us > 0) {
        struct timespec ts;
        ts.tv_sec = (time_t)(gap_us / 1000000ULL);
        ts.tv_nsec = (long)((gap_us % 1000000ULL) * 1000ULL);
        nanosleep(&ts, NULL);
      }
    }
  }

  // Wait for all packets to be acknowledged. Only retransmit on a genuine
  // timeout (no ACK progress during the wait), not every time a packet arrives.
  while (1) {
    pthread_mutex_lock(&(sock->window_lock));
    int all_acked = (sock->packet_buffer_head == NULL);
    pthread_mutex_unlock(&(sock->window_lock));

    if (all_acked) {
      break;
    }

    check_for_data(sock, TIMEOUT);

    pthread_mutex_lock(&(sock->window_lock));
    int still_outstanding = (sock->packet_buffer_head != NULL);
    // Time-based timeout detection (independent of duplicate ACKs).
    if (timer_expired(sock) && still_outstanding) {
      on_timeout(sock);
    }
    pthread_mutex_unlock(&(sock->window_lock));
  }
}

void *begin_backend(void *in) {
  cmu_socket_t *sock = (cmu_socket_t *)in;
  int death, buf_len, send_signal;
  uint8_t *data;

  // Perform handshake if initiator
  if (sock->type == TCP_INITIATOR) {
    // Send SYN
    pthread_mutex_lock(&(sock->window_lock));
    uint16_t src = sock->my_port;
    uint16_t dst = ntohs(sock->conn.sin_port);
    uint32_t seq = sock->window.next_seq_to_send;
    uint32_t ack = 0;
    uint16_t hlen = sizeof(cmu_tcp_header_t);
    uint16_t plen = hlen;
    uint8_t flags = SYN_FLAG_MASK;
    uint16_t adv_window = 1;
    uint16_t ext_len = 0;

    uint8_t *syn_packet = create_packet(src, dst, seq, ack, hlen, plen, flags,
                                        adv_window, ext_len, NULL, NULL, 0);

    socklen_t conn_len = sizeof(sock->conn);
    sendto(sock->socket, syn_packet, plen, 0, (struct sockaddr *)&(sock->conn),
           conn_len);

    // Store SYN for retransmission
    add_packet_to_buffer(sock, syn_packet, plen, seq);
    free(syn_packet);

    sock->window.next_seq_to_send += 1;
    pthread_mutex_unlock(&(sock->window_lock));

    // Wait for SYN-ACK with retransmission
    while (sock->state != ESTABLISHED) {
      check_for_data(sock, TIMEOUT);

      // Retransmit SYN if not established
      if (sock->state != ESTABLISHED) {
        retransmit_packets(sock);
      }
    }
  } else {
    // Listener: wait for handshake to complete
    while (sock->state != ESTABLISHED) {
      check_for_data(sock, TIMEOUT);

      // Retransmit SYN-ACK if in SYN_RECEIVED state
      if (sock->state == SYN_RECEIVED) {
        retransmit_packets(sock);
      }
    }
  }

  // Main loop for data transfer
  while (1) {
    while (pthread_mutex_lock(&(sock->death_lock)) != 0) {
    }
    death = sock->dying;
    pthread_mutex_unlock(&(sock->death_lock));

    while (pthread_mutex_lock(&(sock->send_lock)) != 0) {
    }
    buf_len = sock->sending_len;

    if (death && buf_len == 0) {
      break;
    }

    if (buf_len > 0) {
      data = malloc(buf_len);
      memcpy(data, sock->sending_buf, buf_len);
      sock->sending_len = 0;
      free(sock->sending_buf);
      sock->sending_buf = NULL;
      pthread_mutex_unlock(&(sock->send_lock));
      single_send(sock, data, buf_len);
      free(data);
    } else {
      pthread_mutex_unlock(&(sock->send_lock));
    }

    // When there is outstanding unacknowledged data but the application isn't
    // actively sending, block up to DEFAULT_TIMEOUT waiting for ACKs so that a
    // silent peer reliably triggers the retransmission timer (and thus the
    // return to slow start). Otherwise poll without blocking.
    pthread_mutex_lock(&(sock->window_lock));
    int outstanding = (sock->packet_buffer_head != NULL);
    pthread_mutex_unlock(&(sock->window_lock));
    if (outstanding) {
      check_for_data(sock, TIMEOUT);
    } else {
      // Idle: nothing queued to send and nothing outstanding. Block briefly in
      // poll() so this backend thread yields the CPU instead of busy-spinning
      // on NO_WAIT. A short bound keeps us responsive to the dying flag and to
      // data newly queued by cmu_write() (which do not wake poll directly),
      // while any arriving packet wakes us immediately.
      struct pollfd pfd;
      pfd.fd = sock->socket;
      pfd.events = POLLIN;
      poll(&pfd, 1, 10 /* ms */);
      check_for_data(sock, NO_WAIT);
    }

    // Retransmission timer must run whenever data is outstanding, even when the
    // application isn't actively writing (i.e. we're not inside single_send).
    // This ensures a timeout fires and we return to slow start under loss.
    pthread_mutex_lock(&(sock->window_lock));
    if (timer_expired(sock) && sock->packet_buffer_head != NULL) {
      on_timeout(sock);
    }
    pthread_mutex_unlock(&(sock->window_lock));

    while (pthread_mutex_lock(&(sock->recv_lock)) != 0) {
    }

    send_signal = sock->received_len > 0;

    pthread_mutex_unlock(&(sock->recv_lock));

    if (send_signal) {
      pthread_cond_signal(&(sock->wait_cond));
    }
  }

  pthread_exit(NULL);
  return NULL;
}
