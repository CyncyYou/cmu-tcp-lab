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
 * This file implements the high-level API for CMU-TCP sockets.
 */

#include "cmu_tcp.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include "backend.h"

int cmu_socket(cmu_socket_t *sock, const cmu_socket_type_t socket_type,
               const int port, const char *server_ip) {
  int sockfd, optval;
  socklen_t len;
  struct sockaddr_in conn, my_addr;
  len = sizeof(my_addr);

  sockfd = socket(AF_INET, SOCK_DGRAM, 0);
  if (sockfd < 0) {
    perror("ERROR opening socket");
    return EXIT_ERROR;
  }
  sock->socket = sockfd;
  sock->received_buf = NULL;
  sock->received_len = 0;
  sock->ofo_head = NULL;
  sock->ofo_bytes = 0;
  pthread_mutex_init(&(sock->recv_lock), NULL);

  sock->sending_buf = NULL;
  sock->sending_len = 0;
  pthread_mutex_init(&(sock->send_lock), NULL);

  sock->type = socket_type;
  sock->dying = 0;
  pthread_mutex_init(&(sock->death_lock), NULL);

  // window_lock is acquired re-entrantly: handle_message() and single_send()
  // hold it while calling helpers (add_packet_to_buffer, remove_acked_packets,
  // retransmit_packets, has_been_acked) that also acquire it. Use a RECURSIVE
  // mutex so the same thread can lock it multiple times without deadlocking.
  pthread_mutexattr_t window_lock_attr;
  pthread_mutexattr_init(&window_lock_attr);
  pthread_mutexattr_settype(&window_lock_attr, PTHREAD_MUTEX_RECURSIVE);
  pthread_mutex_init(&(sock->window_lock), &window_lock_attr);
  pthread_mutexattr_destroy(&window_lock_attr);

  // Initialize packet buffer
  sock->packet_buffer_head = NULL;

  // Generate random ISN
  srand(time(NULL) ^ getpid());
  uint32_t isn = rand();

  sock->window.last_ack_received = isn;
  sock->window.next_seq_to_send = isn;
  sock->window.next_seq_expected = 0;
  // Start by assuming the peer's receive buffer is empty (full capacity), so
  // the sender can transmit immediately. The first real ACK refines this.
  sock->window.adv_window_from_receiver = MAX_NETWORK_BUFFER;

  // Congestion control initial state (TCP Reno): begin in slow start with
  // cwnd = WINDOW_INITIAL_WINDOW_SIZE and ssthresh = WINDOW_INITIAL_SSTHRESH.
  sock->cwnd = WINDOW_INITIAL_WINDOW_SIZE;
  sock->ssthresh = WINDOW_INITIAL_SSTHRESH;
  sock->dup_ack_count = 0;
  sock->cc_state = CC_SLOW_START;
  sock->timer_running = 0;
  sock->timer_start_ms = 0;

  // TCP Vegas (delay-based) initial state. base_rtt starts at "unknown"
  // (UINT32_MAX) so the first sample becomes the minimum. No RTT sample is
  // outstanding yet, and the first round boundary is the initial send seq.
  sock->base_rtt_ms = UINT32_MAX;
  sock->rtt_sample_ms = 0;
  sock->rtt_pending = 0;
  sock->rtt_seq = 0;
  sock->rtt_send_ms = 0;
  sock->round_end_seq = isn;
  sock->vegas_inited = 0;
  sock->base_rtt_stamp_us = 0;
  sock->win_min_us = UINT32_MAX;
  sock->win_start_us = 0;
  sock->overload_count = 0;

  if (pthread_cond_init(&sock->wait_cond, NULL) != 0) {
    perror("ERROR condition variable not set\n");
    return EXIT_ERROR;
  }

  switch (socket_type) {
    case TCP_INITIATOR:
      if (server_ip == NULL) {
        perror("ERROR server_ip NULL");
        return EXIT_ERROR;
      }
      memset(&conn, 0, sizeof(conn));
      conn.sin_family = AF_INET;
      conn.sin_addr.s_addr = inet_addr(server_ip);
      conn.sin_port = htons(port);
      sock->conn = conn;

      my_addr.sin_family = AF_INET;
      my_addr.sin_addr.s_addr = htonl(INADDR_ANY);
      my_addr.sin_port = 0;
      if (bind(sockfd, (struct sockaddr *)&my_addr, sizeof(my_addr)) < 0) {
        perror("ERROR on binding");
        return EXIT_ERROR;
      }

      sock->state = CLOSED;
      break;

    case TCP_LISTENER:
      memset(&conn, 0, sizeof(conn));
      conn.sin_family = AF_INET;
      conn.sin_addr.s_addr = htonl(INADDR_ANY);
      conn.sin_port = htons((uint16_t)port);

      optval = 1;
      setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, (const void *)&optval,
                 sizeof(int));
      if (bind(sockfd, (struct sockaddr *)&conn, sizeof(conn)) < 0) {
        perror("ERROR on binding");
        return EXIT_ERROR;
      }
      sock->conn = conn;
      sock->state = LISTEN;
      break;

    default:
      perror("Unknown Flag");
      return EXIT_ERROR;
  }
  getsockname(sockfd, (struct sockaddr *)&my_addr, &len);
  sock->my_port = ntohs(my_addr.sin_port);

  pthread_create(&(sock->thread_id), NULL, begin_backend, (void *)sock);
  return EXIT_SUCCESS;
}

int cmu_close(cmu_socket_t *sock) {
  while (pthread_mutex_lock(&(sock->death_lock)) != 0) {
  }
  sock->dying = 1;
  pthread_mutex_unlock(&(sock->death_lock));

  pthread_join(sock->thread_id, NULL);

  if (sock != NULL) {
    if (sock->received_buf != NULL) {
      free(sock->received_buf);
    }
    if (sock->sending_buf != NULL) {
      free(sock->sending_buf);
    }
    // Free packet buffer
    packet_buf_t *current = sock->packet_buffer_head;
    while (current != NULL) {
      packet_buf_t *next = current->next;
      free(current->packet);
      free(current);
      current = next;
    }
    // Free out-of-order buffer
    ofo_buf_t *o = sock->ofo_head;
    while (o != NULL) {
      ofo_buf_t *onext = o->next;
      free(o->payload);
      free(o);
      o = onext;
    }
  } else {
    perror("ERROR null socket\n");
    return EXIT_ERROR;
  }
  return close(sock->socket);
}

int cmu_read(cmu_socket_t *sock, void *buf, int length, cmu_read_mode_t flags) {
  uint8_t *new_buf;
  int read_len = 0;

  if (length < 0) {
    perror("ERROR negative length");
    return EXIT_ERROR;
  }

  while (pthread_mutex_lock(&(sock->recv_lock)) != 0) {
  }

  switch (flags) {
    case NO_FLAG:
      while (sock->received_len == 0) {
        pthread_cond_wait(&(sock->wait_cond), &(sock->recv_lock));
      }
    // Fall through.
    case NO_WAIT:
      if (sock->received_len > 0) {
        if (sock->received_len > length)
          read_len = length;
        else
          read_len = sock->received_len;

        memcpy(buf, sock->received_buf, read_len);
        if (read_len < sock->received_len) {
          new_buf = malloc(sock->received_len - read_len);
          memcpy(new_buf, sock->received_buf + read_len,
                 sock->received_len - read_len);
          free(sock->received_buf);
          sock->received_len -= read_len;
          sock->received_buf = new_buf;
        } else {
          free(sock->received_buf);
          sock->received_buf = NULL;
          sock->received_len = 0;
        }
      }
      break;
    default:
      perror("ERROR Unknown flag.\n");
      read_len = EXIT_ERROR;
  }

  // Zero-window recovery: the application just consumed data, which frees
  // receive-buffer space and grows our advertised window. The peer only learns
  // about a reopened window from an ACK we send, and it only sends us data
  // (which would trigger an ACK) when its send window is open. If our window
  // had collapsed, the sender is now parked and no data is arriving, so we must
  // proactively send an unsolicited window-update ACK to wake it up.
  if (read_len > 0) {
    int free_space = MAX_NETWORK_BUFFER - sock->received_len;
    if (free_space < 0) free_space = 0;
    if (free_space > MAX_NETWORK_BUFFER) free_space = MAX_NETWORK_BUFFER;

    pthread_mutex_lock(&(sock->window_lock));
    uint16_t src = sock->my_port;
    uint16_t dst = ntohs(sock->conn.sin_port);
    uint32_t seq = sock->window.next_seq_to_send;
    uint32_t ack = sock->window.next_seq_expected;
    pthread_mutex_unlock(&(sock->window_lock));

    uint16_t hlen = sizeof(cmu_tcp_header_t);
    uint16_t plen = hlen;
    uint8_t *upd = create_packet(src, dst, seq, ack, hlen, plen, ACK_FLAG_MASK,
                                 (uint16_t)free_space, 0, NULL, NULL, 0);
    sendto(sock->socket, upd, plen, 0, (struct sockaddr *)&(sock->conn),
           sizeof(sock->conn));
    free(upd);
  }

  pthread_mutex_unlock(&(sock->recv_lock));
  return read_len;
}

int cmu_write(cmu_socket_t *sock, const void *buf, int length) {
  while (pthread_mutex_lock(&(sock->send_lock)) != 0) {
  }
  if (sock->sending_buf == NULL)
    sock->sending_buf = malloc(length);
  else
    sock->sending_buf = realloc(sock->sending_buf, length + sock->sending_len);
  memcpy(sock->sending_buf + sock->sending_len, buf, length);
  sock->sending_len += length;

  pthread_mutex_unlock(&(sock->send_lock));
  return EXIT_SUCCESS;
}
