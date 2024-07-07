// SPDX-License-Identifier: BSD-3-Clause

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/sendfile.h>
#include <sys/eventfd.h>
#include <libaio.h>
#include <errno.h>

#include "aws.h"
#include "utils/util.h"
#include "utils/debug.h"
#include "utils/sock_util.h"
#include "utils/w_epoll.h"

/* server socket file descriptor */
static int listenfd;

/* epoll file descriptor */
static int epollfd;

static io_context_t ctx;

static int aws_on_path_cb(http_parser *p, const char *buf, size_t len)
{
	struct connection *conn = (struct connection *)p->data;

	memcpy(conn->request_path, buf, len);
	conn->request_path[len] = '\0';
	conn->have_path = 1;

	return 0;
}

static void connection_prepare_send_reply_header(struct connection *conn)
{
	/**
	 * Prepare the 200 header.
	 */
	sprintf(conn->send_buffer,
				"HTTP/1.1 200 OK\r\n"
				"Content-Length: %ld\r\n"
				"Connection: close\r\n"
				"Content-Type: text/html\r\n\r\n", conn->file_size);

	conn->send_len = strlen(conn->send_buffer);
}

static void connection_prepare_send_404(struct connection *conn)
{
	/**
	 * Prepare the 404 header.
	 */
	sprintf(conn->send_buffer,
				"HTTP/1.1 404 Not Found\r\n"
				"Content-Length: 0\r\n"
				"Connection: close\r\n\r\n");

	conn->send_len = strlen(conn->send_buffer);
}

static enum resource_type connection_get_resource_type(struct connection *conn)
{
	/**
	 * Check if the resource is static or dynamic.
	 */
	if (strstr(conn->request_path, "static") != NULL) {
		conn->res_type = RESOURCE_TYPE_STATIC;
		return RESOURCE_TYPE_STATIC;
	}

	if (strstr(conn->request_path, "dynamic") != NULL) {
		conn->res_type = RESOURCE_TYPE_DYNAMIC;
		return RESOURCE_TYPE_DYNAMIC;
	}
	return RESOURCE_TYPE_NONE;
}


struct connection *connection_create(int sockfd)
{
	/**
	 * Create a new connection.
	 */
	struct connection *conn = malloc(sizeof(struct connection));

	DIE(!conn, "malloc failed\n");

	conn->fd = -1;
	memset(conn->filename, 0, BUFSIZ);

	conn->eventfd = -1;
	conn->sockfd = sockfd;

	io_queue_init(1, &conn->ctx);
	memset(&conn->iocb, 0, sizeof(struct iocb));
	conn->piocb[0] = &conn->iocb;
	conn->file_size = 0;

	memset(conn->recv_buffer, 0, BUFSIZ);
	conn->recv_len = 0;

	memset(conn->send_buffer, 0, BUFSIZ);
	conn->send_len = 0;
	conn->send_pos = 0;
	conn->file_pos = 0;
	conn->async_read_len = 0;

	conn->have_path = 0;
	memset(conn->request_path, 0, BUFSIZ);
	conn->res_type = RESOURCE_TYPE_NONE;
	conn->state = STATE_INITIAL;

	return conn;
}

void connection_start_async_io(struct connection *conn)
{
	/**
	 * Assigns the iocb in the array of iocbs and submits the request.
	 * The eventfd is used to signal the completion of the request.
	 */
	conn->piocb[0] = &conn->iocb;

	io_set_eventfd(conn->piocb[0], conn->eventfd);

	int ret = io_submit(conn->ctx, 1, conn->piocb);

	DIE(ret != 1, "io_submit failed\n");
}

void connection_remove(struct connection *conn)
{
	/**
	 * Remove the connection from the epoll and close the socket.
	 */
	int ret = w_epoll_remove_ptr(epollfd, conn->sockfd, conn);

	DIE(ret < 0, "w_epoll_remove_ptr failed\n");

	ret = close(conn->sockfd);

	DIE(ret < 0, "close failed\n");

	ret = close(conn->fd);

	DIE(ret < 0, "close failed\n");

	free(conn);

	dlog(LOG_DEBUG, "Connection closed\n");
}

void handle_new_connection(void)
{
	socklen_t addrlen = sizeof(struct sockaddr_in);
	struct sockaddr_in addr;

	/**
	 * Accept a new connection and add it to the epoll.
	 */
	int sockfd = accept(listenfd, (SSA *) &addr, &addrlen);

	DIE(sockfd < 0, "accept failed\n");

	/**
	 * Set the socket to non-blocking.
	 */
	fcntl(sockfd, F_SETFL, fcntl(sockfd, F_GETFL) | O_NONBLOCK);

	struct connection *conn = connection_create(sockfd);

	/**
	 * Add the connection to the epoll.
	 */
	int ret = w_epoll_add_ptr_in(epollfd, sockfd, conn);

	DIE(ret < 0, "w_epoll_add_in failed\n");
}

void receive_data(struct connection *conn)
{
	ssize_t bytes_received = 0;

	/**
	 * Receive the data from the socket.
	 */
	do {
		bytes_received = recv(conn->sockfd, conn->recv_buffer + conn->recv_len, BUFSIZ, 0);

		if (!(bytes_received == -1 && conn->recv_len > 0))
			conn->recv_len += bytes_received;

		dlog(LOG_DEBUG, "Read %lu bytes\n", bytes_received);
	} while (bytes_received > 0);

	/**
	 * Check if the connection was closed or there was an error.
	 */
	if (conn->recv_len < 0) {
		dlog(LOG_DEBUG, "Error in communication\n");
		connection_remove(conn);
		return;
	}

	/**
	 * Check if the connection was closed.
	 */
	if (conn->recv_len == 0) {
		dlog(LOG_DEBUG, "Connection closed\n");
		connection_remove(conn);
		return;
	}

	dlog(LOG_DEBUG, "Received %s with len %lu\n", conn->recv_buffer, conn->recv_len);
}

int connection_open_file(struct connection *conn)
{
	/**
	 * Open the file.
	 */
	conn->fd = open(conn->filename, O_RDONLY);

	if (conn->fd == -1) {
		dlog(LOG_DEBUG, "Failed to open file %s\n", conn->filename);
		connection_prepare_send_404(conn);
		return -1;
	}

	dlog(LOG_DEBUG, "File opened successfully: %s\n", conn->filename);

	struct stat st;
	int ret = fstat(conn->fd, &st);

	DIE(ret < 0, "fstat failed\n");

	conn->file_size = st.st_size;
	connection_prepare_send_reply_header(conn);

	dlog(LOG_DEBUG, "Read from path %s the message %s\n", conn->request_path,
		conn->send_buffer);

	return 0;
}

void connection_complete_async_io(struct connection *conn)
{
	conn->eventfd = eventfd(0, EFD_NONBLOCK);
	DIE(conn->eventfd < 0, "eventfd failed\n");

	int ret = w_epoll_add_ptr_in(epollfd, conn->eventfd, conn);

	DIE(ret < 0, "w_epoll_add_ptr_in failed\n");

	struct io_event *event = malloc(sizeof(struct io_event));

	dlog(LOG_DEBUG, "File size %ld\n", conn->file_size);
	while (conn->async_read_len < conn->file_size) {
		/**
		 * Read from the file and write to the socket.
		 */
		io_prep_pread(&conn->iocb, conn->fd, conn->send_buffer, BUFSIZ, conn->async_read_len);

		connection_start_async_io(conn);

		ret = io_getevents(conn->ctx, 1, 1, event, NULL);
		DIE(ret < 0, "io_getevents failed\n");

		io_prep_pwrite(&conn->iocb, conn->sockfd, conn->send_buffer, event->res, 0);

		connection_start_async_io(conn);

		ret = io_getevents(conn->ctx, 1, 1, event, NULL);
		DIE(ret < 0, "io_getevents failed\n");

		conn->async_read_len += event->res;
	}
	dlog(LOG_DEBUG, "Total transfered %ld\n", conn->async_read_len);

	ret = w_epoll_remove_ptr(epollfd, conn->eventfd, conn);
	DIE(ret < 0, "w_epoll_add_ptr_in failed\n");

	free(event);
	close(conn->eventfd);
}

int parse_header(struct connection *conn)
{
	http_parser_settings settings_on_path = {
		.on_message_begin = 0,
		.on_header_field = 0,
		.on_header_value = 0,
		.on_path = aws_on_path_cb,
		.on_url = 0,
		.on_fragment = 0,
		.on_query_string = 0,
		.on_body = 0,
		.on_headers_complete = 0,
		.on_message_complete = 0
	};

	/**
	 * Parse the header and get the path.
	 */
	http_parser_init(&conn->request_parser, HTTP_REQUEST);
	conn->request_parser.data = conn;

	size_t parsed_bytes = http_parser_execute(&conn->request_parser, &settings_on_path, conn->recv_buffer,
											  conn->recv_len);

	if (conn->have_path == 0) {
		dlog(LOG_DEBUG, "No header\n");
		return -1;
	}

	dlog(LOG_DEBUG, "Parsed HTTP request %lu bytes:\n%s\n", parsed_bytes, conn->request_path);

	sprintf(conn->filename, "%s%s", AWS_DOCUMENT_ROOT, conn->request_path + 1);

	return 0;
}

enum connection_state connection_send_static(struct connection *conn)
{
	ssize_t bytes_sendfile, total_sent = 0;

	/**
	 * Send the file to the socket.
	 */
	do {
		bytes_sendfile = sendfile(conn->sockfd, conn->fd, NULL, BUFSIZ);
		DIE(bytes_sendfile < 0, "sendfile failed\n");

		dlog(LOG_DEBUG, "Sendfile sent %lu bytes\n", bytes_sendfile);

		total_sent += bytes_sendfile;
	} while (bytes_sendfile > 0);


	dlog(LOG_DEBUG, "Total sendfile %ld\n", total_sent);

	return 0;
}

int connection_send_data(struct connection *conn)
{
	ssize_t bytes_send = 0;

	/**
	 * Send the header to the socket.
	 */
	do {
		bytes_send = send(conn->sockfd, conn->send_buffer + conn->send_pos, conn->send_len - conn->send_pos, 0);

		if (!(bytes_send == -1 && conn->send_pos > 0))
			conn->send_pos += bytes_send;

		dlog(LOG_DEBUG, "Sent %lu bytes\n", bytes_send);
	} while (bytes_send > 0);

	/**
	 * Check if the connection was closed or there was an error.
	 */
	if (conn->send_pos < 0) {
		dlog(LOG_DEBUG, "Error in communication\n");
		connection_remove(conn);
		return -1;
	}

	if (conn->send_pos == 0) {
		dlog(LOG_DEBUG, "Connection closed\n");
		connection_remove(conn);
		return -1;
	}

	dlog(LOG_DEBUG, "Total sent %lu bytes out of %lu\n", conn->send_pos, conn->send_len);

	return 0;
}


void handle_input(struct connection *conn)
{
	/**
	 * The state machine handles the input based on the connection state.
	 */
	while (1)
		switch (conn->state) {
		case STATE_INITIAL:
			dlog(LOG_DEBUG, "Receiving data\n");
			receive_data(conn);
			conn->state = STATE_RECEIVING_DATA;
			break;

		case STATE_RECEIVING_DATA:
			if (parse_header(conn) == -1) {
				dlog(LOG_DEBUG, "Invalid path for the header\n");
				conn->state = STATE_SENDING_404;
				break;
			}

			conn->state = STATE_REQUEST_RECEIVED;

			break;

		case STATE_REQUEST_RECEIVED:
			if (connection_open_file(conn) == 0) {
				conn->state = STATE_SENDING_HEADER;
				break;
			}

			conn->state = STATE_SENDING_404;

			break;

		default:
			return;
		}
}

void handle_output(struct connection *conn)
{
	/**
	 * The state machine handles the output based on the connection state.
	 */
	while (1)
		switch (conn->state) {
		case STATE_SENDING_404:
			connection_send_data(conn);
			conn->state = STATE_404_SENT;
			break;

		case STATE_SENDING_HEADER:
			if (connection_get_resource_type(conn) == RESOURCE_TYPE_NONE) {
				conn->state = STATE_SENDING_404;
				break;
			}

			connection_send_data(conn);
			conn->state = STATE_HEADER_SENT;

			if (connection_get_resource_type(conn) == RESOURCE_TYPE_STATIC) {
				conn->state = STATE_SENDING_DATA;
				break;
			}

			conn->state = STATE_ASYNC_ONGOING;

			break;

		case STATE_SENDING_DATA:
			connection_send_static(conn);

			conn->state = STATE_DATA_SENT;
			break;

		case STATE_ASYNC_ONGOING:
			connection_complete_async_io(conn);

			conn->state = STATE_DATA_SENT;
			break;

		case STATE_404_SENT:
		case STATE_HEADER_SENT:
		case STATE_DATA_SENT:
			connection_remove(conn);
			conn->state = STATE_CONNECTION_CLOSED;
			break;

		case STATE_CONNECTION_CLOSED:
			conn->state = STATE_NO_STATE;

		default:
			return;
		}
}

void handle_client(uint32_t event, struct connection *conn)
{
	/**
	 * Handle the client based on the input/output event.
	 */
	if (event & EPOLLIN) {
		dlog(LOG_DEBUG, "New message\n");
		handle_input(conn);

		int ret = w_epoll_update_ptr_out(epollfd, conn->sockfd, conn);

		DIE(ret < 0, "w_epoll_update_ptr_out failed\n");

	} else if (event & EPOLLOUT) {
		dlog(LOG_DEBUG, "Ready to send message\n");
		handle_output(conn);
	}
}

int main(void)
{
	int ret = io_setup(1, &ctx);

	DIE(ret < 0, "io_setup failed\n");

	/**
	 * Initialize multiplexing and create the server socket.
	 */
	epollfd = w_epoll_create();
	DIE(epollfd < 0, "create epoll failed\n");

	listenfd = tcp_create_listener(AWS_LISTEN_PORT, DEFAULT_LISTEN_BACKLOG);
	DIE(listenfd < 0, "tcp_create_listener failed\n");

	/**
	 * Add the server socket to the epoll.
	 */
	ret = w_epoll_add_fd_in(epollfd, listenfd);

	DIE(ret < 0, "w_epoll_add_fd_in failed\n");

	dlog(LOG_DEBUG, "Server waiting for connections on port %d\n", AWS_LISTEN_PORT);

	while (1) {
		struct epoll_event rev;

		/**
		 * Wait for an event on the epoll.
		 */
		ret = w_epoll_wait_infinite(epollfd, &rev);
		DIE(ret < 0, "w_epoll_wait_infinite failed\n");

		dlog(LOG_DEBUG, "Found something on fd %u\n", rev.data.fd);

		if (rev.data.fd == listenfd) {
			dlog(LOG_DEBUG, "New connection\n");
			if (rev.events & EPOLLIN)
				handle_new_connection();

		} else {
			handle_client(rev.events, rev.data.ptr);
		}
	}

	io_destroy(ctx);

	return 0;
}
