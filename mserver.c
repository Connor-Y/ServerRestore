// The metadata server implementation

#include <assert.h>
#include <errno.h>
#include <limits.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>

#include "defs.h"
#include "hash.h"
#include "util.h"


// Program arguments

// Ports for listening to incoming connections from clients and servers
static uint16_t clients_port = 0;
static uint16_t servers_port = 0;

// Server configuration file name
static char cfg_file_name[PATH_MAX] = "";

// Timeout for detecting server failures; you might want to adjust this default value
static const int default_server_timeout = 3;
static int server_timeout = 0;

// Log file name
static char log_file_name[PATH_MAX] = "";


// Tmp checker TODO: remove after
char command[50];

static void usage(char **argv)
{
	printf("usage: %s -c <client port> -s <servers port> -C <config file> "
	       "[-t <timeout (seconds)> -l <log file>]\n", argv[0]);
	printf("Default timeout is %d seconds\n", default_server_timeout);
	printf("If the log file (-l) is not specified, log output is written to stdout\n");
}

// Returns false if the arguments are invalid
static bool parse_args(int argc, char **argv)
{
	char option;
	while ((option = getopt(argc, argv, "c:s:C:l:t:")) != -1) {
		switch(option) {
			case 'c': clients_port = atoi(optarg); break;
			case 's': servers_port = atoi(optarg); break;
			case 'l': strncpy(log_file_name, optarg, PATH_MAX); break;
			case 'C': strncpy(cfg_file_name, optarg, PATH_MAX); break;
			case 't': server_timeout = atoi(optarg); break;
			default:
				fprintf(stderr, "Invalid option: -%c\n", option);
				return false;
		}
	}

	server_timeout = (server_timeout != 0) ? server_timeout : default_server_timeout;

	return (clients_port != 0) && (servers_port != 0) && (cfg_file_name[0] != '\0');
}


// Current machine host name
static char mserver_host_name[HOST_NAME_MAX] = "";

// Sockets for incoming connections from clients and servers
static int clients_fd = -1;
static int servers_fd = -1;

// Store socket fds for all connected clients, up to MAX_CLIENT_SESSIONS
#define MAX_CLIENT_SESSIONS 1000
static int client_fd_table[MAX_CLIENT_SESSIONS];


// Structure describing a key-value server state
typedef struct _server_node {
	// Server host name, possibly prefixed by "user@" (for starting servers remotely via ssh)
	char host_name[HOST_NAME_MAX];
	// Servers/client/mserver port numbers
	uint16_t sport;
	uint16_t cport;
	uint16_t mport;
	// Server ID
	int sid;
	// Socket for receiving requests from the server
	int socket_fd_in;
	// Socket for sending requests to the server
	int socket_fd_out;
	// Server process PID (it is a child process of mserver)
	pid_t pid;
	
	// Tells if the server is alive
	int is_alive;
	// Time of last heartbeat received
	struct timeval last_heartbeat;
	
	// Flags for restoring crashed serverse
	bool halt_client_req;
	bool updated_primary;
	bool updated_secondary;
	
} server_node;

// Total number of servers
static int num_servers = 0;
// Server state information
static server_node *server_nodes = NULL;


// Hash table to store the keys in range for each server
//static hash_table server_hash_table;

// Read the configuration file, fill in the server_nodes array
// Returns false if the configuration is invalid
static bool read_config_file()
{
	FILE *cfg_file = fopen(cfg_file_name, "r");
	if (cfg_file == NULL) {
		perror(cfg_file_name);
		return false;
	}
	bool result = false;

	// The first line contains the number of servers
	if (fscanf(cfg_file, "%d\n", &num_servers) < 1) {
		goto end;
	}

	// Need at least 3 servers to avoid cross-replication
	if (num_servers < 3) {
		fprintf(stderr, "Invalid number of servers: %d\n", num_servers);
		goto end;
	}

	if ((server_nodes = calloc(num_servers, sizeof(server_node))) == NULL) {
		perror("calloc");
		goto end;
	}

	for (int i = 0; i < num_servers; i++) {
		server_node *node = &(server_nodes[i]);

		// Format: <host_name> <clients port> <servers port> <mservers_port>
		if ((fscanf(cfg_file, "%s %hu %hu %hu\n", node->host_name,
		            &(node->cport), &(node->sport), &(node->mport)) < 4) ||
		    ((strcmp(node->host_name, "localhost") != 0) && (strchr(node->host_name, '@') == NULL)) ||
		    (node->cport == 0) || (node->sport == 0) || (node->mport == 0))
		{
			free(server_nodes);
			server_nodes = NULL;
			goto end;
		}

		node->sid = i;
		node->socket_fd_in = -1;
		node->socket_fd_out = -1;
		node->pid = 0;
		
		// TODO: Set new server_node attributes
		node->is_alive = 1; 
		gettimeofday(&node->last_heartbeat, NULL);
		node->halt_client_req = false;
		node->updated_primary = false;
		node->updated_secondary = false;
	}

	// Print server configuration
	fprintf(stdout, "Key-value servers configuration:\n");
	for (int i = 0; i < num_servers; i++) {
		server_node *node = &(server_nodes[i]);
		fprintf(stdout, "\thost: %s, client port: %d, server port: %d\n", node->host_name, node->cport, node->sport);
	}
	result = true;

end:
	fclose(cfg_file);
	return result;
}


static void cleanup();
static bool init_servers();

// Initialize and start the metadata server
static bool init_mserver()
{
	for (int i = 0; i < MAX_CLIENT_SESSIONS; i++) {
		client_fd_table[i] = -1;
	}

	// Get the host name that server is running on
	if (get_local_host_name(mserver_host_name, sizeof(mserver_host_name)) < 0) {
		return false;
	}
	log_write("%s Metadata server starts on host: %s\n", current_time_str(), mserver_host_name);

	// Create sockets for incoming connections from servers
	if ((servers_fd = create_server(servers_port, num_servers + 1, NULL)) < 0) {
		goto cleanup;
	}

	// Start key-value servers
	if (!init_servers()) {
		goto cleanup;
	}
	
	// Create sockets for incoming connections from clients
	if ((clients_fd = create_server(clients_port, MAX_CLIENT_SESSIONS, NULL)) < 0) {
		goto cleanup;
	}

	log_write("Metadata server initialized\n");
	return true;

cleanup:
	cleanup();
	return false;
}

// Cleanup and release all the resources
static void cleanup()
{
	fprintf(stdout, "Cleanup started\n");
	close_safe(&clients_fd);
	close_safe(&servers_fd);

	// Close all client connections
	for (int i = 0; i < MAX_CLIENT_SESSIONS; i++) {
		close_safe(&(client_fd_table[i]));
	}

	if (server_nodes != NULL) {
		for (int i = 0; i < num_servers; i++) {
			printf("cleanup - 2.%d\n", i);
			server_node *node = &(server_nodes[i]);

			if (node->socket_fd_out != -1) {
				// Request server shutdown
				printf("cleanup - req shutdown\n");
				server_ctrl_request request = {0};
				request.hdr.type = MSG_SERVER_CTRL_REQ;
				request.type = SHUTDOWN;
				send_msg(node->socket_fd_out, &request, sizeof(request));
			}

			// Close the connections
			close_safe(&(server_nodes[i].socket_fd_out));
			close_safe(&(server_nodes[i].socket_fd_in));

			// Wait with timeout (or kill if timeout expires) for the server process
			if (server_nodes[i].pid > 0) {
				kill_safe(&(server_nodes[i].pid), 5);
			}
			
		}
		
		free(server_nodes);
		server_nodes = NULL;
		
		fprintf(stdout, "Cleanup Completed\n");
	}
}


static const int max_cmd_length = 32;

static const char *remote_path = "csc469_a3/";

// Generate a command to start a key-value server (see server.c for arguments description)
static char **get_spawn_cmd(int sid)
{
	char **cmd = calloc(max_cmd_length, sizeof(char*));
	assert(cmd != NULL);

	server_node *node = &(server_nodes[sid]);
	int i = -1;

	if (strcmp(node->host_name, "localhost") != 0) {
		// Remote server, host_name format is "user@host"
		assert(strchr(node->host_name, '@') != NULL);

		// Use ssh to run the command on a remote machine
		cmd[++i] = strdup("ssh");
		cmd[++i] = strdup(node->host_name);
		cmd[++i] = strdup("cd");
		cmd[++i] = strdup(remote_path);
		cmd[++i] = strdup("&&");
	}

	cmd[++i] = strdup("./server\0");

	cmd[++i] = strdup("-h");
	cmd[++i] = strdup(mserver_host_name);

	cmd[++i] = strdup("-m");
	cmd[++i] = malloc(8); sprintf(cmd[i], "%hu", servers_port);

	cmd[++i] = strdup("-c");
	cmd[++i] = malloc(8); sprintf(cmd[i], "%hu", node->cport);

	cmd[++i] = strdup("-s");
	cmd[++i] = malloc(8); sprintf(cmd[i], "%hu", node->sport);

	cmd[++i] = strdup("-M");
	cmd[++i] = malloc(8); sprintf(cmd[i], "%hu", node->mport);

	cmd[++i] = strdup("-S");
	cmd[++i] = malloc(8); sprintf(cmd[i], "%d", sid);

	cmd[++i] = strdup("-n");
	cmd[++i] = malloc(8); sprintf(cmd[i], "%d", num_servers);

	cmd[++i] = strdup("-l");
	cmd[++i] = malloc(20); sprintf(cmd[i], "server_%d.log", sid);

	cmd[++i] = NULL;
	assert(i < max_cmd_length);
	return cmd;
}

static void free_cmd(char **cmd)
{
	assert(cmd != NULL);

	for (int i = 0; i < max_cmd_length; i++) {
		if (cmd[i] != NULL) {
			free(cmd[i]);
		}
	}
	free(cmd);
}

// Start a key-value server with given id
static int spawn_server(int sid)
{
	server_node *node = &(server_nodes[sid]);

	close_safe(&(node->socket_fd_in));
	close_safe(&(node->socket_fd_out));
	kill_safe(&(node->pid), 0);

	// Spawn the server as a process on either the local machine or a remote machine (using ssh)
	pid_t pid = fork();
	switch (pid) {
		case -1:
			perror("fork");
			return -1;
		case 0: {
			char **cmd = get_spawn_cmd(sid);
			execvp(cmd[0], cmd);
			// If exec returns, some error happened
			perror(cmd[0]);
			free_cmd(cmd);
			exit(1);
		}
		default:
			node->pid = pid;
			break;
	}

	// Wait for the server to connect
	int fd_idx = accept_connection(servers_fd, &(node->socket_fd_in), 1);
	if (fd_idx < 0) {
		// Something went wrong, kill the server process
		kill_safe(&(node->pid), 1);
		return -1;
	}
	assert(fd_idx == 0);

	// Extract the host name from "user@host"
	char *at = strchr(node->host_name, '@');
	char *host = (at == NULL) ? node->host_name : (at + 1);

	// Connect to the server
	if ((node->socket_fd_out = connect_to_server(host, node->mport)) < 0) {
		// Something went wrong, kill the server process
		close_safe(&(node->socket_fd_in));
		kill_safe(&(node->pid), 1);
		return -1;
	}
	return 0;
}

/* Send the initial SET-SECONDARY message to a newly created server; 
 * returns true on success */
static bool send_set_secondary(int sid)
{
	char buffer[MAX_MSG_LEN] = {0};
	server_ctrl_request *request = (server_ctrl_request*)buffer;

	// Fill in the request parameters
	request->hdr.type = MSG_SERVER_CTRL_REQ;
	request->type = SET_SECONDARY;
	server_node *secondary_node = &(server_nodes[secondary_server_id(sid, num_servers)]);
	request->port = secondary_node->sport;


	log_write("===Set server %d's secondary to be server %d===\n", sid, secondary_node->sid);

	// Extract the host name from "user@host"
	char *at = strchr(secondary_node->host_name, '@');
	char *host = (at == NULL) ? secondary_node->host_name : (at + 1);

	int host_name_len = strlen(host) + 1;
	strncpy(request->host_name, host, host_name_len);

	// Send the request and receive the response
	server_ctrl_response response = {0};
	if (!send_msg(server_nodes[sid].socket_fd_out, request, sizeof(*request) + host_name_len))
	{
		fprintf(stderr, "1 Failed to send set_secondary response to %d\n", sid);
		return false;
	}
	
	if (!recv_msg(server_nodes[sid].socket_fd_out, &response, sizeof(response), MSG_SERVER_CTRL_RESP))
	{
		fprintf(stderr, "2 Failed to receive set_secondary response from %d\n", sid);
		return false;
	}

	if (response.status != CTRLREQ_SUCCESS) {
		fprintf(stderr, "Server %d failed SET-SECONDARY\n", sid);
		return false;
	}
	return true;
}

// Send a UPDATE-PRIMARY message to the given secondary server (e.g Sb in the handout)
// Used during crash recovery 
static bool send_update_primary(int sid)
{
	fprintf(stdout, "-- Primary Server %d--\n", primary_server_id(sid, num_servers));
	
	char buffer[MAX_MSG_LEN] = {0};
	server_ctrl_request *request = (server_ctrl_request*)buffer;

	// Fill in the request parameters
	request->hdr.type = MSG_SERVER_CTRL_REQ;
	request->type = UPDATE_PRIMARY;
	server_node *secondary_node = &(server_nodes[primary_server_id(sid, num_servers)]);
	request->port = secondary_node->sport;
	
	// Extract the host name from "user@host"
	char *at = strchr(secondary_node->host_name, '@');
	char *host = (at == NULL) ? secondary_node->host_name : (at + 1);

	int host_name_len = strlen(host) + 1;
	strncpy(request->host_name, host, host_name_len);

	// Send the request and receive the response
	server_ctrl_response response = {0};
	if (!send_msg(server_nodes[sid].socket_fd_out, request, sizeof(*request) + host_name_len) ||
	    !recv_msg(server_nodes[sid].socket_fd_out, &response, sizeof(response), MSG_SERVER_CTRL_RESP))
	{
		return false;
	}

	if (response.status != CTRLREQ_SUCCESS) {
		fprintf(stderr, "Server %d failed UPDATE-PRIMARY\n", sid);
		return false;
	}
	
	printf("send_update_primary - complete\n");
	return true;
}

// Send a UPDATE-SECONDARY message to given secondary server's primary (e.g Sc in the handout, assuming Sa is/was the primary)
// Used during crash recovery 
static bool send_update_secondary(int sid)
{
	fprintf(stdout, "--- Sending Update Secondary to %d ---\n", sid);
	char buffer[MAX_MSG_LEN] = {0};
	server_ctrl_request *request = (server_ctrl_request*)buffer;

	// Fill in the request parameters
	request->hdr.type = MSG_SERVER_CTRL_REQ;
	request->type = UPDATE_SECONDARY;
	server_node *secondary_node = &(server_nodes[secondary_server_id(sid, num_servers)]);
	request->port = secondary_node->sport;
	fprintf(stdout, "send_update_secondary - Secondary Server %d\n", secondary_node->sid);
	// Extract the host name from "user@host"
	char *at = strchr(secondary_node->host_name, '@');
	char *host = (at == NULL) ? secondary_node->host_name : (at + 1);

	int host_name_len = strlen(host) + 1;
	strncpy(request->host_name, host, host_name_len);

	// Send the request and receive the response
	server_ctrl_response response = {0};
	if (!send_msg(server_nodes[sid].socket_fd_out, request, sizeof(*request) + host_name_len))
	{
		fprintf(stderr, "Failed to send update secondary to %d\n", sid);
		return false;
	}

	if (!recv_msg(server_nodes[sid].socket_fd_out, &response, sizeof(response), MSG_SERVER_CTRL_RESP))
	{
		fprintf(stderr, "Failed to receive update secondary from %d\n", sid);
		return false;
	}
	
	
	if (response.status != CTRLREQ_SUCCESS) {
		fprintf(stderr, "Server %d failed UPDATE-SECONDARY\n", sid);
		return false;
	}
	return true;
}

// Send a SWITCH PRIMARY message
// Used during the recovery process
static bool send_switch_primary(int sid)
{
	fprintf(stdout, "Switch primary sid %d\n", sid);

	
	char buffer[MAX_MSG_LEN] = {0};
	server_ctrl_request *request = (server_ctrl_request*)buffer;

	// Fill in the request parameters
	request->hdr.type = MSG_SERVER_CTRL_REQ;
	request->type = SWITCH_PRIMARY;
	// Don't need the host_name field 

	// Send the request and receive the response
	server_ctrl_response response = {0};
	if (!send_msg(server_nodes[sid].socket_fd_out, request, sizeof(*request)) ||
	    !recv_msg(server_nodes[sid].socket_fd_out, &response, sizeof(response), MSG_SERVER_CTRL_RESP))
	{
		return false;
	}

	if (response.status != CTRLREQ_SUCCESS) {
		fprintf(stderr, "Server %d failed SWITCH-PRIMARY\n", sid);
		return false;
	}
	return true;
}

// Start all key-value servers
static bool init_servers()
{
	printf("init_servers - start\n");
	printf("init_servers - spawn servers\n");
	// Spawn all the servers
	for (int i = 0; i < num_servers; i++) {
		if (spawn_server(i) < 0) {
			return false;
		}
	}

	printf("init_servers - set secondaries\n");
	// Let each server know the location of its secondary replica
	for (int i = 0; i < num_servers; i++) {
		if (!send_set_secondary(i)) {
			return false;
		}
	}

	printf("init_servers - complete\n");
	return true;
}

// Connection will be closed after calling this function regardless of result
static void process_client_message(int fd)
{
	log_write("%s mserver receiving a client message\n", current_time_str());

	// Read and parse the message
	locate_request request = {0};
	if (!recv_msg(fd, &request, sizeof(request), MSG_LOCATE_REQ)) {
		return;
	}

	// Determine which server is responsible for the requested key
	int server_id = key_server_id(request.key, num_servers);

	// If halt_client_req is set to true for server_id, then ignore the client 
	// request and let it timeout (https://piazza.com/class/isb6pxw9i2n73q?cid=181)
	if (server_nodes[server_id].halt_client_req) {
		return;
	}

	// redirect client requests to the secondary replica while the primary is being recovered
	if (server_nodes[server_id].is_alive == 0) {
		// The primary is dead/being recovered, so redirect client requests to the secondary replica
		fprintf(stdout, "process_client_message - redirecting client requests to secondary replica\n");
		server_id = secondary_server_id(server_id, num_servers);
	}

	// Fill in the response with the key-value server location information
	char buffer[MAX_MSG_LEN] = {0};
	locate_response *response = (locate_response *)buffer;
	response->hdr.type = MSG_LOCATE_RESP;
	response->port = server_nodes[server_id].cport;

	// Extract the host name from "user@host"
	char *at = strchr(server_nodes[server_id].host_name, '@');
	char *host = (at == NULL) ? server_nodes[server_id].host_name : (at + 1);

	int host_name_len = strlen(host) + 1;
	strncpy(response->host_name, host, host_name_len);

	// Reply to the client
	send_msg(fd, response, sizeof(*response) + host_name_len);
}

// Returns false if the message was invalid (so the connection will be closed)
static bool process_server_message(int fd)
{
	
	// Read and parse the message
	char req_buffer[MAX_MSG_LEN] = {0};
	if (!recv_msg(fd, req_buffer, MAX_MSG_LEN, MSG_MSERVER_CTRL_REQ)) {
		fprintf(stderr, "process_server_message failed at req msg\n");
		return false;
	}
	
	
	mserver_ctrl_request *request = (mserver_ctrl_request *)req_buffer;
	if (((int) request->type) != HEARTBEAT) {
		log_write("%s server->mserver : Receiving a server message\n", current_time_str());
	}
	
	int primary_id, secondary_id, temp_primary_id;
	int server_id = -1;
	// Process the request based on its type
	switch (request->type) {
		// When we get a heartbeat, update that server's last heartbeat time
		case HEARTBEAT:
			for (int i = 0; i < num_servers; i++) {
				if (server_nodes[i].socket_fd_in == fd) {
					server_id = i;
					break;
				}
			}
			gettimeofday(&(server_nodes[server_id].last_heartbeat), NULL);
			//*fprintf(stderr, "Recieved heartbeat from server %d\n", server_id);
			break;

		case UPDATE_PRIMARY_FAILED:
			perror("Crash recovery failed: Received UPDATE_PRIMARY_FAILED");
			break;
			
		case UPDATE_SECONDARY_FAILED:
			perror("Crash recovery failed: Received UPDATE_SECONDARY_FAILED");
			break;
			
		case UPDATED_PRIMARY:
			log_write("--- UPDATED_PRIMARY ---\n");
			for (int i = 0; i < num_servers; i++) {
				if (server_nodes[i].socket_fd_in == fd) {
					server_id = i;
					break;
				}
			}
			// Get the sid of the newly spawned replacement server
			primary_id = primary_server_id(server_id, num_servers);
			server_nodes[primary_id].updated_primary = 1;
			// Check to see if the newly spawned server is now fully in sync
			if (server_nodes[primary_id].updated_primary && server_nodes[primary_id].updated_secondary) {
				server_nodes[primary_id].halt_client_req = true;
				// Send a switch primary message to the temporary primary server, letting it know that the recovery
				// process has almost completed
				
				if (!send_switch_primary(server_id)) {
					perror("Crash recovery failed: Did not receive a response for a SWITCH-PRIMARY message");
					return false;
				}
				
				
				fprintf(stderr, "1 Setting Secondary for %d\n", primary_id);
				if (!send_set_secondary(primary_id)) {
					perror("Failure setting secondary after crash\n");
					return false;
				} 
				
				server_nodes[primary_id].halt_client_req = false;
				server_nodes[primary_id].is_alive = 1;
				fprintf(stderr, "Crash recovery - end 1\n");
			}
			break;
			
		case UPDATED_SECONDARY:
			log_write("--- UPDATED_SECONDARY ---\n");
			for (int i = 0; i < num_servers; i++) {
				if (server_nodes[i].socket_fd_in == fd) {
					server_id = i;
					break;
				}
			}
			// Get the sid of the newly spawned replacement server
			secondary_id = secondary_server_id(server_id, num_servers);
			// Get the sid of the server that's temporarily acting as the primary for the newly spawned server
			temp_primary_id = secondary_server_id(secondary_id, num_servers);
			server_nodes[secondary_id].updated_secondary = 1;
			// Check to see if the newly spawned server is now fully in sync
			if (server_nodes[secondary_id].updated_primary && server_nodes[secondary_id].updated_secondary) {
				server_nodes[secondary_id].halt_client_req = true;
				// Send a switch primary message to the temporary primary server, letting it know that the recovery
				// process has almost completed
				
				if (!send_switch_primary(temp_primary_id)) {
					perror("Crash recovery failed: Did not receive a response for a SWITCH-PRIMARY message");
					return false;
				}
				
				
				printf("Server id %d\n", server_id);
				printf("secondary_id %d\n", secondary_id);
				printf("temp_primary_id %d\n", temp_primary_id);
				/* Steven (Dec 2, 2016): Secondary already re-initializes the its primary fd after its primary restarts, 
				   so this is probably not needed*/
				// Connor (Dec 6): Seems like it is needed since it doesn't work otherwise 

				fprintf(stderr, "2 Setting Secondary for %d\n", secondary_id);
				if (!send_set_secondary(secondary_id)) {
					fprintf(stderr, "Failure setting secondary after crash 2\n");
					return false;
				} 
				
				server_nodes[secondary_id].halt_client_req = false;
				server_nodes[secondary_id].is_alive = 1;
				server_nodes[secondary_id].updated_primary = 0;		
				server_nodes[secondary_id].updated_secondary = 0;
				
				fprintf(stderr, "Crash recovery - end 2\n");
			}
			break;
			
		default: // impossible
			fprintf(stderr, "Hit default process_server_message\n");
			cleanup();
			exit(1);

	}
	
	return true;
}


static const int select_timeout_interval = 1;// seconds

// Returns false if stopped due to errors, true if shutdown was requested
static bool run_mserver_loop()
{
	// Usual preparation stuff for select()
	fd_set rset, allset;
	FD_ZERO(&allset);
	// End-of-file on stdin (e.g. Ctrl+D in a terminal) is used to request shutdown
	FD_SET(fileno(stdin), &allset);
	FD_SET(servers_fd, &allset);
	FD_SET(clients_fd, &allset);

	int max_server_fd = -1;
	for (int i = 0; i < num_servers; i++) {
		FD_SET(server_nodes[i].socket_fd_in, &allset);
		fprintf(stdout, "Initial fds - id %d, fd %d\n", i, server_nodes[i].socket_fd_in);
		max_server_fd = max(max_server_fd, server_nodes[i].socket_fd_in);
	}

	int maxfd = max(clients_fd, servers_fd);
	maxfd = max(maxfd,  max_server_fd);
	
	struct timeval now;
	double max_time_between_heartbeats = 1800; 	// In milliseconds

	for (int i = 0; i < num_servers; i++) {
		fprintf(stdout, "Server %d with port %d\n", i, server_nodes[i].sport);
	}
	
	// Metadata server sits in an infinite loop waiting for incoming connections from clients
	// and for incoming messages from already connected servers and clients
	for (;;) {
		rset = allset;

		struct timeval time_out;
		time_out.tv_sec = select_timeout_interval;
		time_out.tv_usec = 0;

		// Wait with timeout (in order to be able to handle asynchronous events such as heartbeat messages)
		int num_ready_fds = select(maxfd + 1, &rset, NULL, NULL, &time_out);
		if (num_ready_fds < 0) {
			perror("select");
			return false;
		}

		// Stop if detected EOF on stdin
		if (FD_ISSET(fileno(stdin), &rset)) {
			char buffer[1024];
			if (fgets(buffer, sizeof(buffer), stdin) == NULL) {
				return true;
			}
		}

		// Need to go through the list of servers and figure out which servers have not sent a heartbeat message yet
		// within the timeout interval. Keep information in the server_node structure regarding when was the last
		// heartbeat received from a server and compare to current time. Initiate recovery if discovered a failure.
		for (int i = 0; i < num_servers; i++) {
			gettimeofday(&now, NULL);
			int milliseconds = ((now.tv_sec - server_nodes[i].last_heartbeat.tv_sec) * 1000) + ((now.tv_usec - server_nodes[i].last_heartbeat.tv_usec) / 1000);
			// Check to see if server i has sent a heartbeat message within the last 6 secs
			if (milliseconds > max_time_between_heartbeats) {
				// If we haven't reconnected to the crashed server, keep skipping this until we do.
				
				
				server_node *node = &(server_nodes[i]);
				if (server_nodes[i].is_alive == 0) {
					gettimeofday(&(server_nodes[i].last_heartbeat), NULL);
					fprintf(stderr, "Long delay in setting up new server\n");
				}
				
				
				
				fprintf(stderr, "--- Server %d crashed ---\n", i);
				printf("--- Server %d crashed ---\n", i); ////XX
				log_write("--- Server %d crashed ---\n", i); //XX
				fprintf(stdout, "Time elapsed before server failure: %d\n", milliseconds); ////XX
				fprintf(stderr, "===Servers Alive===\n"); ////XX
				system(command); //XX
					
				// Remove from mserver fdset
				FD_CLR(node->socket_fd_in, &allset);

				// Spawn a new server (cleans fds inside)
				if (spawn_server(i) == -1) {
					perror("Crash recovery failed: Could not spawn a replacement server");
					return false;
				} 
				fprintf(stderr, "Spawned Server %d\n", i); //XX
				// Add new server to fdset
				node = &(server_nodes[i]);
				FD_SET(node->socket_fd_in, &allset);
				max_server_fd = max(max_server_fd, server_nodes[i].socket_fd_in);
				
				
				// 2&3. Send an UPDATE-PRIMARY message to the secondary server Sb for i's primary replica, and wait for the secondary server Sb to process and respond to the message
				int sb = secondary_server_id(i, num_servers);
				fprintf(stdout, "Server %d recovery. Sending update-primary msg to %d\n", i, sb);
				if (!send_update_primary(sb)) {
					perror("Crash recovery failed: Did not receive a response for a UPDATE-PRIMARY message");
					return false;
				}
				
				// 5&6. Send an UPDATE-SECONDARY message to the primary server Sc for i's secondary replica, and wait for the primary server Sc to process and respond to the message
				int sc = primary_server_id(i, num_servers);
				fprintf(stdout, "Server %d recovery. Sending update-secondary msg to %d\n", i, sc);
				if (!send_update_secondary(sc)) {
					fprintf(stderr, "Crash recovery failed: Did not receive a response for a UPDATE-SECONDARY message from server %d", sc);
					perror("");
					return false;
				}
				
				// 4. Mark Sb as the primary for the old server i's primary keys
				server_nodes[i].is_alive = 0;

				gettimeofday(&(server_nodes[i].last_heartbeat), NULL);
				
				// This service only tolerates f=1 failure
				// break;
				
				fprintf(stdout, "update_primary and update_secondary sent by time %d\n", (int) time(NULL));
			}
		}

		if (num_ready_fds <= 0 ) {
			// Due to time out
			continue;
		}

		// Incoming connection from a client
		if (FD_ISSET(clients_fd, &rset)) {
			int fd_idx = accept_connection(clients_fd, client_fd_table, MAX_CLIENT_SESSIONS);
			if (fd_idx >= 0) {
				FD_SET(client_fd_table[fd_idx], &allset);
				maxfd = max(maxfd, client_fd_table[fd_idx]);
			}

			if (--num_ready_fds <= 0) {
				continue;
			}
		}

		// Check for any messages from connected servers
		for (int i = 0; i < num_servers; i++) {
			server_node *node = &(server_nodes[i]);
			if ((node->socket_fd_in != -1) && FD_ISSET(node->socket_fd_in, &rset)) {
				if (!process_server_message(node->socket_fd_in)) {
					// Received an invalid message, close the connection
					FD_CLR(node->socket_fd_in, &allset);
					close_safe(&(node->socket_fd_in));
				}

				if (--num_ready_fds <= 0) {
					break;
				}
			}
		}
		if (num_ready_fds <= 0) {
			continue;
		}

		// Check for any messages from connected clients
		for (int i = 0; i < MAX_CLIENT_SESSIONS; i++) {
			if ((client_fd_table[i] != -1) && FD_ISSET(client_fd_table[i], &rset)) {
				process_client_message(client_fd_table[i]);
				// Close connection after processing (semantics are "one connection per request")
				FD_CLR(client_fd_table[i], &allset);
				close_safe(&(client_fd_table[i]));

				if (--num_ready_fds <= 0 ) {
					break;
				}
			}
		}
	}
}


int main(int argc, char **argv)
{
	fprintf(stdout, "main - Start mserver\n");
	signal (SIGPIPE, SIG_IGN);
	strcpy(command, "ps aux | grep yoshimo1 | grep server 1>&2");
	if (!parse_args(argc, argv)) {
		usage(argv);
		return 1;
	}
	fprintf(stdout, "main - Open log\n");
	open_log(log_file_name);
	fprintf(stdout, "main - Read config\n");
	if (!read_config_file()) {
		fprintf(stderr, "Invalid configuraion file\n");
		return 1;
	}

	fprintf(stdout, "main - Init mserver\n");
	if (!init_mserver()) {
		return 1;
	}

	fprintf(stdout, "Run mserver loop\n");
	run_mserver_loop();

	fprintf(stdout, "Main - cleanup\n");
	cleanup();
	return 0;
}
