// The key-value server implementation

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

#include "defs.h"
#include "hash.h"
#include "util.h"


// Program arguments

// Host name and port number of the metadata server
static char mserver_host_name[HOST_NAME_MAX] = "";
static uint16_t mserver_port = 0;

// Ports for listening to incoming connections from clients, servers and mserver
static uint16_t clients_port = 0;
static uint16_t servers_port = 0;
static uint16_t mservers_port = 0;

// Current server id and total number of servers
static int server_id = -1;
static int num_servers = 0;

// Log file name
static char log_file_name[PATH_MAX] = "";


static void usage(char **argv)
{
	printf("usage: %s -h <mserver host> -m <mserver port> -c <clients port> -s <servers port> "
	       "-M <mservers port> -S <server id> -n <num servers> [-l <log file>]\n", argv[0]);
	printf("If the log file (-l) is not specified, log output is written to stdout\n");
}

// Returns false if the arguments are invalid
static bool parse_args(int argc, char **argv)
{
	char option;
	while ((option = getopt(argc, argv, "h:m:c:s:M:S:n:l:")) != -1) {
		switch(option) {
			case 'h': strncpy(mserver_host_name, optarg, HOST_NAME_MAX); break;
			case 'm': mserver_port  = atoi(optarg); break;
			case 'c': clients_port  = atoi(optarg); break;
			case 's': servers_port  = atoi(optarg); break;
			case 'M': mservers_port = atoi(optarg); break;
			case 'S': server_id     = atoi(optarg); break;
			case 'n': num_servers   = atoi(optarg); break;
			case 'l': strncpy(log_file_name, optarg, PATH_MAX); break;
			default:
				fprintf(stderr, "Invalid option: -%c\n", option);
				return false;
		}
	}

	return (mserver_host_name[0] != '\0') && (mserver_port != 0) && (clients_port != 0) && (servers_port != 0) &&
	       (mservers_port != 0) && (num_servers >= 3) && (server_id >= 0) && (server_id < num_servers);
}


// Socket for sending requests to the metadata server
static int mserver_fd_out = -1;
// Socket for receiving requests from the metadata server
static int mserver_fd_in = -1;

// Sockets for listening for incoming connections from clients, servers and mserver
static int my_clients_fd = -1;
static int my_servers_fd = -1;
static int my_mservers_fd = -1;

// Store fds for all connected clients, up to MAX_CLIENT_SESSIONS
#define MAX_CLIENT_SESSIONS 1000
static int client_fd_table[MAX_CLIENT_SESSIONS];

// Store fds for connected servers
static int server_fd_table[4] = {-1, -1, -1, -1};

// Storage for primary key set
hash_table primary_hash = {0};
hash_table secondary_hash = {0};

// Primary server (the one that stores the primary copy for this server's secondary key set)
static int primary_sid = -1;
static int primary_fd = -1;

// Secondary server (the one that stores the secondary copy for this server's primary key set)
static int secondary_sid = -1;
static int secondary_fd = -1;

// Variables for heartbeat thread that runs in the background
pthread_attr_t heart_attr;
pthread_t heart_thread;
static int server_live = 0;
pthread_mutex_t server_live_lock = PTHREAD_MUTEX_INITIALIZER;

// Other variables for the recovery process
static bool secondary_replica_primary_up = true;
pthread_t sync_thread;
static int sync_primary_fd = -1;
static int sync_secondary_fd = -1;

// True if a "bad" fd was recently discovered while process_client_message was running
static bool client_fd_invalid = false;

static void cleanup();

static const int hash_size = 65536;

/* Thread for sending heartbeats periodically to the mserver
   delay: Delay (in milliseconds) between each heartbeat message sent
          to the server
 */
static void *heartbeats(void *unused) {
	printf("heartbeats - start\n"); 
	long delay_milliseconds = 1; 
	struct timespec ts;
	ts.tv_sec = 1;
	ts.tv_nsec = (delay_milliseconds % 1000) * 1000000L;
	int live = 1;
	
	while (live) {	
		char send_buffer[MAX_MSG_LEN] = {0};
		mserver_ctrl_request *request = (mserver_ctrl_request *)send_buffer;
		request->hdr.type = MSG_MSERVER_CTRL_REQ;
		request->type = HEARTBEAT;
		//*fprintf(stdout, "Server %d sending heartbeat at time %d\n", server_id, (int) time(NULL));
		send_msg(mserver_fd_out, request, sizeof(*request));
		nanosleep(&ts, NULL);
		//sleep(5);
		pthread_mutex_lock(&server_live_lock);
		live = server_live;
		pthread_mutex_unlock(&server_live_lock); 
	}
	
	fprintf(stdout, "Thread exited\n");
	return NULL;
}

// Initialize and start the server
static bool init_server()
{
	printf("init_server - start\n");
	for (int i = 0; i < MAX_CLIENT_SESSIONS; i++) {
		client_fd_table[i] = -1;
	}

	// Get the host name that server is running on
	char my_host_name[HOST_NAME_MAX] = "";
	if (get_local_host_name(my_host_name, sizeof(my_host_name)) < 0) {
		return false;
	}
	log_write("%s Server starts on host: %s\n", current_time_str(), my_host_name);

	// Create sockets for incoming connections from clients and other servers
	if (((my_clients_fd  = create_server(clients_port, MAX_CLIENT_SESSIONS, NULL)) < 0) ||
	    ((my_servers_fd  = create_server(servers_port, 2, NULL)) < 0) ||
	    ((my_mservers_fd = create_server(mservers_port, 1, NULL)) < 0))
	{
		goto cleanup;
	}

	// Connect to mserver to "register" that we are live
	if ((mserver_fd_out = connect_to_server(mserver_host_name, mserver_port)) < 0) {
		goto cleanup;
	}

	// Determine the ids of replica servers
	primary_sid = primary_server_id(server_id, num_servers);
	secondary_sid = secondary_server_id(server_id, num_servers);

	// Initialize key-value storage
	if (!hash_init(&primary_hash, hash_size) || !hash_init(&secondary_hash, hash_size)) {
		goto cleanup;
	}

	/* 
	// Create a separate thread that takes care of sending periodic heartbeat messages
	*/
	
	printf("init_server - make heartbeat\n");
	pthread_create(&heart_thread, NULL, heartbeats, NULL);
	server_live = 1;
	
	log_write("Server initialized\n");
	return true;

cleanup:
	cleanup();
	return false;
}

// Hash iterator for freeing memory used by values; called during storage cleanup
static void clean_iterator_f(const char key[KEY_SIZE], void *value, size_t value_sz, void *arg)
{
	(void)key;
	(void)value_sz;
	(void)arg;

	assert(value != NULL);
	free(value);
}

// Cleanup and release all the resources
static void cleanup()
{
	fprintf(stdout, "server cleanup - start\n");
	close_safe(&mserver_fd_out);
	close_safe(&mserver_fd_in);
	close_safe(&my_clients_fd);
	close_safe(&my_servers_fd);
	close_safe(&my_mservers_fd);
	close_safe(&secondary_fd);
	close_safe(&primary_fd);

	for (int i = 0; i < MAX_CLIENT_SESSIONS; i++) {
		close_safe(&(client_fd_table[i]));
	}
	for (int i = 0; i < 4; i++) {
		close_safe(&(server_fd_table[i]));
	}

	hash_iterate(&primary_hash, clean_iterator_f, NULL);
	hash_cleanup(&primary_hash);

	// TODO: release all other resources
	// Cleanup the secondary hash table
	hash_iterate(&secondary_hash, clean_iterator_f, NULL);
	hash_cleanup(&secondary_hash);
	
	// Let the heartbeat thread exit
	pthread_mutex_lock(&server_live_lock);
	server_live = 0;
	pthread_mutex_unlock(&server_live_lock);
	pthread_join(heart_thread, NULL);
}


// Connection will be closed after calling this function regardless of result
static void process_client_message(int fd)
{
	log_write("%s server %d on fd %d receiving a client message\n", current_time_str(), server_id, my_servers_fd);
	fprintf(stdout, "---server %d on port %d receiving a client message---\n", server_id, servers_port);
	// Initialize the response		
	char resp_buffer[MAX_MSG_LEN] = {0};		
	operation_response *response = (operation_response *)resp_buffer;		
	response->hdr.type = MSG_OPERATION_RESP;		
	uint16_t value_sz = 0;

	// Read and parse the message
	char req_buffer[MAX_MSG_LEN] = {0};
	if (!recv_msg(fd, req_buffer, MAX_MSG_LEN, MSG_OPERATION_REQ)) {
		response->status = SERVER_FAILURE;		
		fprintf(stderr, "Failed to recieve client message\n");		
		send_msg(fd, response, sizeof(*response) + value_sz);
		return;
	}

	operation_request *request = (operation_request *)req_buffer;

	// Steven: Only using the lock/unlock functions from hash.h atm. More synchronization to be added here when/if needed

	// Check that requested key is valid
	int key_srv_id = key_server_id(request->key, num_servers);

	// Process the request based on its type
	switch (request->type) {
		case OP_NOOP:
			response->status = SUCCESS;
			break;

		case OP_GET: {
			void *data = NULL;
			size_t size = 0;
			
			if (server_id == secondary_server_id(key_srv_id, num_servers)) {
				// The server is temporarily acting as the primary for the crashed server
				// Get the value for requested key from the hash table
				hash_lock(&secondary_hash, request->key);
				if (!hash_get(&secondary_hash, request->key, &data, &size)) {
					hash_unlock(&secondary_hash, request->key);
					fprintf(stderr, "sid %d: key %s not found on acting primary %d \n", server_id, key_to_str(request->key), key_srv_id);
					log_write("Key %s not found while acting as temporary primary\n", key_to_str(request->key));
					response->status = KEY_NOT_FOUND;
					break;
				}
				hash_unlock(&secondary_hash, request->key);
				fprintf(stdout, "sid %d received request for key %s while acting as temporary primary\n", server_id, key_to_str(request->key));
			} 
			else if (key_srv_id != server_id)  {
				fprintf(stderr, "sid %d: Invalid client key %s sid %d\n", server_id, key_to_str(request->key), key_srv_id);
				response->status = KEY_NOT_FOUND;
				send_msg(fd, response, sizeof(*response) + value_sz);
				return;
			}
			else {
				// Get the value for requested key from the hash table
				hash_lock(&primary_hash, request->key);
				if (!hash_get(&primary_hash, request->key, &data, &size)) {
					hash_unlock(&primary_hash, request->key);
					fprintf(stderr, "sid %d: key %s not found on sid %d \n", server_id, key_to_str(request->key), key_srv_id);
					log_write("Key %s not found\n", key_to_str(request->key));
					response->status = KEY_NOT_FOUND;
					break;
				}
				hash_unlock(&primary_hash, request->key);
			}

			// Copy the stored value into the response buffer
			memcpy(response->value, data, size);
			value_sz = size;

			response->status = SUCCESS;
			break;
		}

		case OP_PUT: {
			// Need to copy the value to dynamically allocated memory
			size_t value_size = request->hdr.length - sizeof(*request);
			void *value_copy = malloc(value_size);
			if (value_copy == NULL) {
				perror("malloc");
				fprintf(stderr, "sid %d: Out of memory\n", server_id);
				response->status = OUT_OF_SPACE;
				break;
			}
			memcpy(value_copy, request->value, value_size);

			void *old_value = NULL;
			size_t old_value_sz = 0;
			
			// Check to see if the key falls within this server's key range (this is done to avoid any
			// malicious attacks conducted by users)
			int key_primary_sid = key_server_id(request->key, num_servers);
			if ((key_primary_sid != server_id) && (server_id != secondary_server_id(key_primary_sid, num_servers))) {
				// This server does not hold either the primary replica or secondary replica for this specific key
				// So we need to send an "invalid request" to the client and ignore the request
				// TODO: Not sure about which error message we're supposed to send. Using SERVER_FAILURE for now as a 
				//		 placeholder
				response->status = SERVER_FAILURE;
				break;
			}
			
			char recv_buffer[MAX_MSG_LEN] = {0};
			if ((!secondary_replica_primary_up) && (server_id == secondary_server_id(key_primary_sid, num_servers))) {
				log_write("Secondary Replica's Primary is Down\n");
				// This server is temporarily acting as the primary server for the secondary replica's primary
				// So update the secondary replica and also forward the PUT request to the secondary replica's primary
				fprintf(stdout, "Stored %s in secondary hash on server %d (with primary down), replacing %s\n", (char *) value_copy, server_id, (char *) old_value);
				hash_lock(&secondary_hash, request->key);
				if (!hash_put(&secondary_hash, request->key, value_copy, value_size, &old_value, &old_value_sz))
				{
					hash_unlock(&secondary_hash, request->key);
					fprintf(stderr, "sid %d: Out of memory\n", server_id);
					free(value_copy);
					response->status = OUT_OF_SPACE;
					break;
				}
				if (!send_msg(primary_fd, request, sizeof(*request) + value_size)) {
					fprintf(stderr, "Could not forward PUT request to this secondary server's primary %d\n", primary_sid);
					response->status = SERVER_FAILURE;
					hash_unlock(&secondary_hash, request->key);
					break;
				}

				if (!recv_msg(primary_fd, recv_buffer, MAX_MSG_LEN*sizeof(char), MSG_OPERATION_RESP)) {
					fprintf(stderr, "Did not receive response when forwarding PUT requests to this secondary server's primary %d\n", primary_sid);
					response->status = SERVER_FAILURE;
					hash_unlock(&secondary_hash, request->key);
					break;
				}
				hash_unlock(&secondary_hash, request->key);
				break;
			}
			
			// The current server is meant to hold the primary replica for this key
			// Put the <key, value> pair into the hash table
			hash_lock(&primary_hash, request->key);
			if (!hash_put(&primary_hash, request->key, value_copy, value_size, &old_value, &old_value_sz))
			{
				hash_unlock(&primary_hash, request->key);
				fprintf(stderr, "sid %d: Out of memory\n", server_id);
				free(value_copy);
				response->status = OUT_OF_SPACE;
				break;
			}
			fprintf(stdout, "Stored %s in primary hash on server %d, replacing %s\n", (char *) value_copy, server_id, (char *) old_value);

			// forward the PUT request to the secondary replica
			// Forward the PUT request, and make sure the secondary replica accepts these changes before responding to the client
			fprintf(stdout, "Request type %s\n", msg_type_str[request->hdr.type]);
			fprintf(stdout, "Writing to secondary fd %d with msg\n", secondary_fd);
			if (!send_msg(secondary_fd, request, sizeof(*request) + value_size))
			{
				// The message couldn't be sent to the replica
				fprintf(stderr, "Secondary message send failure\n");
				client_fd_invalid = true;
				response->status = SERVER_FAILURE;
				hash_unlock(&primary_hash, request->key);
				break;
			}
			fprintf(stdout, "Msg sent\n");

			if (!recv_msg(secondary_fd, recv_buffer, MAX_MSG_LEN*sizeof(char), MSG_OPERATION_RESP)) {
				fprintf(stderr, "Secondary message response failure when sending messages to %d from %d\n", secondary_sid, server_id);
				log_write("Secondary message response failure when sending messages to %d from %d\n", secondary_sid, server_id);
				client_fd_invalid = true;
				response->status = SERVER_FAILURE;
				hash_unlock(&primary_hash, request->key);
				break;
			}

			hash_unlock(&primary_hash, request->key);
			operation_response *op_resp = (operation_response *)recv_buffer;
			assert (op_resp != NULL);
			// Check to see if the forwarded key value pair was successfully stored in the secondary replica
			if ((op_resp->status == SERVER_FAILURE) ||
				(op_resp->status == OUT_OF_SPACE)) {
					fprintf(stdout, "Secondary storage failure\n");
				response->status = SERVER_FAILURE;
				break;
			}

			// Need to free the old value (if there was any)
			if (old_value != NULL) {
				free(old_value);
			}

			response->status = SUCCESS;
			break;
		}

		default:
			fprintf(stderr, "sid %d: Invalid client operation type\n", server_id);
			return;
	}

	// Send reply to the client
	if (response->status == SERVER_FAILURE) {
		log_write("Server %d failed to send/receive msg\n", server_id);
		fprintf(stderr, "Server %d failed to send/receive msg\n", server_id);
	}
	
	send_msg(fd, response, sizeof(*response) + value_sz);
}


// Returns false if either the message was invalid or if this was the last message
// (in both cases the connection will be closed)
static bool process_server_message(int fd)
{
	
	fprintf(stdout, "---server %d on port %d receiving a server message from fd %d---\n", server_id, servers_port, fd);
	// Read and parse the message
	char req_buffer[MAX_MSG_LEN] = {0};
	if (!recv_msg(fd, req_buffer, MAX_MSG_LEN, MSG_OPERATION_REQ)) {
		return false;
	}
	operation_request *request = (operation_request *)req_buffer;

	// NOOP operation request is used to indicate the last message in an UPDATE sequence
	if (request->type == OP_NOOP) {
		log_write("Received the last server message, closing connection\n");
		return false;
	}

	// TODO: process the message and send the response
	// Initialize the response
	char resp_buffer[MAX_MSG_LEN] = {0};
	//server_ctrl_response response = {0}; // TODO: Unused
	operation_response *response = (operation_response *)resp_buffer;
	uint16_t value_sz = 0;
	
	// Process the request based on its type
	if (request->type == OP_PUT) {
		// A server holding the primary replica for the key has forwarded a PUT request to this server so store the key value pair in the secondary replica
		size_t value_size = request->hdr.length - sizeof(*request);
		void *value_copy = malloc(value_size);
		if (value_copy == NULL) {
			perror("malloc");
			fprintf(stderr, "sid %d: Out of memory\n", server_id);
			response->status = OUT_OF_SPACE;
			send_msg(fd, response, sizeof(*response) + value_sz);
			return false;
		}
		memcpy(value_copy, request->value, value_size);
		void *old_value = NULL;
		size_t old_value_sz = 0;
		
		int key_primary_sid = key_server_id(request->key, num_servers);
		if (key_primary_sid == server_id) {
			hash_lock(&primary_hash, request->key);
		
			printf("--Copying value %s to server %d secondary hash---\n", (char *)request->value, server_id);
			if (!hash_put(&primary_hash, request->key, value_copy, value_size, &old_value, &old_value_sz))
			{
				hash_unlock(&primary_hash, request->key);
				fprintf(stderr, "sid %d: Out of memory\n", server_id);
				free(value_copy);
				response->status = OUT_OF_SPACE;
				return false;
			}
			hash_unlock(&primary_hash, request->key);
		}
		else if (secondary_server_id(key_primary_sid, num_servers) == server_id) {
			hash_lock(&secondary_hash, request->key);
		
			printf("--Copying value %s to server %d secondary hash---\n", (char *)request->value, server_id);
			if (!hash_put(&secondary_hash, request->key, value_copy, value_size, &old_value, &old_value_sz))
			{
				hash_unlock(&secondary_hash, request->key);
				fprintf(stderr, "sid %d: Out of memory\n", server_id);
				free(value_copy);
				response->status = OUT_OF_SPACE;
				return false;
			}
			hash_unlock(&secondary_hash, request->key);
		}
		else {
			// Impossible
			return false;
		}
		free(old_value);
		response->hdr.type = MSG_OPERATION_RESP;
		response->status = SUCCESS;
	} else if (request->type == OP_PUT_PRIMARY) {
			// This is being used when the server has crashed to update
			// directly to the servers primary hash
			hash_lock(&primary_hash, request->key);
			printf("--Copying value %s to server %d primary hash---\n", (char *)request->value, server_id);
			size_t value_size = request->hdr.length - sizeof(*request);
			void *value_copy = malloc(value_size);
			if (value_copy == NULL) {
				perror("malloc");
				fprintf(stderr, "sid %d: Out of memory\n", server_id);
				response->status = OUT_OF_SPACE;
				send_msg(fd, response, sizeof(*response) + value_sz);
				return false;
			}
			memcpy(value_copy, request->value, value_size);
			void *old_value = NULL;
			size_t old_value_sz = 0;
			printf("---Primary Hash - Putting value %s on server %d---\n", (char *)request->value, server_id);
			if (!hash_put(&primary_hash, request->key, value_copy, value_size, &old_value, &old_value_sz))
			{
				hash_unlock(&primary_hash, request->key);
				fprintf(stderr, "sid %d: Out of memory\n", server_id);
				free(value_copy);
				response->status = OUT_OF_SPACE;
				return false;
			}
			free(old_value);
			hash_unlock(&primary_hash, request->key);
			response->hdr.type = MSG_OPERATION_RESP;
			response->status = SUCCESS;
	} else {// impossible
			log_write("Error - reached default with message type %d\n", request->type);
			fprintf(stderr, "Error - reached default with message type %d\n", request->type);
			assert(false);
			response->status = SERVER_FAILURE;
	}

	// Send the response
	fprintf(stdout, "process_server_message - send_msg\n"); 
	send_msg(fd, response, sizeof(*response) + value_sz);
	return true;
}

// Send the given (key, value) pair to the server with the given sid
static void *update_server(const char key[KEY_SIZE], void *value, size_t value_sz, void *new_server_id) {
	assert(key != NULL);
	assert(value  != NULL);
	assert(new_server_id != NULL);
	int sid = *((int *) new_server_id);
	char send_buffer[MAX_MSG_LEN] = {0};
	char recv_buffer[MAX_MSG_LEN] = {0};
	operation_request *request = (operation_request*)send_buffer;
	request->hdr.type = MSG_OPERATION_REQ;
	memcpy(request->key, key, KEY_SIZE);

	strncpy(request->value, value, value_sz);
	
	int server_fd;
	if (sid == primary_sid) {
		server_fd = sync_primary_fd;
		request->type = OP_PUT_PRIMARY;
		printf("update_server copying key %s to server %d's primary\n", key, sid);
		log_write("primary sid %d, fd %d\n", primary_sid, primary_fd);
	}
	else if (sid == secondary_sid) {
		server_fd = sync_secondary_fd;
		request->type = OP_PUT;
		printf("update_server copying key %s to server %d' secondary\n", key, sid);
		log_write("secondary sid %d, fd %d\n", secondary_sid, secondary_fd);
	} else {
		// Impossible
		fprintf(stderr, "%d does not match primary or secondary sid in update_server (Current server id: %d)\n", sid, server_id);
		return NULL;
	}

	printf("Sending value %s to server %d with fd %d\n", (char *)value, sid, server_fd);

	// Temporary	
	operation_response *response = (operation_response *)request;
	if (response->status == KEY_NOT_FOUND)
		fprintf(stderr, "Possibly something wrong with update_server");

	if (!send_msg(server_fd, request, sizeof(*request) + value_sz)) {
		fprintf(stderr, "Encountered a problem while sending the server %d with the given (key, value) pair\n", sid);
		return false;
	}
	
	if (!recv_msg(server_fd, recv_buffer, sizeof(recv_buffer), MSG_OPERATION_RESP)) {
		fprintf(stderr, "Encountered a problem while receiving the msg from server %d with the given (key, value) pair\n", sid);
		return false;
	}
	
	operation_response *op_resp = (operation_response *)recv_buffer;
	printf("Response status %s\n", op_status_str[op_resp->status]);

	return NULL;
}


// Sends this server's secondary replica to the new server
static void *synchronize_server(void *new_server_id) {
	hash_iterator *iterator = (hash_iterator *) update_server;
	
	// Check to see if this server is the primary for the crashed server's secondary
	// or if its the secondary for the crashed server's primary
	int sid = *((int *) new_server_id);

	fprintf(stderr, "synchronize_server - start with id %d. Current server id %d\n", sid, server_id);
	int primary_replica_secondary_sid = secondary_server_id(sid, num_servers);
	int secondary_replica_primary_sid = primary_server_id(sid, num_servers);
	if (primary_replica_secondary_sid == server_id) {
		// This server is primary server sid's secondary, so update
		// the primary with the data in this server's secondary replica
		printf("synchronize_server - updating primary sid %d\n", sid);
		hash_iterate(&secondary_hash, iterator, new_server_id);
		
		printf("synchronize_server - done sending primary\n");
		// Send response to server
		mserver_ctrl_request request = {0};
		request.hdr.type = MSG_MSERVER_CTRL_REQ;
		request.type = UPDATED_PRIMARY;
		send_msg(mserver_fd_out, &request, sizeof(request));
		close_safe(&sync_primary_fd);
	}
	else if (secondary_replica_primary_sid == server_id) {
		printf("synchronize_server - updating secondary sid %d\n", sid);
		// This server is secondary server sid's primary, so update
		// the secondary with the data in this server's primary replica
		hash_iterate(&primary_hash, iterator, new_server_id);
		printf("synchronize_server - done sending secondary\n");
		mserver_ctrl_request request = {0};
		request.hdr.type = MSG_MSERVER_CTRL_REQ;
		request.type = UPDATED_SECONDARY;
		send_msg(mserver_fd_out, &request, sizeof(request));
		close_safe(&sync_secondary_fd);
	}
	else {
		// Impossible
		fprintf(stderr, "synchronize_server - This server is not the given new_server_id's primary or secondary\n");
	}
	return NULL;
}

// Returns false if the message was invalid (so the connection will be closed)
// Sets *shutdown_requested to true if received a SHUTDOWN message (so the server will terminate)
static bool process_mserver_message(int fd, bool *shutdown_requested)
{
	assert(shutdown_requested != NULL);
	*shutdown_requested = false;

	log_write("%s mserver->server : Receiving a metadata server message\n", current_time_str());

	// Read and parse the message
	char req_buffer[MAX_MSG_LEN] = {0};
	if (!recv_msg(fd, req_buffer, MAX_MSG_LEN, MSG_SERVER_CTRL_REQ)) {
		return false;
	}
	server_ctrl_request *request = (server_ctrl_request*)req_buffer;

	// Initialize the response
	server_ctrl_response response = {0};
	response.hdr.type = MSG_SERVER_CTRL_RESP;

	// Process the request based on its type
	switch (request->type) {
		case SET_SECONDARY:
			response.status = ((secondary_fd = connect_to_server(request->host_name, request->port)) < 0)
			                ? CTRLREQ_FAILURE : CTRLREQ_SUCCESS;
			printf("Connected to secondary port %d, with status %s\n", request->port, op_status_str[response.status]);
			break;

		case UPDATE_PRIMARY: 
			log_write("server - Update Primary\n");
			secondary_replica_primary_up = false;
			if (((primary_fd = connect_to_server(request->host_name, request->port)) < 0) || 
				((sync_primary_fd = connect_to_server(request->host_name, request->port)) < 0)) {
				response.status = CTRLREQ_FAILURE;
				fprintf(stderr, "UPDATE_PRIMARY - Couldn't connect to new server\n");
			}
			else {
				// Spawn a new thread to asynchronously send its secondary replica to the new server	
				pthread_create(&sync_thread, NULL, synchronize_server, &primary_sid);
				
				response.status = CTRLREQ_SUCCESS;
			}
				
			break;
		case UPDATE_SECONDARY:
			log_write("--- Server %d recieved UPDATE_SECONDARY---\n", server_id);
			close_safe(&secondary_fd);
			if (((secondary_fd = connect_to_server(request->host_name, request->port)) < 0) ||
				((sync_secondary_fd = connect_to_server(request->host_name, request->port)) < 0)){
				response.status = CTRLREQ_FAILURE;
				fprintf(stderr, "UPDATE_SECONDARY - Couldn't connect to new server\n");
			} else {
				pthread_create(&sync_thread, NULL, synchronize_server, &secondary_sid);

				response.status = CTRLREQ_SUCCESS;
			}
			break;
		case SWITCH_PRIMARY:
			// Don't accept anymore incoming client requests for set X
			
			close_safe(&primary_fd);
			secondary_replica_primary_up = true;
			response.status = CTRLREQ_SUCCESS;
			break;
		case SHUTDOWN:
			*shutdown_requested = true;
			return true;

		default:// impossible
			assert(false);
			break;
	}

	fprintf(stdout, "process_mserver_message - send_msg\n");
	send_msg(fd, &response, sizeof(response));
	return true;
}


// Returns false if stopped due to errors, true if shutdown was requested
static bool run_server_loop()
{
	// Usual preparation stuff for select()
	fd_set rset, allset;
	FD_ZERO(&allset);
	FD_SET(my_clients_fd, &allset);
	FD_SET(my_servers_fd, &allset);
	FD_SET(my_mservers_fd, &allset);

	int maxfd = max(my_clients_fd, my_servers_fd);
	maxfd = max(maxfd, my_mservers_fd);

	// Server sits in an infinite loop waiting for incoming connections from mserver/servers/client
	// and for incoming messages from already connected mserver/servers/clients
	for (;;) {
		rset = allset;

		int num_ready_fds = select(maxfd + 1, &rset, NULL, NULL, NULL);
		if (num_ready_fds < 0) {
			perror("select");
			return false;
		}

		if (num_ready_fds <= 0) {
			continue;
		}

		// Incoming connection from the metadata server
		if (FD_ISSET(my_mservers_fd, &rset)) {
			int fd_idx = accept_connection(my_mservers_fd, &mserver_fd_in, 1);
			if (fd_idx >= 0) {
				FD_SET(mserver_fd_in, &allset);
				maxfd = max(maxfd, mserver_fd_in);
			}
			assert(fd_idx == 0);

			if (--num_ready_fds <= 0) {
				continue;
			}
		}

		// Incoming connection from a key-value server
		if (FD_ISSET(my_servers_fd, &rset)) {
			int fd_idx = accept_connection(my_servers_fd, server_fd_table, 4);
			if (fd_idx >= 0) {
				printf("Server %d connected\n", fd_idx);
				FD_SET(server_fd_table[fd_idx], &allset);
				maxfd = max(maxfd, server_fd_table[fd_idx]);
			}

			if (--num_ready_fds <= 0) {
				continue;
			}
		}

		// Incoming connection from a client
		if (FD_ISSET(my_clients_fd, &rset)) {
			int fd_idx = accept_connection(my_clients_fd, client_fd_table, MAX_CLIENT_SESSIONS);
			if (fd_idx >= 0) {
				FD_SET(client_fd_table[fd_idx], &allset);
				maxfd = max(maxfd, client_fd_table[fd_idx]);
			}

			if (--num_ready_fds <= 0) {
				continue;
			}
		}

		// Check for any messages from the metadata server
		if ((mserver_fd_in != -1) && FD_ISSET(mserver_fd_in, &rset)) {
			bool shutdown_requested = false;
			if (!process_mserver_message(mserver_fd_in, &shutdown_requested)) {
				// Received an invalid message, close the connection
				fprintf(stderr, "Invalid message from mserver\n");
				FD_CLR(mserver_fd_in, &allset);
				close_safe(&(mserver_fd_in));
			} else if (shutdown_requested) {
				fprintf(stderr, "Shutdown requested by mserver for Server %d\n", server_id); //TODO
				return true;
			}

			if (--num_ready_fds <= 0) {
				continue;
			}
		}

		// Check for any messages from connected key-value servers
		for (int i = 0; i < 4; i++) {
			if ((server_fd_table[i] != -1) && FD_ISSET(server_fd_table[i], &rset)) {
				if (!process_server_message(server_fd_table[i])) {
					// Received an invalid message (or the last valid message), close the connection
					FD_CLR(server_fd_table[i], &allset);
					close_safe(&(server_fd_table[i]));
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
		client_fd_invalid = false;
		for (int i = 0; i < MAX_CLIENT_SESSIONS; i++) {
			if ((client_fd_table[i] != -1) && FD_ISSET(client_fd_table[i], &rset)) {
				//*printf("Got client message on server %d\n", server_id);
				process_client_message(client_fd_table[i]);
				//*fprintf(stderr, "Server %d has processed msg\n", server_id);
				// Close connection after processing (semantics are "one connection per request")
				FD_CLR(client_fd_table[i], &allset);
				close_safe(&(client_fd_table[i]));

				if (--num_ready_fds <= 0) {
					break;
				}
			}
		}
	}
}


int main(int argc, char **argv)
{
	signal (SIGPIPE, SIG_IGN);
	if (!parse_args(argc, argv)) {
		usage(argv);
		return 1;
	}

	open_log(log_file_name);

	if (!init_server()) {
		return 1;
	}
	printf("Server %d, my_servers_fd %d\n", server_id, my_servers_fd);
	run_server_loop();

	cleanup();
	return 0;
}
