#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>

#include "collectd.h"
#include "common.h"
#include "plugin.h"
#include "cJSON.h"

/* Plugin name */
#define PLUGIN_NAME "docker"

#define STRING_LEN 128

typedef struct container_s {
	char id[STRING_LEN];
	char image_name[STRING_LEN];
} container_t;

enum disk_switch {
	OPS, BYTES
};

container_t* containers;
int containers_num = 0;

static void init_value_list (value_list_t *vl)
{
    sstrncpy (vl->plugin, PLUGIN_NAME, sizeof (vl->plugin));

    sstrncpy (vl->host, hostname_g, sizeof (vl->host));
	vl->meta = meta_data_create();
}
/*
static void vcpu_total_submit (unsigned long long cpu_time, container_t* container, const char *type)
{
    cpu_submit (cpu_time, container, type);
}*/

static void cpu_submit (unsigned long long cpu_time, container_t* container, const char *type)
{
    value_t values[1];
    value_list_t vl = VALUE_LIST_INIT;

    init_value_list (&vl);

    values[0].derive = cpu_time;

    vl.values = values;
    vl.values_len = 1;

	char tags[1024];
    ssnprintf(tags,1024,"container_id=%s image_name=%s",container->id,container->image_name);
	meta_data_add_string (vl.meta,"tsdb_tags",tags);
	
    sstrncpy (vl.type, type, sizeof (vl.type));

    plugin_dispatch_values (&vl);
}

static void cpus_submit (unsigned long long vcpu_time, int vcpu_nr, container_t* container, const char *type) {
	value_t values[1];
    value_list_t vl = VALUE_LIST_INIT;

    init_value_list (&vl);

    values[0].derive = vcpu_time;

    vl.values = values;
    vl.values_len = 1;

	char tags[1024];
    ssnprintf(tags,1024,"container_id=%s image_name=%s",container->id,container->image_name);
	meta_data_add_string (vl.meta,"tsdb_tags",tags);
	
    sstrncpy (vl.type, type, sizeof (vl.type));
    ssnprintf (vl.type_instance, sizeof (vl.type_instance), "%d", vcpu_nr);
    
    plugin_dispatch_values (&vl);
}

void submit_cpu_stats (cJSON* root, container_t* container){
	int cpus_num,i;
	
	/*int vcpu=cJSON_GetObjectItem(cJSON_GetObjectItem(cJSON_GetObjectItem(root,"cpu_stats"),"cpu_usage"),"total_usage")->valueint;
	vcpu_total_submit (get_vcpu(root), container, "virt_cpu_total");*/
	
	int cpu = cJSON_GetObjectItem(cJSON_GetObjectItem(cJSON_GetObjectItem(root,"cpu_stats"),"cpu_usage"),"total_usage")->valueint;
	cpu_submit (cpu, container, "cpu");
	
	cJSON* cpus_array = cJSON_GetObjectItem(cJSON_GetObjectItem(cJSON_GetObjectItem(root,"cpu_stats"),"cpu_usage"),"percpu_usage");
	cpus_num = cJSON_GetArraySize(cpus_array);
	
	for (i=0;i<cpus_num;i++) {
		cpus_submit (cJSON_GetArrayItem(cpus_array,i)->valueint,i,container,"cpu");
	}
}

static void get_disk_stats (cJSON* root, unsigned long* write_val, unsigned long* read_val, enum disk_switch data_type) {
	cJSON* disk_stats_array = NULL;
	switch (data_type) {
		case OPS:
			disk_stats_array = cJSON_GetObjectItem(cJSON_GetObjectItem(root,"blkio_stats"),"io_serviced_recursive");
			break;
		case BYTES:
			disk_stats_array = cJSON_GetObjectItem(cJSON_GetObjectItem(root,"blkio_stats"),"io_service_bytes_recursive");
			break;
	}
	
	int i, disk_stats_num = cJSON_GetArraySize(disk_stats_array);
	
	cJSON* tmp;
	char* op;
	for (i=0;i<disk_stats_num;i++) {
		 tmp = cJSON_GetArrayItem(disk_stats_array,i);
		 op = cJSON_GetObjectItem(tmp,"op")->valuestring;
		 if (!strcmp(op,"Read")) {
			 *read_val = cJSON_GetObjectItem(tmp,"value")->valueint;
		 }
		 if (!strcmp(op,"Write")) {
			 *write_val = cJSON_GetObjectItem(tmp,"value")->valueint;
		 }
	}
}

static void
submit_derive2 (const char *type, derive_t v0, derive_t v1, container_t* container)
{
    value_t values[2];
    value_list_t vl = VALUE_LIST_INIT;

    init_value_list (&vl);

    values[0].derive = v0;
    values[1].derive = v1;
    vl.values = values;
    vl.values_len = 2;

	char tags[1024];
    ssnprintf (tags,1024,"container_id=%s image_name=%s",container->id,container->image_name);
	meta_data_add_string (vl.meta,"tsdb_tags",tags);
	
    sstrncpy (vl.type, type, sizeof (vl.type));

    plugin_dispatch_values (&vl);
}

void submit_disk_stats (cJSON* root, container_t* container) {
	unsigned long read = 0, write = 0;
	get_disk_stats (root, &write, &read, OPS);
	submit_derive2 ("disk_ops", (derive_t) read, (derive_t) write, container);
	
	get_disk_stats (root, &write, &read, BYTES);
	submit_derive2 ("disk_octets", (derive_t) read, (derive_t) write, container);
}

void memory_submit (unsigned long value, container_t* container, const char* type_instance) {
	value_t values[1];
    value_list_t vl = VALUE_LIST_INIT;

    init_value_list (&vl);

    values[0].gauge = value;

    vl.values = values;
    vl.values_len = 1;

	char tags[1024];
    ssnprintf(tags,1024,"container_id=%s image_name=%s",container->id,container->image_name);
	meta_data_add_string (vl.meta,"tsdb_tags",tags);
	
    sstrncpy (vl.type, "memory", sizeof (vl.type));
    ssnprintf (vl.type_instance, sizeof (vl.type_instance), "%s", type_instance);
    
    plugin_dispatch_values (&vl);
}

void submit_memory_stats (cJSON* root, container_t* container) {
	/*get current memory usage*/
	unsigned long mem_value = cJSON_GetObjectItem(cJSON_GetObjectItem(root,"memory_stats"),"usage")->valueint;
	memory_submit(mem_value,container,"usage");
	
	mem_value = cJSON_GetObjectItem(cJSON_GetObjectItem(root,"memory_stats"),"failcnt")->valueint;
	memory_submit(mem_value,container,"fail_count");

	mem_value = cJSON_GetObjectItem(cJSON_GetObjectItem(cJSON_GetObjectItem(root,"memory_stats"),"stats"),"rss")->valueint;
	memory_submit(mem_value,container,"rss");
	
	mem_value = cJSON_GetObjectItem(cJSON_GetObjectItem(cJSON_GetObjectItem(root,"memory_stats"),"stats"),"pgmajfault")->valueint;
	memory_submit(mem_value,container,"pgfault_major");
	
	mem_value = cJSON_GetObjectItem(cJSON_GetObjectItem(cJSON_GetObjectItem(root,"memory_stats"),"stats"),"pgfault")->valueint;
	memory_submit(mem_value,container,"pgfault_minor");
}

static void get_network_stats (cJSON* root, unsigned long* rx_val, unsigned long* tx_val, const char* rx_string, const char* tx_string) {
	*rx_val = (unsigned long) cJSON_GetObjectItem(cJSON_GetObjectItem(root,"network"),rx_string)->valueint;
	*tx_val = (unsigned long) cJSON_GetObjectItem(cJSON_GetObjectItem(root,"network"),tx_string)->valueint;
}

void submit_network_stats (cJSON* root, container_t* container) {
	unsigned long rx = 0, tx = 0;
	get_network_stats (root, &rx, &tx, "rx_dropped","tx_dropped");
	submit_derive2 ("if_dropped", (derive_t) rx, (derive_t) tx, container);
	
	get_network_stats (root, &rx, &tx, "rx_bytes","tx_bytes");
	submit_derive2 ("if_octets", (derive_t) rx, (derive_t) tx, container);
	
	get_network_stats (root, &rx, &tx, "rx_packets","tx_packets");
	submit_derive2 ("if_packets", (derive_t) rx, (derive_t) tx, container);
	
	get_network_stats (root, &rx, &tx, "rx_errors","tx_errors");
	submit_derive2 ("if_errors", (derive_t) rx, (derive_t) tx, container);
}

void *read_container (void* cont) {
	container_t* container = (container_t*) cont;
	struct sockaddr_un address;
	int nbytes,socket_fd;
	char buffer[256 + STRING_LEN];
	char response[5000];
	char* json_string;
	
	socket_fd = socket(PF_UNIX, SOCK_STREAM, 0);
	if(socket_fd < 0)
	{
		ERROR(PLUGIN_NAME " plugin: socket() failed for container with id %s\n",container->id);
		return NULL;
	}
	
	memset(&address, 0, sizeof(struct sockaddr_un));

	address.sun_family = AF_UNIX;
	snprintf(address.sun_path, 107, "/var/run/docker.sock");

	if(connect(socket_fd, (struct sockaddr *) &address, sizeof(struct sockaddr_un)) != 0)
	{
		ERROR(PLUGIN_NAME " plugin: connect() failed for container with id %s\n",container->id);
		return NULL;
	}
	
	nbytes = snprintf(buffer, STRING_LEN + 255, "GET /containers/%s/stats?stream=false HTTP/1.1\n\r\n",container->id);
	if (write(socket_fd, buffer, nbytes) == -1) {
		ERROR (PLUGIN_NAME " plugin: socket write() failed for container %s\n",container->id);
		goto exit;
	}

	nbytes = recv(socket_fd, response, 4999,0);
	response[nbytes] = 0;
	
	json_string = strchr(response,'{');
	if (json_string == NULL) {
		ERROR (PLUGIN_NAME " plugin: unexpected response format, when asked for container %s stats\n",container->id);
		goto exit;
	}
	cJSON* root = cJSON_Parse(json_string);
	
	submit_cpu_stats(root, container);
	
	submit_disk_stats(root, container);
	
	submit_memory_stats(root, container);
	
	submit_network_stats(root, container);
	
	exit:
	close(socket_fd);
	pthread_exit(NULL);
	return NULL;
}



static int docker_read (void)
{
	pthread_t read_containers_threads[containers_num];
	
	int i;
	for (i=0;i<containers_num;i++) {
		if(pthread_create(&read_containers_threads[i], NULL, read_container, &containers[i])) {
			ERROR(PLUGIN_NAME " plugin: error creating threads for reading containers\n");
			return -1;
		}
	}
	
	pthread_join (read_containers_threads[0], NULL);
    
	return 0;
}

static int refresh_containers () {
	struct sockaddr_un address;
	int response_bytes = 0, nbytes, socket_fd, buffer_size = 255;
	char buffer[buffer_size+1];
	char* response = malloc(1);
	char* json_string;
	response[0] = 0;

	socket_fd = socket(PF_UNIX, SOCK_STREAM, 0);
	if(socket_fd < 0){
		ERROR (PLUGIN_NAME " plugin: socket() failed\n");
		return 1;
	}
	
	memset(&address, 0, sizeof(struct sockaddr_un));

	address.sun_family = AF_UNIX;
	snprintf(address.sun_path, 107, "/var/run/docker.sock");

	if(connect(socket_fd, (struct sockaddr *) &address, sizeof(struct sockaddr_un)) != 0){
		ERROR (PLUGIN_NAME " plugin: connect() failed\n");
		return 1;
	 }
	 
	nbytes = snprintf(buffer, buffer_size, "GET /containers/json HTTP/1.1\n\r\n");
	if (write(socket_fd, buffer, nbytes) == -1) {
		ERROR (PLUGIN_NAME " plugin: write() failed\n");
		goto exit;
	}
		
	nbytes = read(socket_fd, buffer, buffer_size);
	
	while (nbytes>0) {		
		buffer[nbytes] = 0;
		response_bytes = response_bytes + nbytes;
		response = realloc(response,response_bytes+1);
		strcat(response,buffer);
		response[response_bytes] = 0;
		if (nbytes>=buffer_size) {
			nbytes = read(socket_fd, buffer, buffer_size);
		} else {
			nbytes=0;
		}
	}	
	
	json_string = strchr(response,'[');
	if (json_string == NULL) {
		ERROR (PLUGIN_NAME " plugin: unexpected response format, when asked for container list\n");
		goto exit;
	}
	cJSON* root = cJSON_Parse(json_string);
	containers_num = cJSON_GetArraySize(root);
	containers = realloc (containers, sizeof(container_t)*containers_num);

	int i;
	for (i=0;i<containers_num;i++) {
		cJSON* container = cJSON_GetArrayItem(root,i);
		strncpy(containers[i].id,cJSON_GetObjectItem(container,"Id")->valuestring,STRING_LEN);
		if (strlen(cJSON_GetObjectItem(container,"Id")->valuestring) >= STRING_LEN) {
			containers[i].id[STRING_LEN-1]=0;
		}
		strncpy(containers[i].image_name,cJSON_GetObjectItem(container,"Image")->valuestring,STRING_LEN);
		if (strlen(cJSON_GetObjectItem(container,"Image")->valuestring) >= STRING_LEN) {
			containers[i].image_name[STRING_LEN-1]=0;
		}
	}
	
	exit:
	close(socket_fd);
	return 0;
}

static int docker_shutdown (void)
{
	free(containers);

    return 0;
}

static int docker_init (void)
{
	containers= malloc (sizeof(container_t));
	containers_num = 1;
	
	refresh_containers();
		
	return 0;
}
void module_register (void)
{
	plugin_register_read (PLUGIN_NAME, docker_read);
	plugin_register_init (PLUGIN_NAME, docker_init);
	plugin_register_shutdown (PLUGIN_NAME, docker_shutdown);
    return;
}

