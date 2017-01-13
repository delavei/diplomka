#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <float.h>

#include "collectd.h"
#include "common.h"
#include "plugin.h"
#include "cJSON.h"

/* Plugin name */
#define PLUGIN_NAME "docker"

#define STRING_LEN 128

static const char *config_keys[] = {
    "RefreshInterval",
    NULL
};

static int config_keys_num = STATIC_ARRAY_SIZE (config_keys);

static time_t last_refresh = (time_t) 0;
static int refresh_interval = 60;

typedef struct container_s {
	char id[STRING_LEN];
	char image_name[STRING_LEN];
	double_t last_cpu_time;
	double_t last_container_time;
	int init;
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

static int docker_config (const char *key, const char *value)
{
    if (strcasecmp (key, "RefreshInterval") == 0) {
        char *eptr = NULL;
        refresh_interval = strtol (value, &eptr, 10);
        if (eptr == NULL || *eptr != '\0') return 1;
        return 0;
    }

    /* Unrecognised option. */
    return -1;
}

static void cpu_time_submit (unsigned long cpu_time, container_t* container)
{
    value_t values[1];
    value_list_t vl = VALUE_LIST_INIT;

    init_value_list (&vl);

    values[0].gauge = cpu_time;

    vl.values = values;
    vl.values_len = 1;

	char tags[1024];
    ssnprintf(tags,1024,"container_id=%s image_name=%s",container->id,container->image_name);
	meta_data_add_string (vl.meta,"tsdb_tags",tags);
	
    sstrncpy (vl.type, "cpu_time", sizeof (vl.type));

    plugin_dispatch_values (&vl);
}

static void cpu_load_submit(double_t cpu_load_percent, container_t* container)
{
    value_t values[1];
    value_list_t vl = VALUE_LIST_INIT;

    init_value_list (&vl);

    values[0].gauge = cpu_load_percent;

    vl.values = values;
    vl.values_len = 1;

	char tags[1024];
    ssnprintf(tags,1024,"container_id=%s image_name=%s",container->id,container->image_name);
	meta_data_add_string (vl.meta,"tsdb_tags",tags);
	
    sstrncpy (vl.type, "cpu_load", sizeof (vl.type));

    plugin_dispatch_values (&vl);
}

double_t compute_container_load(double_t container_time_in_secs, container_t* container) {
	long int user = 0;
	long int nice = 0;
	long int system = 0;
	long int idle = 0;
	long int iowait = 0;
	long int irq = 0;
	long int softirq = 0;
	long int steal = 0;
	long int guest = 0;
	long int guest_nice = 0;

	FILE* stat_file = fopen ("/proc/stat", "r");
	if(stat_file != NULL){
		int items_scanned = fscanf(stat_file, "cpu  %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld", &user, &nice, &system, &idle, &iowait, &irq, &softirq, &steal, &guest, &guest_nice);
		if (items_scanned <10) {
			ERROR (PLUGIN_NAME " plugin: error parsing /proc/stat file");
		}
		fclose(stat_file);
	} else {
		ERROR (PLUGIN_NAME " plugin: error opening /proc/stat file");
		return -1.0;
	}
	
	long int cpu_time_sum = user+nice+system+idle+iowait+irq+softirq+steal+guest+guest_nice;
	double_t cpu_time_in_secs = (double_t) (cpu_time_sum/sysconf(_SC_CLK_TCK));

	if (container->init == 0) {
		container->last_cpu_time = cpu_time_in_secs;
		container->last_container_time = container_time_in_secs;
		container->init = 1;
	}
	
	double_t delta_container_time = container_time_in_secs - container->last_container_time;
	double_t delta_cpu_time = cpu_time_in_secs - container->last_cpu_time;
	
	container->last_container_time = container_time_in_secs;
	container->last_cpu_time = cpu_time_in_secs;

	double_t container_load = 0;
	if (delta_cpu_time != 0) {
		container_load = delta_container_time/delta_cpu_time*100;
	}
		
	return container_load;
}

void submit_cpu_stats (cJSON* root, container_t* container){
	cJSON* cpu_stats = cJSON_GetObjectItem(root,"cpu_stats");
	if (cpu_stats == NULL) {
		ERROR (PLUGIN_NAME " plugin: error getting \"cpu_stats\" node from container json.");
		return;
	}
	cJSON* cpu_usage = cJSON_GetObjectItem(cpu_stats,"cpu_usage");
	if (cpu_usage == NULL) {
		ERROR (PLUGIN_NAME " plugin: error getting \"cpu_usage\" node from container json.");
		return;
	}
	cJSON* total_usage = cJSON_GetObjectItem(cpu_usage,"total_usage");
	if (cpu_usage == NULL) {
		ERROR (PLUGIN_NAME " plugin: error getting \"total_usage\" node from container json.");
		return;
	}
		
	unsigned long container_cpu_time = (unsigned long) total_usage->valuedouble;

	cpu_time_submit (container_cpu_time, container);
	double_t container_time_in_secs = (double_t) (container_cpu_time/1000000000);

	double_t container_load = compute_container_load(container_time_in_secs, container);
	
	if (container_load < 0) {
		return;
	}
	
	cpu_load_submit (container_load, container);
}

static void get_disk_stats (cJSON* root, unsigned long* write_val, unsigned long* read_val, enum disk_switch data_type) {
	cJSON* disk_stats_array = NULL;
	cJSON* blkio_stats = cJSON_GetObjectItem(root,"blkio_stats");
	if (blkio_stats == NULL) {
		ERROR (PLUGIN_NAME " plugin: error getting \"blkio_stats\" node from container json.");
		return;
	}
	switch (data_type) {
		case OPS:
			disk_stats_array = cJSON_GetObjectItem(blkio_stats,"io_serviced_recursive");
			if (disk_stats_array == NULL) {
				ERROR (PLUGIN_NAME " plugin: error getting \"io_serviced_recursive\" node from container json.");
				return;
			}
			break;
		case BYTES:
			disk_stats_array = cJSON_GetObjectItem(blkio_stats,"io_service_bytes_recursive");
			if (disk_stats_array == NULL) {
				ERROR (PLUGIN_NAME " plugin: error getting \"io_service_bytes_recursive\" node from container json.");
				return;
			}
			break;
	}
	
	 
	int i;
	int disk_stats_num = cJSON_GetArraySize(disk_stats_array);
	
	cJSON* tmp;
	for (i=0;i<disk_stats_num;i++) {
		tmp = cJSON_GetArrayItem(disk_stats_array,i);
		if (tmp == NULL) {
			ERROR (PLUGIN_NAME " plugin: error getting io read-write stats node from container json for disk number.");
			return;
		}
		cJSON* op = cJSON_GetObjectItem(tmp,"op");
		if (op == NULL) {
			ERROR (PLUGIN_NAME " plugin: error getting \"op\" node from container json.");
			return;
		}
		cJSON* value = cJSON_GetObjectItem(tmp,"value");
		if (value == NULL) {
			ERROR (PLUGIN_NAME " plugin: error getting \"value\" node from container json (disk info).");
			return;
		}
		if (!strcmp(op->valuestring,"Read")) {
			*read_val = value->valueint;
		}
		if (!strcmp(op->valuestring,"Write")) {
			*write_val = value->valueint;
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
	cJSON* memory_stats = cJSON_GetObjectItem(root,"memory_stats");
	if (memory_stats == NULL) {
		ERROR (PLUGIN_NAME " plugin: error getting \"memory_stats\" node from container json.");
		return;
	}
	
	cJSON* usage = cJSON_GetObjectItem(memory_stats,"usage");
	if (usage == NULL) {
		ERROR (PLUGIN_NAME " plugin: error getting \"usage\" node from container json.");
		return;
	}
	unsigned long mem_value = (unsigned long) usage->valuedouble;
	memory_submit(mem_value,container,"usage");
	
	cJSON* failcnt = cJSON_GetObjectItem(memory_stats,"failcnt");
	if (failcnt == NULL) {
		ERROR (PLUGIN_NAME " plugin: error getting \"failcnt\" node from container json.");
		return;
	}
	mem_value = (unsigned long) failcnt->valuedouble;
	memory_submit(mem_value,container,"fail_count");
		
	cJSON* limit = cJSON_GetObjectItem(memory_stats,"limit");
	if (limit == NULL) {
		ERROR (PLUGIN_NAME " plugin: error getting \"limit\" node from container json.");
		return;
	}
	mem_value = (unsigned long) limit->valuedouble;
	memory_submit(mem_value,container,"limit");
	
	cJSON* stats = cJSON_GetObjectItem(memory_stats,"stats");
	if (stats == NULL) {
		ERROR (PLUGIN_NAME " plugin: error getting \"stats\" node from container json.");
		return;
	}
	
	cJSON* rss = cJSON_GetObjectItem(stats,"rss");
	if (rss == NULL) {
		ERROR (PLUGIN_NAME " plugin: error getting \"rss\" node from container json.");
		return;
	}
	mem_value = (unsigned long) rss->valuedouble;
	memory_submit(mem_value,container,"rss");
	
	cJSON* pgfault_major = cJSON_GetObjectItem(stats,"pgmajfault");
	if (pgfault_major == NULL) {
		ERROR (PLUGIN_NAME " plugin: error getting \"pgmajfault\" node from container json.");
		return;
	}
	mem_value = pgfault_major->valueint;
	memory_submit(mem_value,container,"pgfault_major");
	
	cJSON* pgfault = cJSON_GetObjectItem(stats,"pgfault");
	if (pgfault == NULL) {
		ERROR (PLUGIN_NAME " plugin: error getting \"pgfault\" node from container json.");
		return;
	}
	mem_value = pgfault->valueint;
	memory_submit(mem_value,container,"pgfault_minor");	
}

static int get_network_stats (cJSON* network, unsigned long* rx_val, unsigned long* tx_val, const char* rx_string, const char* tx_string) {
	cJSON* rx = cJSON_GetObjectItem(network,rx_string);
	if (rx == NULL) {
		ERROR (PLUGIN_NAME " plugin: error getting \"rx\" node from container json.");
		return -1;
	}
	*rx_val = (unsigned long) rx->valuedouble;
	cJSON* tx = cJSON_GetObjectItem(network,tx_string);
	if (tx == NULL) {
		ERROR (PLUGIN_NAME " plugin: error getting \"tx\" node from container json.");
		return -1;
	}
	*tx_val = (unsigned long) tx->valuedouble;
	
	return 0;
}

void submit_network_stats (cJSON* root, container_t* container) {
	unsigned long rx = 0, tx = 0;
	cJSON* networks = cJSON_GetObjectItem(root,"networks");
	if (networks == NULL) {
		ERROR (PLUGIN_NAME " plugin: error getting \"networks\" node from container json.");
		return;
	}
	cJSON* network = networks->child;
	while (network != NULL) {
		if (get_network_stats (network, &rx, &tx, "rx_dropped","tx_dropped") == 0) {
			submit_derive2 ("if_dropped", (derive_t) rx, (derive_t) tx, container);
		}		
		if (get_network_stats (network, &rx, &tx, "rx_bytes","tx_bytes") == 0) {
			submit_derive2 ("if_octets", (derive_t) rx, (derive_t) tx, container);
		}
		if (get_network_stats (network, &rx, &tx, "rx_packets","tx_packets") == 0) {
			submit_derive2 ("if_packets", (derive_t) rx, (derive_t) tx, container);
		}
		if (get_network_stats (network, &rx, &tx, "rx_errors","tx_errors") == 0) {
			submit_derive2 ("if_errors", (derive_t) rx, (derive_t) tx, container);
		}		
		network = network->next;
	}
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
	
	nbytes = snprintf(buffer, STRING_LEN + 255, "GET /containers/%s/stats?stream=false HTTP/1.1\r\nHost: localhost\r\n\r\n",container->id);
	if (write(socket_fd, buffer, nbytes) == -1) {
		ERROR (PLUGIN_NAME " plugin: socket write() failed for container %s\n",container->id);
		goto exit;
	}

	nbytes = recv(socket_fd, response, 4999,0);
	response[nbytes] = 0;
	close(socket_fd);

	json_string = strchr(response,'{');
	if (json_string == NULL) {
		ERROR (PLUGIN_NAME " plugin: unexpected response format, when asked for container %s stats\n",container->id);
		goto exit;
	}
	cJSON* root = cJSON_Parse(json_string);
	if (root == NULL) {
		ERROR (PLUGIN_NAME " plugin: error parsing container json.");
		goto exit;
	}
	
	submit_cpu_stats(root, container);
	
	submit_disk_stats(root, container);
	
	submit_memory_stats(root, container);
	
	submit_network_stats(root, container);
	
	cJSON_Delete(root);
	
	exit:
	pthread_exit(NULL);
	return NULL;
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
		return -1;
	}
	
	memset(&address, 0, sizeof(struct sockaddr_un));

	address.sun_family = AF_UNIX;
	snprintf(address.sun_path, 107, "/var/run/docker.sock");

	if(connect(socket_fd, (struct sockaddr *) &address, sizeof(struct sockaddr_un)) != 0){
		ERROR (PLUGIN_NAME " plugin: connect() failed\n");
		return -1;
	 }
	 
	nbytes = snprintf(buffer, buffer_size, "GET /containers/json HTTP/1.1\r\nHost: localhost\r\n\r\n");
	if (write(socket_fd, buffer, nbytes) == -1) {
		ERROR (PLUGIN_NAME " plugin: write() failed\n");
		close(socket_fd);
		return -1;
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
	close(socket_fd);
	
	json_string = strchr(response,'[');
	if (json_string == NULL) {
		ERROR (PLUGIN_NAME " plugin: unexpected response format, when asked for container list.");
		return -1;
	}
	cJSON* root = cJSON_Parse(json_string);
	if (root == NULL) {
		ERROR (PLUGIN_NAME " plugin: error parsing containers json.");
		return -1;
	}
	containers_num = cJSON_GetArraySize(root);
	containers = realloc (containers, sizeof(container_t)*containers_num);

	if ((containers == NULL) && (containers_num != 0)) {
		ERROR (PLUGIN_NAME " plugin: realloc failed when refreshing containers.");
		return -1;
	}
	
	int i;
	for (i=0;i<containers_num;i++) {
		cJSON* container = cJSON_GetArrayItem(root,i);
		if (container == NULL) {
			ERROR (PLUGIN_NAME " plugin: error getting \"container\" node from json.");
			return -1;
		}
		cJSON* id = cJSON_GetObjectItem(container,"Id");
		if (container == NULL) {
			ERROR (PLUGIN_NAME " plugin: error getting \"Id\" node from container json.");
			return -1;
		}
		
		strncpy(containers[i].id,id->valuestring,STRING_LEN);
		if (strlen(id->valuestring) >= STRING_LEN) {
			containers[i].id[STRING_LEN-1]=0;
		}
		
		cJSON* image = cJSON_GetObjectItem(container,"Image");
		if (container == NULL) {
			ERROR (PLUGIN_NAME " plugin: error getting \"Image\" node from container json.");
			return -1;
		}
		
		strncpy(containers[i].image_name,image->valuestring,STRING_LEN);
		if (strlen(image->valuestring) >= STRING_LEN) {
			containers[i].image_name[STRING_LEN-1]=0;
		}
		containers[i].init = 0;
	}
	
	return 0;
}

static int docker_read (void)
{
	time_t t;
    time (&t);
    
    /* Need to refresh domain or device lists? */
    if ((last_refresh == (time_t) 0) ||
            ((refresh_interval > 0) && ((last_refresh + refresh_interval) <= t))) {
        if (refresh_containers() != 0) {
            return -1;
        }
        last_refresh = t;
    }

	if (containers_num != 0) { 
		pthread_t read_containers_threads[containers_num];
		
		int i;
		for (i=0;i<containers_num;i++) {
			if(pthread_create(&read_containers_threads[i], NULL, read_container, &containers[i])) {
				ERROR(PLUGIN_NAME " plugin: error creating threads for reading containers\n");
				return -1;
			}
		}
		
		pthread_join (read_containers_threads[0], NULL);
	}
	return 0;
}

static int docker_shutdown (void)
{
	free(containers);
	containers_num = 0;

    return 0;
}

static int docker_init (void)
{
	containers = malloc (sizeof(container_t));
	containers_num = 1;
		
	return 0;
}

void module_register (void)
{
	plugin_register_config (PLUGIN_NAME, docker_config, config_keys, config_keys_num);
	plugin_register_read (PLUGIN_NAME, docker_read);
	plugin_register_init (PLUGIN_NAME, docker_init);
	plugin_register_shutdown (PLUGIN_NAME, docker_shutdown);
    return;
}

