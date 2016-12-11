#include <stdlib.h>
#include <string.h>
#include <curl/curl.h>

#include "common.h"

#include "plugin.h"
#include "cJSON.h"

#define PLUGIN_NAME "hadoop_node"
#define TAGS_SIZE 1024
#define NODE_SEARCH_SUBSTR "{\"node\":"
#define CLUSTER_INFO_SEARCH_SUBSTR "clusterInfo\":"

char* yarn_url;
char* yarn_port;

static const char *config_keys[] = {
    "YARNUrl",
    "Port"
};
static int config_keys_num = STATIC_ARRAY_SIZE (config_keys);
 
char* nodes_json = NULL;
int nodes_json_length = 0;
CURL* easy_handle;
int new_server_read = 1;
int read_success = -1;
int read_finished = 1;
int read_info_success = -1;
long int cluster_id;


static void init_value_list (value_list_t *vl)
{
    sstrncpy (vl->plugin, PLUGIN_NAME, sizeof (vl->plugin));

	vl->meta = meta_data_create();
	
}

void submit_node_value (unsigned long value, const char* type_instance, const char* tags, const char* node_id, const char* node_hostname) {
	value_t values[1];
    value_list_t vl = VALUE_LIST_INIT;

    init_value_list (&vl);

    values[0].derive = value;

    vl.values = values;
    vl.values_len = 1;

	meta_data_add_string (vl.meta,"tsdb_tags",tags);
	
	sstrncpy (vl.host, node_hostname, sizeof (vl.host));
    sstrncpy (vl.type, "node", sizeof (vl.type));
    /*if too much nodes, this distinguishes different nodes and helps avoid value too old warnings */
    sstrncpy (vl.plugin_instance, node_id, sizeof (vl.type));
    sstrncpy (vl.type_instance, type_instance, sizeof (vl.type_instance));
    
    plugin_dispatch_values (&vl);
}

static int submit_node_stats (char* nodes_json) {
	cJSON* root = cJSON_Parse(nodes_json);
	read_finished = 1;

	if (root == NULL) {
		ERROR(PLUGIN_NAME " plugin: error parsing nodes json.");
		return -1;
	}
	cJSON* nodes = NULL;
	nodes = cJSON_GetObjectItem(root,"nodes");
	if (nodes == NULL) {
		ERROR(PLUGIN_NAME " plugin: \"nodes\" node not found when parsing response.");
		return -1;
	}
	cJSON* node = NULL;
	node = cJSON_GetObjectItem(nodes,"node");
	if (node == NULL) {
		ERROR(PLUGIN_NAME " plugin: \"node\" node not found when parsing response.");
		return -1;
	}
	
	int nodes_num = cJSON_GetArraySize(node);
	int i;
	for (i=0;i<nodes_num;i++) {
		cJSON* node_stats = cJSON_GetArrayItem(node,i);
		if (node_stats == NULL) {
			ERROR(PLUGIN_NAME " plugin: unable to get node statistics json for node %d, when parsing node stats.",i);
			return -1;
		}
		cJSON* usedMemoryMB = cJSON_GetObjectItem(node_stats,"usedMemoryMB");
		if (usedMemoryMB == NULL) {
			ERROR(PLUGIN_NAME " plugin: \"usedMemoryMB\" node not found when parsing node stats.");
			return -1;
		}
		long int used_memory_mb = (long int) usedMemoryMB->valuedouble;

		cJSON* availMemoryMB = cJSON_GetObjectItem(node_stats,"availMemoryMB");
		if (availMemoryMB == NULL) {
			ERROR(PLUGIN_NAME " plugin: \"availMemoryMB\" node not found when parsing node stats.");
			return -1;
		}
		long int available_memory_mb = (long int) availMemoryMB->valuedouble;
		
		cJSON* usedVirtualCores = cJSON_GetObjectItem(node_stats,"usedVirtualCores");
		if (usedVirtualCores == NULL) {
			ERROR(PLUGIN_NAME " plugin: \"usedVirtualCores\" node not found when parsing node stats.");
			return -1;
		}
		int used_virtual_cores = usedVirtualCores->valueint;
		
		cJSON* availableVirtualCores = cJSON_GetObjectItem(node_stats,"availableVirtualCores");
		if (availableVirtualCores == NULL) {
			ERROR(PLUGIN_NAME " plugin: \"availableVirtualCores\" node not found when parsing node stats.");
			return -1;
		}
		int available_virtual_cores = availableVirtualCores->valueint;
		
		cJSON* numContainers = cJSON_GetObjectItem(node_stats,"numContainers");
		if (numContainers == NULL) {
			ERROR(PLUGIN_NAME " plugin: \"numContainers\" node not found when parsing node stats.");
			return -1;
		}
		int containers_running = numContainers->valueint;

		cJSON* rack_node = cJSON_GetObjectItem(node_stats,"rack");
		if (rack_node == NULL) {
			ERROR(PLUGIN_NAME " plugin: \"rack\" node not found when parsing node stats.");
			return -1;
		}
		char* rack = rack_node->valuestring;
		
		cJSON* nodeHostName = cJSON_GetObjectItem(node_stats,"nodeHostName");
		if (nodeHostName == NULL) {
			ERROR(PLUGIN_NAME " plugin: \"nodeHostName\" node not found when parsing node stats.");
			return -1;
		}
		char* node_hostname = nodeHostName->valuestring;
		
		cJSON* id = cJSON_GetObjectItem(node_stats,"id");
		if (id == NULL) {
			ERROR(PLUGIN_NAME " plugin: \"id\" node not found when parsing node stats.");
			return -1;
		}
		char* node_id = id->valuestring;
		
		char tags[TAGS_SIZE];
		ssnprintf(tags,TAGS_SIZE,"cluster_id=%ld node_id=%s rack=%s", cluster_id, node_id, rack);
		
		submit_node_value (used_memory_mb, "used_memory_mb", tags, node_id, node_hostname);
		submit_node_value (available_memory_mb, "available_memory_mb", tags, node_id, node_hostname);
		submit_node_value (used_virtual_cores, "used_virtual_cores", tags, node_id, node_hostname);
		submit_node_value (available_virtual_cores, "available_virtual_cores", tags, node_id, node_hostname);
		submit_node_value (containers_running, "containers_running", tags, node_id, node_hostname);
	}
	
	cJSON_Delete(root);
	return 0;
}

size_t read_response(char *data, size_t size, size_t nmemb, void *userdata) {
	size_t retval = nmemb*size;
	if (new_server_read == 1) {
		new_server_read = 0;
		
		char* node_substr = strstr(data, NODE_SEARCH_SUBSTR);
		if (!node_substr) {
			ERROR(PLUGIN_NAME " plugin: \"nodes\" not found in response.");
			return retval;
		}
				
		nodes_json = malloc(sizeof(char)*(retval+1));
		if (!nodes_json) {
			ERROR(PLUGIN_NAME " plugin: malloc failed for nodes json string.");
			return -1;
		}
		
		nodes_json_length = retval;
		nodes_json[0]=0;
		strncat (nodes_json, data, nodes_json_length);
		nodes_json[nodes_json_length]=0;
	} else {
		int new_size = nodes_json_length+retval;
		char* tmp = realloc(nodes_json, sizeof(char)*new_size);
		if (!tmp) {
			ERROR(PLUGIN_NAME " plugin: realloc failed for nodes json string.");
			return -1;
		} else {
			nodes_json = tmp;
			nodes_json_length = new_size;
			strncat (nodes_json, data, nodes_json_length);
			nodes_json[nodes_json_length] = 0;
		}
	}
	
	if ((nodes_json[nodes_json_length-4] == '}') &&
		(nodes_json[nodes_json_length-3] == ']') &&
		(nodes_json[nodes_json_length-2] == '}') &&
		(nodes_json[nodes_json_length-1] == '}')) {
			read_success = submit_node_stats(nodes_json); 
	} 
	
	return retval;
}

size_t read_response_cluster_info(char *data, size_t size, size_t nmemb, void *userdata) {
	size_t retval = nmemb*size;
	char* cluster_info_substr = strstr(data, CLUSTER_INFO_SEARCH_SUBSTR);
	
	if (cluster_info_substr) {
		cJSON* root = cJSON_Parse(data);

		if (root == NULL) {
			return retval;
		} else {
			cluster_id = (long int) cJSON_GetObjectItem(cJSON_GetObjectItem(root,"clusterInfo"),"id")->valuedouble;
			read_info_success = 0;
		}
	}
	
	return retval;
}

static int hadoop_node_read (void)
{
	if (read_finished) {
		read_success = -1;
		new_server_read = 1;

		CURLcode curl_ret = curl_easy_perform(easy_handle);
		if (curl_ret != CURLE_OK) {
			ERROR(PLUGIN_NAME " plugin: error reading node metrics, curl errno %d.",curl_ret);
			return -1;
		}
	} else {
		return 0;
	}
	
	return read_success;
}

static int hadoop_node_shutdown (void)
{
	curl_easy_cleanup(easy_handle);  
    return 0;
}

static int hadoop_node_init (void)
{		
	easy_handle = curl_easy_init();
	curl_easy_setopt(easy_handle, CURLOPT_HTTPAUTH, (long)CURLAUTH_GSSNEGOTIATE);
	curl_easy_setopt(easy_handle, CURLOPT_WRITEFUNCTION, read_response);
    curl_easy_setopt(easy_handle, CURLOPT_USERAGENT, "curl/7.50.1");	
	curl_easy_setopt(easy_handle, CURLOPT_USERPWD, ":");        
    curl_easy_setopt(easy_handle, CURLOPT_TCP_KEEPALIVE, 1L);
    curl_easy_setopt(easy_handle, CURLOPT_NOPROGRESS, 1L);
    curl_easy_setopt(easy_handle, CURLOPT_FOLLOWLOCATION, "true");
    curl_easy_setopt(easy_handle, CURLOPT_MAXREDIRS, 50L);
	
	char cluster_url[256];
	snprintf(cluster_url, 256, "%s:%s/ws/v1/cluster/info", yarn_url, yarn_port);
	curl_easy_setopt(easy_handle, CURLOPT_URL, cluster_url);
	curl_easy_setopt(easy_handle, CURLOPT_WRITEFUNCTION, read_response_cluster_info);
	
	CURLcode curl_ret = curl_easy_perform(easy_handle);
	if (curl_ret == CURLE_OK) {
		if (read_info_success != 0) {
			ERROR(PLUGIN_NAME " plugin: cluster info not found in response.");
			return -1;
		}
	} else {
		ERROR(PLUGIN_NAME " plugin: error reading cluster info, curl errno %d.",curl_ret);
		return -1;
	}
	
	snprintf(cluster_url, 256, "%s:%s/ws/v1/cluster/nodes", yarn_url, yarn_port);
	curl_easy_setopt(easy_handle, CURLOPT_URL, cluster_url);
	curl_easy_setopt(easy_handle, CURLOPT_WRITEFUNCTION, read_response);

	return 0;
}

static int hadoop_node_config (const char *key, const char *value)
{
    if (strcasecmp (key, "YARNUrl") == 0) {
        yarn_url = strdup (value);
        if (yarn_url == NULL) {
            ERROR (PLUGIN_NAME " plugin: YARNUrl strdup failed.");
            return -1;
        }
    }
	
	if (strcasecmp (key, "Port") == 0) {
        yarn_port = strdup (value);
        if (yarn_port == NULL) {
            ERROR (PLUGIN_NAME " plugin: Port strdup failed.");
            return -1;
        }
        return 0;
    }

    /* Unrecognised option. */
    return -1;
}

void module_register (void)
{
	plugin_register_read (PLUGIN_NAME, hadoop_node_read);
	plugin_register_config(PLUGIN_NAME, hadoop_node_config, config_keys, config_keys_num);
	plugin_register_init (PLUGIN_NAME, hadoop_node_init);
	plugin_register_shutdown (PLUGIN_NAME, hadoop_node_shutdown);
    return;
}

