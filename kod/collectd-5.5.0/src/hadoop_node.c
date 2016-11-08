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
int read_info_success = -1;
long int cluster_id;

/*
static void init_value_list (value_list_t *vl)
{
    sstrncpy (vl->plugin, PLUGIN_NAME, sizeof (vl->plugin));

    sstrncpy (vl->host, hostname_g, sizeof (vl->host));
	vl->meta = meta_data_create();
	
}

void submit_cluster_value (unsigned long value, const char* type_instance, const char* tags) {
	value_t values[1];
    value_list_t vl = VALUE_LIST_INIT;

    init_value_list (&vl);

    values[0].derive = value;

    vl.values = values;
    vl.values_len = 1;

	meta_data_add_string (vl.meta,"tsdb_tags",tags);
	
    sstrncpy (vl.type, "cluster", sizeof (vl.type));
    sstrncpy (vl.type_instance, type_instance, sizeof (vl.type_instance));
    
    plugin_dispatch_values (&vl);
}

static int submit_cluster_stats (char* cluster_json) {
	cJSON* root = cJSON_Parse(cluster_json);
	
	if (root == NULL) {
		return -1;
	}
	cJSON* cluster_metrics_node = cJSON_GetObjectItem(root,"clusterMetrics");
	int apps_submitted = cJSON_GetObjectItem(cluster_metrics_node,"appsSubmitted")->valueint;
	int apps_completed = cJSON_GetObjectItem(cluster_metrics_node,"appsCompleted")->valueint;
	int apps_pending = cJSON_GetObjectItem(cluster_metrics_node,"appsPending")->valueint;
	int apps_running = cJSON_GetObjectItem(cluster_metrics_node,"appsRunning")->valueint;
	int apps_failed = cJSON_GetObjectItem(cluster_metrics_node,"appsFailed")->valueint;
	int apps_killed = cJSON_GetObjectItem(cluster_metrics_node,"appsKilled")->valueint;
	int reserved_mb = cJSON_GetObjectItem(cluster_metrics_node,"reservedMB")->valueint;
	int available_mb = cJSON_GetObjectItem(cluster_metrics_node,"availableMB")->valueint;
	int allocated_mb = cJSON_GetObjectItem(cluster_metrics_node,"allocatedMB")->valueint;
	int total_mb = cJSON_GetObjectItem(cluster_metrics_node,"totalMB")->valueint;
	int reserved_virtual_cores = cJSON_GetObjectItem(cluster_metrics_node,"reservedVirtualCores")->valueint;
	int available_virtual_cores = cJSON_GetObjectItem(cluster_metrics_node,"availableVirtualCores")->valueint;
	int allocated_virtual_cores = cJSON_GetObjectItem(cluster_metrics_node,"allocatedVirtualCores")->valueint;
	int total_virtual_cores = cJSON_GetObjectItem(cluster_metrics_node,"totalVirtualCores")->valueint;
	int total_nodes = cJSON_GetObjectItem(cluster_metrics_node,"totalNodes")->valueint;
	int active_nodes = cJSON_GetObjectItem(cluster_metrics_node,"activeNodes")->valueint;
	int lost_nodes = cJSON_GetObjectItem(cluster_metrics_node,"lostNodes")->valueint;
	int unhealthy_nodes = cJSON_GetObjectItem(cluster_metrics_node,"unhealthyNodes")->valueint;
	int decommissioned_nodes = cJSON_GetObjectItem(cluster_metrics_node,"decommissionedNodes")->valueint;
	int rebooted_nodes = cJSON_GetObjectItem(cluster_metrics_node,"rebootedNodes")->valueint;
		
	char tags[TAGS_SIZE];
	ssnprintf(tags,TAGS_SIZE,"cluster_id=%ld", cluster_id);
	
	submit_cluster_value (apps_submitted, "apps_submitted", tags);
	submit_cluster_value (apps_completed, "apps_completed", tags);
	submit_cluster_value (apps_pending, "apps_pending", tags);
	submit_cluster_value (apps_running, "apps_running", tags);
	submit_cluster_value (apps_failed, "apps_failed", tags);
	submit_cluster_value (apps_killed, "apps_killed", tags);
	submit_cluster_value (reserved_mb, "reserved_mb", tags);
	submit_cluster_value (available_mb, "available_mb", tags);
	submit_cluster_value (allocated_mb, "allocated_mb", tags);
	submit_cluster_value (total_mb, "total_mb", tags);
	submit_cluster_value (reserved_virtual_cores, "reserved_virtual_cores", tags);
	submit_cluster_value (available_virtual_cores, "available_virtual_cores", tags);
	submit_cluster_value (allocated_virtual_cores, "allocated_virtual_cores", tags);
	submit_cluster_value (total_virtual_cores, "total_virtual_cores", tags);
	submit_cluster_value (total_nodes, "total_nodes", tags);
	submit_cluster_value (active_nodes, "active_nodes", tags);
	submit_cluster_value (lost_nodes, "lost_nodes", tags);
	submit_cluster_value (unhealthy_nodes, "unhealthy_nodes", tags);
	submit_cluster_value (decommissioned_nodes, "decommissioned_nodes", tags);
	submit_cluster_value (rebooted_nodes, "rebooted_nodes", tags);
	
	cJSON_Delete(root);
	
	return 0;
}*/

size_t read_response(char *data, size_t size, size_t nmemb, void *userdata) {
	size_t retval = nmemb*size;
	
	if (new_server_read == 1) {
		new_server_read = 0;
		
		char* node_substr = strstr(data, NODE_SEARCH_SUBSTR);
		if (node_substr) {
			read_success = 0;
		} else {
			return retval;
		}
		if (data[retval]==0) {
			printf("je to nula\n");
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
		printf("tu sooom\n");
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
	CURLcode curl_ret = curl_easy_perform(easy_handle);
	if (curl_ret == CURLE_OK) {
		new_server_read = 1;
		if (read_success >= 0) {
			return 0;
		} else {
			ERROR(PLUGIN_NAME " plugin: node metrics not found in response.");
		}
	} else {
		ERROR(PLUGIN_NAME " plugin: error reading node metrics, curl errno %d.",curl_ret);
	}
	
	return 0;
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
		}
	} else {
		ERROR(PLUGIN_NAME " plugin: error reading cluster info, curl errno %d.",curl_ret);
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

