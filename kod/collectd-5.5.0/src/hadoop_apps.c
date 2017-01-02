#include <stdlib.h>
#include <string.h>
#include <curl/curl.h>

#include "common.h"

#include "plugin.h"
#include "cJSON.h"

#define PLUGIN_NAME "hadoop_apps"
#define BUFFER_SIZE 4096
#define TAGS_SIZE 1024
#define APP_SEARCH_SUBSTR "app\":["
#define APPS_SEARCH_SUBSTR "apps\":"
#define END_STR "]}}"
#define CLUSTER_INFO_SEARCH_SUBSTR "clusterInfo\":"

enum states {
	JSON_BEGIN_NOT_FOUND, JSON_END_NOT_FOUND
};

char* yarn_url;
char* yarn_port;

static const char *config_keys[] = {
    "YARNUrl",
    "Port"
};
static int config_keys_num = STATIC_ARRAY_SIZE (config_keys);
 
CURL* easy_handle;
int new_server_read = 1;
char app_json[BUFFER_SIZE] = "";
enum states last_state;
int read_success = -1;
int read_finished = 1;
int read_info_success = -1;

int app_count;

static void init_value_list (value_list_t *vl)
{
    sstrncpy (vl->plugin, PLUGIN_NAME, sizeof (vl->plugin));

    sstrncpy (vl->host, hostname_g, sizeof (vl->host));
	vl->meta = meta_data_create();
	
}

void submit_app_value (unsigned long value, const char* type_instance, const char* tags, const char* app_id) {
	value_t values[1];
    value_list_t vl = VALUE_LIST_INIT;

    init_value_list (&vl);

    values[0].derive = value;

    vl.values = values;
    vl.values_len = 1;

	meta_data_add_string (vl.meta,"tsdb_tags",tags);
	
    sstrncpy (vl.type, "apps", sizeof (vl.type));
    /*if too much apps, this distinguishes different app metrics and helps avoid value too old warnings */
    sstrncpy (vl.plugin_instance, app_id, sizeof (vl.type));
    sstrncpy (vl.type_instance, type_instance, sizeof (vl.type_instance));
    
    plugin_dispatch_values (&vl);
}

static int submit_app_stats (char* app_json) {
	cJSON* root = cJSON_Parse(app_json);
	app_count++;
	if (app_count>3000) return 0;
	
	if (root == NULL) {
		ERROR(PLUGIN_NAME " plugin: error when parsing app json.");
		return -1;
	}
	
	cJSON* app_id = cJSON_GetObjectItem(root,"id");
	if (app_id == NULL) {
		ERROR (PLUGIN_NAME " plugin: error getting \"id\" node from app json.");
		return -1;
	}
	cJSON* user = cJSON_GetObjectItem(root,"user");
	if (user == NULL) {
		ERROR (PLUGIN_NAME " plugin: error getting \"user\" node from app json.");
		return -1;
	}
	cJSON* app_name = cJSON_GetObjectItem(root,"name");
	if (app_name == NULL) {
		ERROR (PLUGIN_NAME " plugin: error getting \"name\" node from app json.");
		return -1;
	}
	cJSON* app_type = cJSON_GetObjectItem(root,"applicationType");
	if (app_type == NULL) {
		ERROR (PLUGIN_NAME " plugin: error getting \"applicationType\" node from app json.");
		return -1;
	}
	cJSON* cluster_id = cJSON_GetObjectItem(root,"clusterId");
	if (cluster_id == NULL) {
		ERROR (PLUGIN_NAME " plugin: error getting \"clusterId\" node from app json.");
		return -1;
	}
	
	cJSON* elapsedTime = cJSON_GetObjectItem(root,"elapsedTime");
	if (elapsedTime == NULL) {
		ERROR (PLUGIN_NAME " plugin: error getting \"elapsedTime\" node from app json.");
		return -1;
	}
	long elapsed_time = (long) elapsedTime->valuedouble;
	
	cJSON* allocatedMB = cJSON_GetObjectItem(root,"allocatedMB");
	if (allocatedMB == NULL) {
		ERROR (PLUGIN_NAME " plugin: error getting \"allocatedMB\" node from app json.");
		return -1;
	}
	int allocated_mb = allocatedMB->valueint;
	
	cJSON* allocatedVCores = cJSON_GetObjectItem(root,"allocatedVCores");
	if (allocatedVCores == NULL) {
		ERROR (PLUGIN_NAME " plugin: error getting \"allocatedVCores\" node from app json.");
		return -1;
	}
	int allocated_vcores = allocatedVCores->valueint;
	
	cJSON* runningContainers = cJSON_GetObjectItem(root,"runningContainers");
	if (runningContainers == NULL) {
		ERROR (PLUGIN_NAME " plugin: error getting \"runningContainers\" node from app json.");
		return -1;
	}
	int running_containers = runningContainers->valueint;
	
	cJSON* memorySeconds = cJSON_GetObjectItem(root,"memorySeconds");
	if (memorySeconds == NULL) {
		ERROR (PLUGIN_NAME " plugin: error getting \"memorySeconds\" node from app json.");
		return -1;
	}
	long memory_seconds = (long) memorySeconds->valuedouble;
	
	cJSON* vcoreSeconds = cJSON_GetObjectItem(root,"vcoreSeconds");
	if (vcoreSeconds == NULL) {
		ERROR (PLUGIN_NAME " plugin: error getting \"vcoreSeconds\" node from app json.");
		return -1;
	}
	long vcore_seconds = (long) vcoreSeconds->valuedouble;
	
	char tags[TAGS_SIZE];
	int written_chars = ssnprintf(tags,TAGS_SIZE,"app_id=%s user=%s app_type=%s cluster_id=%ld app_name=%s",
	app_id->valuestring, user->valuestring, app_type->valuestring, (long int) cluster_id->valuedouble, app_name->valuestring);
	strncat(tags,app_name->valuestring,TAGS_SIZE-written_chars);
		
	submit_app_value (elapsed_time, "elapsed_time", tags, app_id->valuestring);
	submit_app_value (allocated_mb, "allocated_mb", tags, app_id->valuestring);
	submit_app_value (allocated_vcores, "allocated_vcores", tags, app_id->valuestring);
	submit_app_value (running_containers, "running_containers", tags, app_id->valuestring);
	submit_app_value (memory_seconds, "memory_seconds", tags, app_id->valuestring);
	submit_app_value (vcore_seconds, "vcore_seconds", tags, app_id->valuestring);
	
	cJSON_Delete(root);
	
	return 0;
}
	
enum states parse_response (char *app_json_begin, char *data_end) {
	char *app_json_end = 0;
	int app_json_length, length_to_end;
	
	if (last_state == JSON_BEGIN_NOT_FOUND)	app_json_begin = strchr(app_json_begin, '{');
	
	while(app_json_begin) {
		app_json_end = strchr(app_json_begin,'}');
		
		if (!app_json_end) {
			length_to_end = data_end - app_json_begin;
			if (length_to_end + strlen (app_json) >= BUFFER_SIZE) {
				ERROR(PLUGIN_NAME " plugin: app json string exceeded buffer");
			} else {
				strncat(app_json,app_json_begin, length_to_end);			
				return JSON_END_NOT_FOUND;
			}			
		} else {
			if (strlen(app_json_end) >= 3) {
				if ((app_json_end[1] == ']') && (app_json_end[2]=='}')) {
					read_finished = 1;
				}
			}
			
			app_json_length = app_json_end - app_json_begin + 1;

			if (app_json_length + strlen (app_json) >= BUFFER_SIZE) {
				ERROR(PLUGIN_NAME " plugin: app json string exceeded buffer");
			} else {
				strncat(app_json, app_json_begin, app_json_length);
				if (submit_app_stats(app_json) != 0) {
					read_success = -1;
				}
				app_json[0]=0;
			}
		}
		app_json_begin = strchr(app_json_end,'{');
	}

	return JSON_BEGIN_NOT_FOUND;
}

size_t read_response(char *data, size_t size, size_t nmemb, void *userdata) {
	char* app_json_begin = 0;
	size_t retval = nmemb*size;
				
	if (new_server_read) {
		new_server_read = 0;
				
		char* apps_substr = strstr(data, APPS_SEARCH_SUBSTR);
		if (apps_substr != NULL) {
			read_success = 0;
		} else {
			return retval;
		}

		app_json_begin = strstr(data, APP_SEARCH_SUBSTR);
		
		if (!app_json_begin) {
			return retval;
		}
		
		app_json_begin += strlen(APP_SEARCH_SUBSTR);
		
		last_state = parse_response(app_json_begin, data+retval);
	} else {
		app_json_begin = data;
		
		switch (last_state) {
			case JSON_END_NOT_FOUND:
				last_state = parse_response(app_json_begin, data+retval);
				break;
			case JSON_BEGIN_NOT_FOUND:
				last_state = parse_response(app_json_begin, data+retval);
				break;
		}
	}
	
	return retval;
}

static int hadoop_apps_read (void)
{
	app_count = 0;
	printf("app count before    %d\n",app_count);
	if (read_finished) {
		read_finished = 0;
		CURLcode curl_ret = curl_easy_perform(easy_handle);
		if (curl_ret == CURLE_OK) {
			new_server_read = 1;
			if (read_success >= 0) {
				printf("app count    %d\n",app_count);
	app_count = 0;
				return 0;
			} else {
				ERROR(PLUGIN_NAME " plugin: apps not found in response.");
			}
		} else {
			ERROR(PLUGIN_NAME " plugin: error reading apps stats, curl errno %d.",curl_ret);
		}
	} else {
		return 0;
	}
	
	
	
	return -1;	
}

static int hadoop_apps_shutdown (void)
{
	curl_easy_cleanup(easy_handle);  
    return 0;
}

size_t read_response_cluster_info(char *data, size_t size, size_t nmemb, void *userdata) {
	size_t retval = nmemb*size;
	char* cluster_info_substr = strstr(data, CLUSTER_INFO_SEARCH_SUBSTR);
	
	if (cluster_info_substr) {
		cJSON* root = cJSON_Parse(data);

		if (root == NULL) {
			return retval;
		} else {
			cJSON* clusterInfo = cJSON_GetObjectItem(root,"clusterInfo");
			if (clusterInfo == NULL) {
				ERROR (PLUGIN_NAME " plugin: error getting \"clusterInfo\" node cluster json.");
				return retval;
			}
			read_info_success = 0;
		}
	}
	
	return retval;
}

static int hadoop_apps_init (void)
{		
	easy_handle = curl_easy_init();
	
	char apps_url[256];	
	snprintf(apps_url, 256, "%s:%s/ws/v1/cluster/info", yarn_url, yarn_port);
	
	curl_easy_setopt(easy_handle, CURLOPT_URL, apps_url);
	curl_easy_setopt(easy_handle, CURLOPT_HTTPAUTH, (long)CURLAUTH_GSSNEGOTIATE);
	curl_easy_setopt(easy_handle, CURLOPT_WRITEFUNCTION, read_response);
    curl_easy_setopt(easy_handle, CURLOPT_USERAGENT, "curl/7.50.1");	
	curl_easy_setopt(easy_handle, CURLOPT_USERPWD, ":");        
    curl_easy_setopt(easy_handle, CURLOPT_TCP_KEEPALIVE, 1L);
    curl_easy_setopt(easy_handle, CURLOPT_NOPROGRESS, 1L);
    curl_easy_setopt(easy_handle, CURLOPT_FOLLOWLOCATION, "true");
    curl_easy_setopt(easy_handle, CURLOPT_MAXREDIRS, 50L);

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
	
	snprintf(apps_url, 256, "%s:%s/ws/v1/cluster/apps?states=finished", yarn_url, yarn_port);
	curl_easy_setopt(easy_handle, CURLOPT_URL, apps_url);
	curl_easy_setopt(easy_handle, CURLOPT_WRITEFUNCTION, read_response);


	return 0;
}

static int hadoop_apss_config (const char *key, const char *value)
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
	plugin_register_read (PLUGIN_NAME, hadoop_apps_read);
	plugin_register_config(PLUGIN_NAME, hadoop_apss_config, config_keys, config_keys_num);
	plugin_register_init (PLUGIN_NAME, hadoop_apps_init);
	plugin_register_shutdown (PLUGIN_NAME, hadoop_apps_shutdown);
    return;
}

