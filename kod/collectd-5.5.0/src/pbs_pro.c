#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "collectd.h"
#include "common.h"
#include "plugin.h"

#include "cJSON.h"
#include <pbs_ifl.h>

/* Plugin name */
#define PLUGIN_NAME "pbs_pro"

#define STRING_LEN 128

static const char *config_keys[] = {
    "Host",
    NULL
};
static int config_keys_num = STATIC_ARRAY_SIZE (config_keys);
int pbs_conn;
char* host;
struct attrl* attrs_to_select;

static void init_value_list (value_list_t *vl)
{
    sstrncpy (vl->plugin, PLUGIN_NAME, sizeof (vl->plugin));

    sstrncpy (vl->host, hostname_g, sizeof (vl->host));
	vl->meta = meta_data_create();
}



static void submit_long_val(unsigned long cpu_time, const char* type, const char* tags)
{
    value_t values[1];
    value_list_t vl = VALUE_LIST_INIT;

    init_value_list (&vl);

    values[0].gauge = cpu_time;

    vl.values = values;
    vl.values_len = 1;

	meta_data_add_string (vl.meta,"tsdb_tags",tags);
    sstrncpy (vl.type, type, sizeof (vl.type));

    plugin_dispatch_values (&vl);
}

void memory_submit (unsigned long value, const char* tags, const char* type_instance) {
	value_t values[1];
    value_list_t vl = VALUE_LIST_INIT;

    init_value_list (&vl);

    values[0].gauge = value;

    vl.values = values;
    vl.values_len = 1;

	meta_data_add_string (vl.meta,"tsdb_tags",tags);
	
    sstrncpy (vl.type, "memory", sizeof (vl.type));
    ssnprintf (vl.type_instance, sizeof (vl.type_instance), "%s", type_instance);
    
    plugin_dispatch_values (&vl);
}

static void make_selection_attribs(){
	int i;
	struct attrl* last_attrib=NULL;
	for (i=0;i<4;i++) {
		struct attrl* rattrib = malloc(sizeof(struct attrl));
		if (i==0) attrs_to_select = rattrib;
		rattrib->next = NULL;
		rattrib->resource = "";
		rattrib->value = "";
		switch (i) {
			case 0:
				rattrib->name = "resources_used";
				break;
			case 1:
				rattrib->name = ATTR_A; // Account_Name
				break;
			case 2:
				rattrib->name = ATTR_exechost;  // exechost
				break;
			case 3:
				rattrib->name = ATTR_owner;
				break;
		}
		
		if (last_attrib!=NULL) {
			last_attrib->next = rattrib;
		} else {
			last_attrib = rattrib;
		}
	}
}

static void free_selection_attribs(struct attrl* attrs_to_select) {
	struct attrl* attrs_iter = attrs_to_select;
	struct attrl* tmp;
	while (attrs_iter!=NULL) {
		tmp = attrs_iter->next;
		free(attrs_iter);
		attrs_iter = tmp;
	}
}
static int pbs_pro_read (void)
{	
	struct batch_status *status = pbs_selstat(pbs_conn, NULL, attrs_to_select, NULL);
	struct batch_status *iter_status = status;
		
	while (iter_status != NULL) {		
		struct attrl * iter_attribs = iter_status->attribs;
		char* job_owner = NULL;
		char tags[1024];
		
		while (iter_attribs != NULL) {
			if (strcmp(iter_attribs->name,"Job_Owner") == 0){
				job_owner = iter_attribs->value;
			}	
			iter_attribs = iter_attribs->next;
		}
		
		iter_attribs = iter_status->attribs;
		ssnprintf(tags,1024,"job_id=%s job_owner=%s",iter_status->name,job_owner);

		while (iter_attribs != NULL) {
			long value = strtol(iter_attribs->value,NULL,10);
			if (strcmp(iter_attribs->name,"resources_used") == 0
				&& strcmp(iter_attribs->resource,"cput") == 0)
				submit_long_val(value,"cpu_time",tags);
				
			if (strcmp(iter_attribs->name,"resources_used") == 0
				&& strcmp(iter_attribs->resource,"cpupercent") == 0)
				submit_long_val(value,"cpu_load",tags);
				
			if (strcmp(iter_attribs->name,"resources_used") == 0
				&& strcmp(iter_attribs->resource,"mem") == 0)
				memory_submit(value,tags,"available");
				
			if (strcmp(iter_attribs->name,"resources_used") == 0
				&& strcmp(iter_attribs->resource,"vmem") == 0)					
				submit_long_val(value,"virtual_memory",tags);
				
			if (strcmp(iter_attribs->name,"resources_used") == 0
				&& strcmp(iter_attribs->resource,"ncpus") == 0)
				submit_long_val(value,"cpu_count",tags);
				
			if (strcmp(iter_attribs->name,"resources_used") == 0
				&& strcmp(iter_attribs->resource,"walltime") == 0)
				submit_long_val(value,"walltime",tags);
				
			//printf("name %s    resource   %s     value %s\n",iter_attribs->name); 			
			iter_attribs = iter_attribs->next;
		}
		
		iter_status = iter_status->next;
	}
	
	pbs_statfree(status);
	
	return 0;
}

static int pbs_pro_config (const char *key, const char *value)
{
	host = "localhost";
    return 0;
}

static int pbs_pro_shutdown (void)
{
	pbs_disconnect(pbs_conn);
	free_selection_attribs(attrs_to_select);
    return 0;
}

static int pbs_pro_init (void)
{		
	pbs_conn = pbs_connect(pbs_default());
	make_selection_attribs();
	
	return 0;
}

void module_register (void)
{
	plugin_register_config (PLUGIN_NAME, pbs_pro_config, config_keys, config_keys_num);
	plugin_register_read (PLUGIN_NAME, pbs_pro_read);
	plugin_register_init (PLUGIN_NAME, pbs_pro_init);
	plugin_register_shutdown (PLUGIN_NAME, pbs_pro_shutdown);
    return;
}

