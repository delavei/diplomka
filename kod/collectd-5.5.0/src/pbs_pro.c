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

/*
static void init_value_list (value_list_t *vl)
{
    sstrncpy (vl->plugin, PLUGIN_NAME, sizeof (vl->plugin));

    sstrncpy (vl->host, hostname_g, sizeof (vl->host));
	vl->meta = meta_data_create();
}
* */


static int pbs_pro_config (const char *key, const char *value)
{
	host = "localhost";
    return 0;
}


static int pbs_pro_read (void)
{	
	struct batch_status *status = pbs_selstat(pbs_conn, NULL, attrs_to_select, NULL);
	struct batch_status *iter_status = status;
	int i =0;
	
	printf("iter_status  %p   chyba %s\n",iter_status, pbs_geterrmsg(pbs_conn));

	while (iter_status != NULL) {
		printf("%d\n",i++);
		fflush(stdout);
				
		printf("name %s    text %s\n",iter_status->name,iter_status->text); 
		fflush(stdout);
		
		struct attrl * iter_attribs = iter_status->attribs;
		int j =0;
		while (iter_attribs != NULL) {
			printf("name %s    resource   %s     value %s\n",iter_attribs->name,iter_attribs->resource,iter_attribs->value); 			
			iter_attribs = iter_attribs->next;
			j++;
		}
		printf ("vypisane atributyyyyyyy  %d\n",j);
		iter_status = iter_status->next;
	}
	
	pbs_statfree(status);
	
	return 0;
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
	printf("pocet atributov %d\n",i);
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
	printf("%s    %d\n",pbs_default(),pbs_conn);

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

