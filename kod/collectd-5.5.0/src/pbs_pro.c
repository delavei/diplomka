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
/*
static void init_value_list (value_list_t *vl)
{
    sstrncpy (vl->plugin, PLUGIN_NAME, sizeof (vl->plugin));

    sstrncpy (vl->host, hostname_g, sizeof (vl->host));
	vl->meta = meta_data_create();
}
*/
static int pbs_pro_config (const char *key, const char *value)
{
    return 0;
}

static int pbs_pro_read (void)
{
	return 0;
}

static int pbs_pro_shutdown (void)
{
    return 0;
}

static int pbs_pro_init (void)
{		
	pbs_connect();
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

