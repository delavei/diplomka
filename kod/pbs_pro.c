#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include <pbs_ifl.h>

/* Plugin name */
#define PLUGIN_NAME "pbs_pro"

#define STRING_LEN 128
int pbs_conn;


int main ( int argc, char **argv ) {
	pbs_conn = pbs_connect(pbs_default());
	struct attrl rattrib;
	rattrib.next = NULL;
	rattrib.resource = "";
	rattrib.value = "";
	rattrib.name = "";
	
	
	struct batch_status *status = pbs_selstat(pbs_conn, NULL,&rattrib, NULL);
	
	printf("aaaaaaaa\n");
		fflush(stdout);

	struct batch_status *iter_status = status;
	int i =0;
		printf("iiiii  %p\n",iter_status);

	while (iter_status != NULL) {
		printf("%d\n",i++);
		fflush(stdout);
		iter_status = iter_status->next;
		printf("name %s    text %s",iter_status->name,iter_status->text); 
		struct attrl * iter_attribs = iter_status->attribs;
		while (iter_attribs != NULL) {
			printf("name %s    resource   %s     value %s\n",iter_attribs->name,iter_attribs->resource,iter_attribs->value); 			
			iter_attribs = iter_attribs->next;
		}
		iter_status = iter_status->next;
		printf("\n\n"); 
	}
	printf("sssss  %p\n",iter_status);
	fflush(stdout);
	pbs_statfree(status);
	printf("uuuuu\n");
	fflush(stdout);
	
	pbs_disconnect(pbs_conn);
    return 0; // Indicates that everything went well.
}
