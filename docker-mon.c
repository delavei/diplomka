// gcc docker-mon.c -o docker-mon -lpthread

#include <stdio.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <string.h>
#include <pthread.h>
void *cont1_collect () {
	struct sockaddr_un address;
	int nbytes,socket_fd;
	char buffer[256];
	char response[5000];

	socket_fd = socket(PF_UNIX, SOCK_STREAM, 0);
	if(socket_fd < 0)
	{
		fprintf(stderr, " plugin: socket() failed for container with id \n");
		return NULL;
	}
	
	memset(&address, 0, sizeof(struct sockaddr_un));

	address.sun_family = AF_UNIX;
	snprintf(address.sun_path, 107, "/var/run/docker.sock");

	if(connect(socket_fd, (struct sockaddr *) &address, sizeof(struct sockaddr_un)) != 0)
	{
		fprintf(stderr, " plugin: connect() failed for container with id \n");
		return NULL;
	}
	nbytes = snprintf(buffer, 255, "GET /containers/e748b034e3821f9a055f8b7ce3adb3a90721dda4fb739b9879d16bf894f76c43/stats?stream=false HTTP/1.1\n\r\n");
	if (write(socket_fd, buffer, nbytes) == -1) {
		fprintf(stderr, " plugin: socket write() failed for container \n");
		goto exit;
	}
	
	
	
	nbytes = read(socket_fd, response, 5000);

	response[nbytes] = 0;
		 
	printf("MESSAGE FROM SERVER: aaaaaa %s\n",response);
	fflush(stdout);
	
	exit:
	close(socket_fd);
	return NULL;		
}

 
int main(void)
{
 struct sockaddr_un address;
 int nbytes,socket_fd;
 char buffer[256];
 char response[2560];

 socket_fd = socket(PF_UNIX, SOCK_STREAM, 0);
 if(socket_fd < 0)
 {
  printf("socket() failed\n");
  return 1;
 }

 /* start with a clean address structure */
 memset(&address, 0, sizeof(struct sockaddr_un));

 address.sun_family = AF_UNIX;
 snprintf(address.sun_path, 107, "/var/run/docker.sock");

 if(connect(socket_fd, 
            (struct sockaddr *) &address, 
            sizeof(struct sockaddr_un)) != 0)
 {
  printf("connect() failed\n");
  return 1;
 }
 int i=0;
 
 pthread_t bla1;

/* create a second thread which executes inc_x(&x) */


 while (1) {
 
 nbytes = snprintf(buffer, 256, "GET /containers/json HTTP/1.1\n\r\n");
 write(socket_fd, buffer, nbytes);
 nbytes = read(socket_fd, response, 2560);

 response[nbytes] = 0;
 
 
 printf("MESSAGE FROM SERVER: aaaaaa %s\n", response);
 
 if(pthread_create(&bla1, NULL, cont1_collect, NULL)) {

fprintf(stderr, "Error creating thread\n");
return 1;

}
sleep(1);
 }

 close(socket_fd);

 return 0;
}
