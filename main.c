#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
//#include <unistd.h>
#include <stdio.h>

#define THREAD_NUM 3


/*struct pair
{
	char* key;
	char* value;
	struct pair *next;
};
*/
struct pair *pair_find(struct pair *head, char *key){
	struct pair *ptr = head;
	for(; ptr != NULL; ptr = (*ptr).next){
		if(strcmp((*ptr).key, key) == 0) return ptr;
	}
	return NULL;
}

void pair_delete(struct pair **head, struct pair *target){
	struct pair *ptr = *head;
	struct pair *nptr;
	if(ptr != NULL){
		if(ptr == target){
			*head = (*ptr).next;
			free(ptr);
			return;
		}
		else nptr = (*ptr).next;
	}
	else return;	
        for(; nptr != NULL; nptr = (*nptr).next){
	        if(nptr == target){
			(*ptr).next = (*nptr).next;
			free(nptr);
			return;
		}
		else ptr = nptr;
        }
}

void pair_add(struct pair **head, char *key, char *value){
	struct pair *current = NULL;

	if(*head == NULL){
		struct pair *temp = (struct pair *)malloc(sizeof(struct pair));
		(*temp).key = key;
		(*temp).value = value;
		(*temp).next = NULL;
		*head = temp;
	}
	else{
		for(current = *head; (*current).next != NULL; current = (*current).next);
		struct pair *temp = (struct pair *)malloc(sizeof(struct pair));
		(*temp).key = key;
		(*temp).value = value;
		(*temp).next = NULL;
		(*current).next = temp;
	}
}

/*struct arg_holder {
    int argc;
    char** argv;
};
*/
/*void * thread_caller(void * arg) {
    struct arg_holder arg_struct = *(struct arg_holder *)arg;
    free(arg);
    return sample_MainEntry(arg_struct.argc, arg_struct.argv);
}*/

pthread_t my_thread[THREAD_NUM];

void SignalHandler(int sig){ // can be called asynchronously

printf("Send signal to thread 1.\n\n");
//pthread_cancel(thread1);
printf("Send signal to thread 2.\n\n");
//pthread_cancel(thread2);

for(int i=0;i<THREAD_NUM;i++){
                pthread_cancel(my_thread[i]);
        }


printf("Send signal to thread 3.\n\n");
//pthread_kill(thread3,SIGINT);

}

int main(int argc, char **argv)
{
	struct pair* head = NULL;
        struct pair* current = NULL;
	
//	char *temp;
//	int option;

// command line input CURL ver.
	if(argc > 1){
		for(argc--, argv++; *argv; argc--, argv++){
			if(strncasecmp(*argv, "-", 1) == 0){
				switch((*argv)[1])
				{
					case 'S':
					case 'U':
					case 'K':
					case 'L':
						pair_add(&head, *argv, *(argv + 1));
						break;
					default:
						printf("[usage] : -S SID [-U url] -K key -L file. \n");
						printf("-S : Session ID \n");
						printf("-U : RDZ Server domain name. \n");
						printf("-K : Binary running key. \n");
						printf("-L : Log file of timestamp. \n");
						return -1;
				}
			}
		}
	}

// comment line input getopt ver.
/*	while( (option = getopt(argc, argv, "S:U:K:L:")) != -1 )
	{
		switch(option)
		{
			case 'S':
			case 'U':
			case 'K':
			case 'L':
				temp = malloc(sizeof(char) * 2);
				sprintf(temp, "-%c", option);
				pair_add(&head, temp, optarg);
				break;
			case ':':
			case '?':
				printf("[usage] : -S SID [-U url] -K key -L file. \n");
				printf("-S : Session ID \n");
				printf("-U : RDZ Server domain name. \n");
				printf("-K : Binary running key. \n");
				printf("-L : Log file of timestamp. \n");
				return -1;
		}
		printf("option = %c\t\t", option);  
  		printf("optarg = %s\t\t",optarg);  
  		printf("optind = %d\t\t",optind);  
  		printf("argv[optind] = %s\n",argv[optind]); 
	}
*/
	for (current = head;current != NULL; current = (*current).next)
        {
                printf("key: %s     value: %s\n",(*current).key, (*current).value);
        }

	struct pair *list = NULL;
	struct pair *lcurrent = NULL;
	char *log_name;
	int count = 0;	

	while(head != NULL){
		if ((current = pair_find(head, "-K")) != NULL){
			if((lcurrent = pair_find(list, "-K")) != NULL){
				(*lcurrent).value = (*current).value;
			}
			else{
				pair_add(&list, (*current).key, (*current).value);
			}
			pair_delete(&head, current);
		}
		if ((current = pair_find(head, "-L")) != NULL){
			if((lcurrent = pair_find(list, "-L")) != NULL){
                                (*lcurrent).value = (*current).value;
                        }
                        else{
                                pair_add(&list, (*current).key, (*current).value);
                        }
			pair_delete(&head, current);
		}
		else{
			if((lcurrent = pair_find(list, "-L")) != NULL){
				log_name = malloc(sizeof(char) * (strlen((*lcurrent).value)+2));
				sprintf(log_name, "%s_", (*lcurrent).value);
				(*lcurrent).value = log_name;
			}
		}
		if ((current = pair_find(head, "-S")) != NULL){
			if((lcurrent = pair_find(list, "-S")) != NULL){
				(*lcurrent).value = (*current).value;
			}
			else{
				pair_add(&list, (*current).key, (*current).value);
			}
			pair_delete(&head, current);
		}
		if ((current = pair_find(head, "-U")) != NULL){
			if((lcurrent = pair_find(list, "-U")) != NULL){
                                (*lcurrent).value = (*current).value;
                        }
                        else{
                                pair_add(&list, (*current).key, (*current).value);
                        }
                        pair_delete(&head, current);
		}

                pthread_create(&my_thread[count++], NULL, sample_MainEntry, list);
                sleep(10);
        }


//printf("\n\n\n");
//	for (current = list;current != NULL; current = (*current).next)
//        {
//                printf("key: %s     value: %s\n",(*current).key, (*current).value);
//        }




	void *ret[THREAD_NUM];

/*        struct arg_holder *arg_struct[THREAD_NUM];

        signal(SIGINT, SignalHandler);

        arg_struct[0] = malloc(sizeof(*arg_struct[0]));
        arg_struct[0]->argc = argc;
        arg_struct[0]->argv = argv;

        arg_struct[1] = malloc(sizeof(*arg_struct[1]));
        arg_struct[1]->argc = argc;

        char** array;
        array = malloc((argc) * sizeof(char*));
        for(int i = 0; i < argc; i++){

                int length = strlen(argv[i]);

                array[i] = malloc((length + 1) * sizeof(char));
                strcpy(array[i], argv[i]);
        }

        arg_struct[1]->argv = array;

        for(int i = 0;i<arg_struct[1]->argc;i++){
//                if(strcmp(arg_struct[1]->argv[i], "session0000000000002") == 0)
//                {
//                        strcpy(arg_struct[1]->argv[i], "UB91ZP6XOC77GOBTSXZR");
//                }
                if(strcmp(arg_struct[1]->argv[i], "8899") == 0)
                {
                        strcpy(arg_struct[1]->argv[i], "8990");
                }
        }

	        arg_struct[2] = malloc(sizeof(*arg_struct[2]));
        arg_struct[2]->argc = argc;

        char** array2;
        array2 = malloc((argc) * sizeof(char*));
        for(int i = 0; i < argc; i++){

                int length = strlen(argv[i]);

                array2[i] = malloc((length + 1) * sizeof(char));
                strcpy(array2[i], argv[i]);
        }

        arg_struct[2]->argv = array2;

        for(int i = 0;i<arg_struct[2]->argc;i++){
//                if(strcmp(arg_struct[2]->argv[i], "session0000000000002") == 0)
//                {
//                        strcpy(arg_struct[2]->argv[i], "LD93FM0ZS9PWKW3V4R3X");
//                }
                if(strcmp(arg_struct[2]->argv[i], "8899") == 0)
                {
                        strcpy(arg_struct[2]->argv[i], "8991");
                }
        }



        for(int i=0;i<THREAD_NUM;i++){
                pthread_create(&my_thread[i], NULL, thread_caller, arg_struct[i]);
                sleep(10);
        }
*/
         for(int i=0;i<THREAD_NUM;i++){
                pthread_join(my_thread[i], &ret[i]);
	}

	return 0;
}

