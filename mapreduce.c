#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <pthread.h>
#include "mapreduce.h"

//Lock initialization
pthread_mutex_t m = PTHREAD_MUTEX_INITIALIZER;

typedef struct value_node {
    char* value;
    void* nextval;
} value_node;

typedef struct kvpair {
    char* key;
    value_node* itr;
    value_node* values;
    value_node* endval;
    void* nextpair;
    int index;
} kvpair;


typedef struct args{
    char* filename;
    Mapper map;
} args;

typedef struct reducer_args{
    char* key;
    Getter get_func;
    int partition_number;
    Reducer reducer_func;
} reducer_args;

typedef struct kvlist{
    kvpair* head;
    kvpair* end;
    kvpair* curr;
    int size;
} partition;

partition **parts;
kvpair ***sorted_arr;
int *partition_iterator;
int head = 0;
int num_partitions;

// needs to take key/value pairs from the many different mappers and store them in a way that later reducers can access them
void MR_Emit(char *key, char *value){
    int listno = MR_DefaultHashPartition(key, num_partitions);
    kvpair *pair;
    printf("Inside MR_EMIT: Listno: %d\n", listno);

    //Grab locks while updating shared data structs
    pthread_mutex_lock(&m);

    // Creates new node for value passed in
    value_node* new_val = malloc(sizeof(value_node));
    new_val->value = value;
    new_val->nextval = NULL;
    printf("New value node with value: %s\n", new_val->value);

    //check if partition is empty
     if (parts[listno]->head == NULL){
            printf("EMPTY PARTITION\n");
            if((pair = malloc(sizeof(kvpair))) == NULL){
                pthread_mutex_unlock(&m);
                exit(1);
            }
            // Initialize first kvpair of partition
            pair->key = key;
            pair->values = new_val;
	    pair->itr = new_val;
            pair->endval = pair->values;
            pair->nextpair = NULL;

            //insert kvpair into partition at index 0
            parts[listno]->head = pair;
            parts[listno]->end = pair;
            parts[listno]->curr = parts[listno]->head;
            parts[listno]->size++;
            printf("Incrementing size in MR_Emit to %d for partition %d. Added: %s\n", parts[listno]->size, listno, pair->key);
            pthread_mutex_unlock(&m);
            return;
    }

    // Start iterator at head
    parts[listno]->curr = parts[listno]->head;

    //search through partition for matching key
    while(parts[listno]->curr != NULL){
        //key is found at head of list, add new value to list
        if(strcmp(parts[listno]->curr->key,key) == 0){
            parts[listno]->curr->endval->nextval = new_val; // Is this right??
            parts[listno]->curr->endval = new_val;
            parts[listno]->head->endval->nextval = NULL;
            pthread_mutex_unlock(&m);
            printf("CURR: added value: %s to list for key %s\n",new_val->value,key);
    	    parts[listno]->curr = parts[listno]->head;
    	    return;
	}
          else{
            // Go to next pair
            parts[listno]->curr = parts[listno]->curr->nextpair;
          }
    }

    parts[listno]->curr = parts[listno]->head;


    //key was not in partition, add new node
    if((pair = malloc(sizeof(kvpair))) == NULL){
        pthread_mutex_unlock(&m);
        exit(1);
    }
    pair->key = key;
    pair->values = new_val;
    new_val->nextval = NULL;
    new_val->value = value;
    parts[listno]->end->nextpair = pair;
    parts[listno]->end = pair;
    parts[listno]->end->itr = new_val;
    parts[listno]->end->nextpair = NULL;
    parts[listno]->size++;
    printf("Incrementing size in MR_Emit to %d for partition %d. Added: %s\n", parts[listno]->size, listno, pair->key);

    //Release lock when done
    pthread_mutex_unlock(&m);
    printf("Leaving MR_EMIT\n");
    return;
}

unsigned long MR_DefaultHashPartition(char *key, int num_partitions){
    unsigned long hash = 5381;
    printf("Hashing Partition Key: %s\n",key);
    int c;

    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    printf("Leaving MR_DefaultHashPartition\n");
    return hash % num_partitions;
}
int n = 0;
char *get_next(char *key, int partition_number){
    printf("Inside get_next\n");

    char *check_key;
    char *return_value = NULL;
    int index;

    // Loop through the different keys in the partition_number
        // If key found
            // Return the next index as long as its not the end of the values
        // else
            // Continue

    // Return NULL if not found
    printf("Partition_Number: %d Size: %d\n", partition_number, parts[partition_number]->size);

    for (int i = 0; i < parts[partition_number]->size; i++) {
      //printf("Index: %d\n",sorted_arr[partition_number][i]->index);

      check_key = sorted_arr[partition_number][i]->key;
//      if (strcmp(check_key, "") == 0)
//	continue;
      printf("get_next: check_key: %s, actual_key: %s for index: %d\n", check_key, key, i);
      //if (strcmp(check_key,ending_key) == 0)
      //  return NULL;
      //printf("DefaultHash after check for null");
      if (strcmp(check_key,key) == 0) {
	printf("Found Key in get_next\n");
	if (sorted_arr[partition_number][i]->itr == NULL)
		return NULL;
	printf("Did not return NULL\n");	
        return_value = (sorted_arr[partition_number][i]->itr)->value;
	printf("Return value in get_next is: %s\n", return_value);
	if (return_value == NULL)
		return NULL;
	sorted_arr[partition_number][i]->itr = sorted_arr[partition_number][i]->itr->nextval;
        //Needed?


        printf("get_next key: %s index: %d, return_value: %s\n",key, index, return_value);

        break;
      }
    }
    printf("GET_NEXT RETURNING %s\n", return_value);
    return return_value;
}

/* qsort struct comparision function (product C-string field) */
int struct_cmp(const void *a, const void *b){
    printf("Inside struct_cmp\n");
    kvpair **ia = (kvpair **)a;
    kvpair **ib = (kvpair **)b;
    printf("struct_cmp: ia: %s  ib: %s\n", (*ia)->key, (*ib)->key);
    return strcmp((*ia)->key, (*ib)->key);
    /* strcmp functions works exactly as expected from
    comparison function */
}

void Partition_sort() {
    printf("Inside MR_Partition_sort\n");
    sorted_arr = malloc(num_partitions*sizeof(void **));
    // Transfer from LL to array
    for(int i = 0; i < num_partitions; i++){
        sorted_arr[i] = malloc(sizeof(partition)*(parts[i]->size));
        for(int j = 0; j < parts[i]->size;j++){
            if(parts[i]->curr == NULL){
		printf("NULL in Partition Sort\n");
                break;
            }
            sorted_arr[i][j] = parts[i]->curr;
            printf(">>>> sorted_arr[%d][%d]->key: %s\n", i,j,sorted_arr[i][j]->key);
            printf(">>>> sorted_arr[%d][%d]->value: %s\n", i,j,sorted_arr[i][j]->values->value);
             parts[i]->curr = parts[i]->curr->nextpair;
            //printf(">>>> sorted_arr[%d][%d]: %s\n", i,j,sorted_arr[i][j]->key);

        }
        printf("Before qsort in partition sort: Size of array is %d\n", parts[i]->size);
        qsort(sorted_arr[i], parts[i]->size, sizeof(kvpair*), struct_cmp);
        printf("After qsort\n");
        for(int j = 0; j < parts[i]->size;j++){

            printf(">>>> After sorted_arr[%d][%d]: %s\n", i,j,sorted_arr[i][j]->key);


        }
    }

}

//finds key inside partition and returns it
char *KeySeek(int part_num){
  printf("Inside KeySeek\n");
  int index = partition_iterator[part_num]++;
  if (index == parts[part_num]->size)
    return NULL;
  char *returnkey = (sorted_arr[part_num][index]->key);
  if (returnkey != NULL)
    printf("KeySeek returnkey: %s for key: %d\n", returnkey,  index);
  return returnkey;
}

void *MapPrepare( void *arguments) {
    printf("Inside MapPrepare\n");
    //Grab lock
    //pthread_mutex_lock(&m);
    //Critical Section
    args *localargs = arguments;
    localargs->map(localargs->filename);
    //Release Lock
    //pthread_mutex_unlock(&m);
    return (void *)localargs ;
}

void *ReducePrepare(void *red_args){
    printf("Inside ReducePrepare\n");
    //Grab lock
    pthread_mutex_lock(&m);
    //Critical Section
    reducer_args *localargs = red_args;
    localargs->reducer_func(localargs->key, localargs->get_func,localargs->partition_number);
    //Release Lock
    pthread_mutex_unlock(&m);
    return (void *)localargs ;
}

void MR_Run(int argc, char *argv[], Mapper map, int num_mappers,Reducer reduce, int num_reducers, Partitioner partition) {
    num_partitions = num_reducers;

    //Malloc the original parts which is a LL

    parts = malloc(num_partitions * sizeof(*partition));
    if (parts == NULL)
      printf("MALLOC FAILURE MR_Run");

    // Initialize partitions
    for(int j = 0; j < num_partitions; j++){

      //Maybe need to to do *partition (Might be too big)
      parts[j] = malloc(100*sizeof(partition)); //Not sure why this works...

      if (parts[j] == NULL)
        printf("MALLOC FAILURE MR_Run");

      // Initialize the struct
      parts[j]->size = 0;
      parts[j]->head = NULL;
      parts[j]->end = NULL;
      parts[j]->curr = NULL;
    }

    partition_iterator = calloc(num_partitions,sizeof(int));
    char *filename[argc];
    for(int j = 0; j < argc -1;j++){
        filename[0] = argv[j+1];
    }
    int files_left = argc - 1;  //tracks number of files left to map
    int num_threads = 0;    //tracks number of threads
    int filenum = 0;
    args **arguments = malloc(argc*sizeof(void *));

    // Loop so that all files get mapped
    while(files_left > 0){
        if(files_left >= num_mappers) {
            num_threads = num_mappers;
        }
        else{
            num_threads = files_left;
        }

        //creat num_mappers threads and have them map
        pthread_t mappers[num_mappers];
        for(int i = 0; i < num_threads; i++){
            arguments[i] = malloc(sizeof(struct args));
            arguments[i]->map = map;
            arguments[i]->filename = filename[filenum++];

            pthread_create(&mappers[i], NULL, &MapPrepare, (void *)arguments[i]);
            printf("Thread Line Check\n");
        }

      //join threads after map
        for(int i = 0; i < num_threads; i++){
            printf("Parent waiting...\n");
            pthread_join(mappers[i],NULL);
         }

    files_left -= num_threads;

    }

    // Sorts each of the partitions in ascending order
    Partition_sort();

      //pass kvpairs to reducer methods for reducing
      reducer_args *reducer_args[num_partitions];
      //create num_reducers threads and have the work on mapped output
      pthread_t reducers[num_reducers];
      char* currkey;
      for(int i = 0; i < num_partitions; i++){
        printf("Loop %d of reduce thread create\n", i);
          while ((currkey = KeySeek(i)) != NULL) {
            reducer_args[i]->get_func = get_next;
            reducer_args[i]->partition_number = i;
            reducer_args[i]->key = currkey;
            reducer_args[i]->reducer_func = reduce;
            pthread_create(&reducers[i], NULL, ReducePrepare, (void *)reducer_args[i]);
            pthread_join(reducers[i],NULL);
          }

      }

      //join threads after map
      //  for(int i = 0; i < num_partitions; i++){
      //      pthread_join(reducers[i],NULL);
      //   }
      printf("Finished\n");
    return ;
}
