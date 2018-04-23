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
    Getter get_func;
    int partition_number;
    Reducer reducer_func;
} reducer_args;

typedef struct kvlist{
    kvpair *head;
    kvpair *end;
    kvpair *curr;
    int size;
} partition;

partition **parts;
kvpair ***sorted_arr;
int *partition_iterator;
int head = 0;
int num_partitions;
char **filename;
int file_index = 0;
int files_left;
Mapper globalmap;
Reducer globalreduce;

// needs to take key/value pairs from the many different mappers and store them in a way that later reducers can access them
void MR_Emit(char *key, char *value){
    printf("Entering MR_Emit");
  for(int i = 0; i < num_partitions;i++){
    parts[i]->curr = parts[i]->head;
  }
  for(int i = 0; i < num_partitions;i++){
    while (parts[i]->curr != NULL) {
      printf("Parts[%d]: key = %s\n", i, parts[i]->curr->key);
      parts[i]->curr = parts[i]->curr->nextpair;

      }
    }
    
  for(int i = 0; i < num_partitions;i++){
    parts[i]->curr = parts[i]->head;
  }
  if ((strcmp(key, "\0") == 0) || key == NULL)
      return;
    int listno = MR_DefaultHashPartition(key, num_partitions);
    kvpair *pair;

    //printf(">>>>>>Inside MR_EMIT: KEY: %s Listno: %d\n", key, listno);
    //pthread_mutex_unlock(&m);
    //Grab locks while updating shared data structs
    pthread_mutex_lock(&m);

    // Creates new node for value passed in
    value_node* new_val = malloc(sizeof(struct value_node));
    new_val->value = value;
    new_val->nextval = NULL;
    //printf("New value node with value: %s and key: %s\n", new_val->value, key);

    // Start iterator at head
    parts[listno]->curr = parts[listno]->head;
    //printf("HEAD: %p\n", parts[listno]->head);
    //check if partition is empty
     if (parts[listno]->head == NULL){
          //  printf("EMPTY PARTITION\n");
            if((pair = malloc(sizeof(struct kvpair))) == NULL){
                pthread_mutex_unlock(&m);
                exit(1);
            }
            // Initialize first kvpair of partition
            pair->key = key;
            pair->values = new_val;
	          pair->itr = new_val;
            pair->endval = new_val;
            pair->nextpair = NULL;

            //insert kvpair into partition at index 0
            parts[listno]->head = pair;
            parts[listno]->end = pair;
            parts[listno]->curr = pair;
            parts[listno]->size++;
        //    printf("Added %s to partition %d\n", pair->key, listno);
            pthread_mutex_unlock(&m);
            return;
    }

    // Start iterator at head
    parts[listno]->curr = parts[listno]->head;

    //search through partition for matching key
    while(parts[listno]->curr != NULL){
        //key is found in the list, add new value to list
        //printf("MR_Emit: head->key: %s key: %s\n", parts[listno]->head->key, key);
        if(strcmp(parts[listno]->curr->key,key) == 0){
            //printf("Keys match curr->key: %s key: %s\n", parts[listno]->curr->key, key);
            parts[listno]->curr->endval->nextval = new_val;
            parts[listno]->curr->endval = new_val;
            pthread_mutex_unlock(&m);
            //printf("CURR: added value: %s to list for key %s\n",new_val->value,key);
    	      parts[listno]->curr = parts[listno]->head;
          //  printf("Added %s to partition %d\n", key, listno);
    	      return;
	      }
        else {
            //printf("Keys dont match\n");
            // Go to next pair
            parts[listno]->curr = parts[listno]->curr->nextpair;
        }
    }

    parts[listno]->curr = parts[listno]->head;


    //key was not in partition, add new node
    if((pair = malloc(sizeof(struct kvpair))) == NULL){
        pthread_mutex_unlock(&m);
        exit(1);
    }

    // Initialize new kvpair
    pair->key = key;
    pair->values = new_val;
    pair->itr = new_val;
    pair->endval = new_val;
    pair->nextpair = NULL;

    parts[listno]->end->nextpair = pair;
    parts[listno]->end = pair;
    parts[listno]->size++;
  //  printf("Incrementing size in MR_Emit to %d for partition %d. Added: %s\n", parts[listno]->size, listno, pair->key);
    parts[listno]->curr = parts[listno]->head;
    //printf("Leaving MR_EMIT. head->key: %s \n", parts[listno]->head->key);
    //Release lock when done

  //  printf("Added %s to partition %d\n", pair->key, listno);
  /*  for(int i = 0; i < num_partitions;i++){
      parts[i]->curr = parts[i]->head;
    }
    for(int i = 0; i < num_partitions;i++){
      while (parts[i]->curr != NULL) {
        printf("Parts[%d]: key = %s\n", i, parts[i]->curr->key);
        parts[i]->curr = parts[i]->curr->nextpair;

        }
      }
    for(int i = 0; i < num_partitions;i++){
      parts[i]->curr = parts[i]->head;
    }*/
    pthread_mutex_unlock(&m);
    return;
}

unsigned long MR_DefaultHashPartition(char *key, int num_partitions){
    unsigned long hash = 5381;
    //printf("Hashing Partition Key: %s\n",key);
    int c;

    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    //printf("Leaving MR_DefaultHashPartition\n");
    return hash % num_partitions;
}


char *get_next(char *key, int partition_number){
    //printf("Inside get_next---> key: %s partition number: %d\n",key, partition_number);

    char *check_key;
    char *return_value = NULL;

    // Return NULL if not found
  //  printf("Get_Next Partition_Number: %d Size: %d\n", partition_number, parts[partition_number]->size);

    for (int i = 0; i < parts[partition_number]->size; i++) {
      //printf("Index: %d\n",sorted_arr[partition_number][i]->index);
      check_key = sorted_arr[partition_number][i]->key;
      //printf("get_next: check_key: %s, actual_key: %s for index: %d\n", check_key, key, i);
      if (strcmp(check_key,key) == 0) {
	       //printf("Found Key in get_next\n");
	        if (sorted_arr[partition_number][i]->itr == NULL)
		        return NULL;
	        //printf("Did not return NULL\n");
          return_value = (sorted_arr[partition_number][i]->itr)->value;
	       // printf("Return value in get_next is: %s\n", return_value);
	        if (return_value == NULL)
		        return NULL;
	        sorted_arr[partition_number][i]->itr = sorted_arr[partition_number][i]->itr->nextval;

          //printf("get_next key: %s index: %d, return_value: %s\n",key, index, return_value);

          break;
      }
    }
    //printf("GET_NEXT RETURNING VALUE %s\n", return_value);
    return return_value;
}

/* qsort struct comparision function (product C-string field) */
int struct_cmp(const void *a, const void *b){
    kvpair **ia = (kvpair **)a;
    kvpair **ib = (kvpair **)b;
    return strcmp((*ia)->key, (*ib)->key);
    /* strcmp functions works exactly as expected from
    comparison function */
}

void Partition_sort() {
  for(int i = 0; i < num_partitions;i++){
    parts[i]->curr = parts[i]->head;
  }
  for(int i = 0; i < num_partitions;i++){
    while (parts[i]->curr != NULL) {
    //  printf("Parts[%d]: key = %s\n", i, parts[i]->curr->key);
      parts[i]->curr = parts[i]->curr->nextpair;

      }
    }
  for(int i = 0; i < num_partitions;i++){
    parts[i]->curr = parts[i]->head;
  }
    //printf("Inside MR_Partition_sort\n");
    sorted_arr = malloc(num_partitions*sizeof(void **));
    // Transfer from LL to array
    for(int i = 0; i < num_partitions; i++){
        sorted_arr[i] = malloc(sizeof(struct kvlist)*(parts[i]->size));
        for(int j = 0; j < parts[i]->size;j++){
            if(parts[i]->curr == NULL){
		//printf("NULL in Partition Sort\n");
                break;
            }
            sorted_arr[i][j] = parts[i]->curr;
            //printf(">>>> sorted_arr[%d][%d]->key: %s\n", i,j,sorted_arr[i][j]->key);

            while (sorted_arr[i][j]->itr != NULL) {
            //  printf(">>>> sorted_arr[%d][%d]->value: %s\n", i,j,sorted_arr[i][j]->itr->value);
              sorted_arr[i][j]->itr = sorted_arr[i][j]->itr->nextval;
            }
            sorted_arr[i][j]->itr = sorted_arr[i][j]->values;

             parts[i]->curr = parts[i]->curr->nextpair;
          //  printf(">>>> sorted_arr[%d][%d]: %s\n", i,j,sorted_arr[i][j]->key);

        }
      //  printf("Before qsort in partition sort: Size of array is %d\n", parts[i]->size);
        qsort(sorted_arr[i], parts[i]->size, sizeof(void*), struct_cmp);
      //  printf("After qsort Size of array is %d\n", parts[i]->size);
        for(int j = 0; j < parts[i]->size;j++){
      //      printf(">>>> After sorted_arr[%d][%d]: %s\n", i,j,sorted_arr[i][j]->key);
        }
    }

}

//finds key inside partition and returns it
char *KeySeek(int part_num){
  //printf("Inside KeySeek\n");
  int index = partition_iterator[part_num]++;
  if (index == parts[part_num]->size)
    return NULL;
  char *returnkey = (sorted_arr[part_num][index]->key);
  //if (returnkey != NULL)
  //  printf("KeySeek returnkey: %s for index: %d of partition number: %d\n", returnkey,  index, part_num);
  return returnkey;
}

void *Map_Threads() {
    char *file_name;

    while (files_left != 0) {
        pthread_mutex_lock(&m);
        // Check condition again in case of changing before grabbing lock
        if(files_left == 0) {
          pthread_mutex_unlock(&m);
          break;
        }
        //printf("Number of files left: %d\n", files_left);
        file_name = filename[--files_left];
        pthread_mutex_unlock(&m);
        //printf("Filename passed to Map: %s\n", file_name);
        globalmap(file_name);

    }
    return NULL;
}


void *ReducePrepare(void *red_args){
    //printf("Inside ReducePrepare\n");
    reducer_args *localargs = red_args;
    char* currkey;
    while ((currkey = KeySeek(localargs->partition_number)) != NULL) {
    //Grab lock
    pthread_mutex_lock(&m);

      //Critical Section

      localargs->reducer_func(currkey, localargs->get_func,localargs->partition_number);

      //Release Lock
      pthread_mutex_unlock(&m);
    }
    return (void *)localargs ;
}

void MR_Run(int argc, char *argv[], Mapper map, int num_mappers,Reducer reduce, int num_reducers, Partitioner partition) {
    num_partitions = num_reducers;
    files_left = argc - 1;
    globalmap = map;
    globalreduce = reduce;
    //struct kvlist *part_tmp[num_partitions];
    //parts = part_tmp;
    //Malloc the original partitions which is a LL
    parts = malloc(num_partitions * sizeof(void*));

    if (parts == NULL)
      printf("MALLOC FAILURE MR_Run");

    // Initialize partitions
    for(int j = 0; j < num_partitions; j++){
      //Maybe need to to do *partition (Might be too big)
      parts[j] = malloc(sizeof(struct kvlist)); //Not sure why this works...
      if (parts[j] == NULL)
        printf("MALLOC FAILURE MR_Run");

    }

    partition_iterator = calloc(num_partitions,sizeof(int));
    filename = malloc(sizeof(char*) * argc);
    for(int j = 0; j < argc -1;j++){
        filename[j] = argv[j+1];
    }
    //parts[0]->size = 0;
    //parts[0]->head = NULL;
    //parts[0]->end = NULL;
    //parts[0]->curr = NULL;
    //printf("TESTING %d\n", num_partitions);
    for(int j = 0; j < num_partitions; j++){
      //printf("loop\n");
      // Initialize the struct
      parts[j]->size = 0;
      parts[j]->head = NULL;
      parts[j]->end = NULL;
      parts[j]->curr = NULL;
    }
    //create num_mappers threads and have them map
    pthread_t mappers[num_mappers];
    for(int i = 0; i < num_mappers; i++){
        pthread_create(&mappers[i], NULL, Map_Threads, NULL);
        //printf("Thread Line Check\n");
    }

    //join threads after map
    for(int i = 0; i < num_mappers; i++){
        //printf("Parent waiting...\n");
        pthread_join(mappers[i],NULL);
     }

     free(filename);

     //printf("\n\n----------------------MAPPING ENDING----------------------------\n\n");
     // Sorts each of the partitions in ascending order
     Partition_sort();

      //pass kvpairs to reducer methods for reducing
      reducer_args *reducer_args[num_partitions];
      //create num_reducers threads and have the work on mapped output
      pthread_t reducers[num_reducers];
      for(int i = 0; i < num_partitions; i++){
            reducer_args[i] = calloc(sizeof(struct reducer_args), 1);
        //printf("Loop %d of reduce thread create\n", i);
            //printf("Initializing Reducer: index:%d size %d\n",i, parts[i]->size);
            reducer_args[i]->get_func = get_next;
            reducer_args[i]->partition_number = i;
            reducer_args[i]->reducer_func = reduce;
            pthread_create(&reducers[i], NULL, ReducePrepare, (void *)reducer_args[i]);
      }

      //join threads after map
        for(int i = 0; i < num_partitions; i++){
           pthread_join(reducers[i],NULL);
        }
      //printf("Finished\n");
    return ;
}
