#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <pthread.h>
#include "mapreduce.h"

typedef struct kvpair {
    char* key;
    char* value;
    void* nextpair;
} kvpair;

typedef struct args{
    char* filename;
    Mapper map;
}args;

typedef struct red_args{
    char* key;
    Getter get_func;
    int partition_number;
}red_args;


typedef struct kvlist{
    kvpair* head;
    kvpair* end;
    kvpair* curr;
    int size;
}partition;

partition **part_array;
kvpair **sorted_arr;
int head = 0;
int num_partitions;
// needs to take key/value pairs from the many different mappers and store them in a way that later reducers can access them
void MR_Emit(char *key, char *value){
    
    int listno = MR_DefaultHashPartition(key, num_partitions);
//TODO: need to grab and release locks here
        kvpair *pair = malloc(sizeof(kvpair));
    
    pair->key = key;
    pair->value = value;
    if(part_array[listno]->head == NULL){
        part_array[listno]->head = pair;
        part_array[listno]->end = pair; 
        part_array[listno]->curr = part_array[listno]->head;
        part_array[listno]->size++;
    }
    else{
        part_array[listno]->end->nextpair = pair;
        part_array[listno]->end = pair;
        part_array[listno]->size++;
    }
       
    return ;
}

unsigned long MR_DefaultHashPartition(char *key, int num_partitions){
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}

char *get_next(char *key, int partition_number){
    kvpair *tmppair = NULL;
    
    if(part_array[partition_number]->curr->nextpair == NULL){
        part_array[partition_number]->curr = part_array[partition_number]->head;
        return NULL; 
    }
    else{
        part_array[partition_number]->curr = part_array[partition_number]->curr->nextpair;
        tmppair = part_array[partition_number]->curr; 
    }
    
    return tmppair->value;
}

/* qsort struct comparision function (product C-string field) */ 
int struct_cmp(const void *a, const void *b) 
{ 
    struct kvpair *ia = (struct kvpair *)a;
    struct kvpair *ib = (struct kvpair *)b;
    return strcmp(ia->key, ib->key);
    /* strcmp functions works exactly as expected from
    comparison function */ 
} 

void Partition_sort() {
    sorted_arr = malloc(num_partitions*sizeof(void *));
    for(int i = 0; i < num_partitions; i++){
        sorted_arr[i] = malloc(sizeof(partition)*(part_array[i]->size));
        for(int j = 0; j<part_array[i]->size;j++){
            if(part_array[i]->curr == NULL){
                break;
            }
            sorted_arr[i][j] = *(part_array[i]->curr);
            part_array[i]->curr = part_array[i]->curr->nextpair;

        }
        qsort(sorted_arr[i], part_array[i]->size, sizeof(kvpair), struct_cmp);
    }

}

void *Map( void *arguments) {
     args *localargs = arguments;
    localargs->map(localargs->filename);

    return (void *)localargs ;
}

void *Reduce(void *red_args){
    char *key;
    Getter get_func;
    int partition_no;
}

void MR_Run(int argc, char *argv[], Mapper map, int num_mappers,Reducer reduce, int num_reducers, Partitioner partition) {   
    num_partitions = num_reducers;
    char *filename[argc];
    for(int j = 0; j < argc -1;j++){
        filename[0] = argv[j+1];
    }
    int files_left = argc - 1;  //tracks number of files left to map
    int num_threads = 0;    //tracks number of threads
    int filenum = 0;
 
    // Loop so that all files get mapped
    while(files_left > 0){
        if(files_left >= num_mappers) {
            num_threads = num_mappers;
        }
        else{
            num_threads = files_left;
        }
        
        args *arguments[argc];

        //creat num_mappers threads and have them map
        pthread_t mappers[num_mappers];
        for(int i = 0; i < num_threads; i++){
           
            arguments[i]->map = map; 
            arguments[i]->filename = filename[filenum++];
            
            pthread_create(&mappers[i], NULL, Map, (void *)arguments[i]);
        }
        
      //join threads after map
        for(int i = 0; i < num_threads; i++){
            pthread_join(mappers[i],NULL);
         }

    files_left -= num_threads;

    }
    int partition_counter = 0;
    while(partition_counter != num_partitions){
        //pass kvpairs to reducer methods for reducing
        red_args *red_args[num_partitions];
        //create num_reducers threads and have the work on mapped output
        pthread_t reducers[num_reducers];
        for(int i = 0; i < ; i++){
           
             
            arguments[i]->filename = filename[filenum++];
            
            pthread_create(&mappers[i], NULL, Map, (void *)arguments[i]);
        }
    }
    return ;
}


