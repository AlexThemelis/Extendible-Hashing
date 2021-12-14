#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

#include "bf.h"
#include "hash_file.h"
#define MAX_OPEN_FILES 20
#define INT_SIZE 5
#define DICT_BLOCKS 1

#define CALL_BF(call)       \
{                           \
  BF_ErrorCode code = call; \
  if (code != BF_OK) {         \
    BF_PrintError(code);    \
    return HP_ERROR;        \
  }                         \
}

//make int to string must free after
char* itos(int number){
  char* number_str = malloc(sizeof(char)*INT_SIZE);
  sprintf(number_str, "%d", number);
  number_str[INT_SIZE-1] = '\0';
  return number_str;
}

//prints the string from start to len
void print_char(int start, int len, char* string){
  printf("Print string: %.*s\n", len, string + start);
}

//reversing a string
char* reverse(char* s){
  int i,swap;
  int new = strlen(s);

  for(i =0; i<new/2; i++){
    swap = s[i];
    s[i]=s[new-1-i];
    s[new-1-i] = swap;
  }
  return s;
}

//binary representation of a int must free after
char* toBinary(int number, int len)
{
    char* binary = (char*)malloc(sizeof(char) * len);
    int k = 0;
    for (unsigned i = (1 << len - 1); i > 0; i = i / 2) {
        binary[k++] = (number & i) ? '1' : '0';
    }
    binary[k] = '\0';
    return binary;
}

//pritns the directory -> keys with pointers
void print_hash_table(char* dict, int depth){
  int len = pow(2,depth);
  unsigned long offset = 0;

  for(int i=0; i<len; i++){

    int key = get_int(offset, INT_SIZE, dict);
    char* binary_key = toBinary(key,depth);
    printf("key: %s ",binary_key);
    free(binary_key);

    offset += sizeof(char)*INT_SIZE;
    int pointer = get_int(offset, INT_SIZE, dict);
    printf("pointer: %d\n",pointer);

    offset += sizeof(char)*INT_SIZE;      
  }
}

//our hash function
char* hashFunction(int id, int depth){
  char* binary = toBinary(id, depth);
  int new_id = (int) strtol(binary, NULL, 2);
  free(binary);
  return toBinary(new_id,depth);
}

//gets a specific part from src string must free after
char* get_string(int start, int len, char* src){
  char* substring = malloc(sizeof(char)*len);
  strncpy(substring, &src[start], len);
  substring[len - 1] = '\0';
  return substring;
}

//from a string we get the int from specific position
int get_int(int start, int len, char* src){
  char* data = get_string(start, len, src);
  int data_int = atoi(data);
  free(data);
  return data_int;
}

//returns the position of last bucket
int get_last_bucket(int index){
  int num;
  BF_GetBlockCounter(index,&num);
  return num;
}

//makes the the original directory(hash table)
void make_dict(int depth, char* data){
  unsigned long offset = 0;
  for (int i=0; i< pow(2,depth); i++){
    char* key = itos(i);
    memcpy(data + offset, key, strlen(key));
    offset += sizeof(char)*INT_SIZE;

    char* pointer = itos(i+DICT_BLOCKS+1);
    memcpy(data + offset, pointer, strlen(pointer));
    offset += sizeof(char)*INT_SIZE;

    free(key);
    free(pointer);
  }
}

//expands the directory(hash table) when we encounter an overflow
void expand_dict(int new_depth, char* dict, int overflowed_bucket, int last){

  if(pow(2,new_depth) == 64){
    printf("Cant store more than %d records\n",RECORDS_NUM);
    exit(1);
  }

  int size = pow(2,new_depth-1);
  int temp_keys[size];
  int temp_pointers[size];
  int dict_offset = pow(2,new_depth-1);
  int bit_offset = 0;

  for (int i=0; i< pow(2,new_depth); i++){

    if(i < pow(2,new_depth-1)){
      int key = get_int(bit_offset,INT_SIZE,dict);
      temp_keys[i] = key;
      bit_offset += sizeof(char)*INT_SIZE;

      int pointer = get_int(bit_offset,INT_SIZE,dict);
      temp_pointers[i] = pointer;
      bit_offset += sizeof(char)*INT_SIZE;
    }
    else{

        int temp_key = temp_keys[i-dict_offset] + pow(2,new_depth-1);
        char* key = itos(temp_key);
        char* pointer = itos(temp_pointers[i-dict_offset]);

        memcpy(dict+bit_offset,key,strlen(key));
        bit_offset += sizeof(char)*INT_SIZE;

        if(atoi(pointer) == overflowed_bucket){
          char* new_bucket = itos(last);
          memcpy(dict+bit_offset,new_bucket,strlen(new_bucket));
          bit_offset += sizeof(char)*INT_SIZE;
          free(new_bucket);
        }
        else{
          memcpy(dict+bit_offset,pointer,strlen(pointer));
          bit_offset += sizeof(char)*INT_SIZE;
        }

        free(key);
        free(pointer);
    }
  }

}

//we get the bucket with hash value from the directory
int get_bucket(char* hash_value, int depth, char* dict){
  int len = pow(2,depth);
  unsigned long offset = 0;

  for(int i=0; i<len; i++){

    int key = get_int(offset, INT_SIZE, dict);
    char* binary_key = toBinary(key,depth);

    offset += sizeof(char)*INT_SIZE;
    int pointer = get_int(offset, INT_SIZE, dict);

    offset += sizeof(char)*INT_SIZE;

    if(strcmp(binary_key,hash_value) == 0){
      return pointer;
    }
    free(binary_key);
  }
  return -1;
}

//Χωράνε 5 records σε καθε bucket
int store_record(Record record, char* data){

  //Σιγουρα γραφουμε μετα το ΙΝΤ_ΜΑΧ => local depth
  int count = get_int(INT_SIZE, INT_SIZE, data);

  if(count == MAX_RECORDS){
    //Υπερχειλιση
    return -1;
  }

  char* id_string = itos(record.id);
  unsigned long offset = sizeof(char)*INT_SIZE*2 + sizeof(char)*RECORD_SIZE*count;
  memcpy(data + offset, id_string, strlen(id_string));
  offset += sizeof(char)*INT_SIZE;
  memcpy(data + offset, record.name, strlen(record.name));
  offset += sizeof(char)*NAME_SIZE;
  memcpy(data + offset, record.surname, strlen(record.surname));
  offset += sizeof(char)*SURNAME_SIZE;
  memcpy(data + offset, record.city, strlen(record.city));


  //πρεπει να το αποθηκευσουμε
  count++;
  char* count_string = itos(count);
  memcpy(data + sizeof(char)*INT_SIZE, count_string, strlen(count_string));
  free(count_string);
  free(id_string);
  return 0;
}

//we split the overflowed bucket
void split(int index, char* bucket, Record record, char* dict){

  BF_Block *block;
  BF_Block_Init(&block);
  BF_AllocateBlock(index, block);

  char* block_data = BF_Block_GetData(block);
  memcpy(block_data,"0", strlen("0"));
  memcpy(block_data + sizeof(char)*INT_SIZE, "0", strlen("0"));

  int counter_old = 0;
  int counter_new = 0;
  char* local_depth = get_string(0,INT_SIZE,bucket);

  unsigned long bucket_offset = sizeof(char)*INT_SIZE*2;
  unsigned long old_offset = INT_SIZE;

  for(int rec=0; rec<MAX_RECORDS; rec++){
    int id = get_int(bucket_offset,INT_SIZE,bucket);
    bucket_offset += INT_SIZE;

    char* hash_value = hashFunction(id,atoi(local_depth));
    int pointer = get_bucket(hash_value,atoi(local_depth),dict);
    free(hash_value);
    
    BF_GetBlock(index, pointer, block);
    char* check_data = BF_Block_GetData(block);
    int check = get_int(0,INT_SIZE,check_data);

    char* name = get_string(bucket_offset,NAME_SIZE,bucket);
    bucket_offset += NAME_SIZE;
    char* surname = get_string(bucket_offset,SURNAME_SIZE,bucket);
    bucket_offset += SURNAME_SIZE;
    char* city = get_string(bucket_offset,CITY_SIZE,bucket);
    bucket_offset += CITY_SIZE;

    if(check == 0){
      unsigned long new_offset = INT_SIZE;
      counter_new++;
      char* string_counter = itos(counter_new);
      memcpy(check_data +sizeof(char)*new_offset,string_counter,strlen(string_counter));
      new_offset += INT_SIZE + sizeof(char)*RECORD_SIZE*(counter_new-1);
      char* string_id = itos(id);
      memcpy(check_data + new_offset,string_id,strlen(string_id));
      new_offset += INT_SIZE;
      memcpy(check_data + new_offset,name,strlen(name));
      new_offset += NAME_SIZE;
      memcpy(check_data + new_offset,surname,strlen(surname));
      new_offset += SURNAME_SIZE;
      memcpy(check_data + new_offset,city,strlen(city));
      free(string_counter);
      free(string_id);
    }
    else{
      unsigned long old_offset = INT_SIZE;
      counter_old++;
      char* string_counter = itos(counter_old);
      memcpy(bucket +sizeof(char)*old_offset,string_counter,strlen(string_counter));
      old_offset += INT_SIZE + sizeof(char)*RECORD_SIZE*(counter_old-1);
      char* string_id = itos(id);
      memcpy(bucket + old_offset,string_id,strlen(string_id));
      old_offset += INT_SIZE;
      memcpy(bucket + old_offset,name,strlen(name));
      old_offset += NAME_SIZE;
      memcpy(bucket + old_offset,surname,strlen(surname));
      old_offset += SURNAME_SIZE;
      memcpy(bucket + old_offset,city,strlen(city));
      free(string_counter);
      free(string_id);
    }
    free(name);
    free(surname);
    free(city);

    if(rec == MAX_RECORDS - 1){
      memcpy(block_data,local_depth,strlen(local_depth));
      char* new_hash = hashFunction(record.id,atoi(local_depth));
      int new_pointer = get_bucket(new_hash,atoi(local_depth),dict);
      BF_GetBlock(index,new_pointer,block);
      char* new_data = BF_Block_GetData(block);
      store_record(record,new_data);
      free(new_hash);
    }
  }
  free(local_depth);
  BF_Block_Destroy(&block);
}

void pointers_adapt(int new_depth, char* dict, int overflowed_bucket, int last){
  int bit_offset = 0;
  int flag = 0;
  for(int i=0; i<pow(2,new_depth); i++){
    int key = get_int(bit_offset,INT_SIZE,dict);
    bit_offset += sizeof(char)*INT_SIZE;
    int pointer = get_int(bit_offset,INT_SIZE,dict);

    if(pointer == overflowed_bucket && flag==0){
      flag++;
      char* string_last = itos(last);
      memcpy(dict + bit_offset, string_last, strlen(string_last));
      free(string_last);
    }
    else if(pointer == overflowed_bucket){
      flag--;
    }
    bit_offset += sizeof(char)*INT_SIZE;
  }
}

void dirty_unpin_all(int indexDesc){
  BF_Block *block;
  BF_Block_Init(&block);

  int num_blocks;
  if(BF_GetBlockCounter(indexDesc,&num_blocks) != BF_OK)
    BF_PrintError(BF_GetBlockCounter(indexDesc,&num_blocks));
  
  for(int i=0; i<num_blocks; i++){
    BF_GetBlock(indexDesc,i,block);
    BF_Block_SetDirty(block);
    BF_UnpinBlock(block);
  }
  BF_Block_Destroy(&block);
}

typedef struct info{
  char file_name[20];
  int index;
}Info;

Info open_files[MAX_OPEN_FILES];          //οσα files ειναι ανοικτα atm
int file_create_counter;              //οσα files εχουν γινει create
int file_open_counter;                //οσα files εχουν γινει open

HT_ErrorCode HT_Init() {
  //global table απο τα ανοικτα files
  file_open_counter = 0; 
  file_create_counter = 0;
  return HT_OK;
}

HT_ErrorCode HT_CreateIndex(const char *filename, int depth) {

  //Δίνουμε οντότητα στην δομή
  BF_Block *block;
  BF_Block_Init(&block);

  int index;

  //Δημιουργούμε το file
  if(BF_CreateFile(filename) != BF_OK)
    return HT_ERROR;

  //αποθηκευεται στον πινακά μας
  if(BF_OpenFile(filename, &index) != BF_OK)
    return HT_ERROR;

  //Δεσμέυουμε info block για το αρχείο
  if(BF_AllocateBlock(index, block) != BF_OK)
    return HT_ERROR;
  
  //Δεσμέυουμε directory block για το αρχείο
  //TODO Επέκταση του directory σε πολλαπλά blocks
  if(BF_AllocateBlock(index, block) != BF_OK)
    return HT_ERROR;

  
  //Επεξεργασία και αρχικοποιηση του dir block
  BF_GetBlock(index, 1, block);
  char* dir = BF_Block_GetData(block);
  make_dict(depth,dir);
  //print_hash_table(dir,depth);
  
  //Δεσμέυουμε κατάλληλα buckets blocks για το αρχείο και τα αρχικοποιούμε (local + counter)
  for(int bucket=0; bucket< pow(2,depth); bucket++){
    if(BF_AllocateBlock(index, block) != BF_OK)
      return HT_ERROR;
    char* bucket = BF_Block_GetData(block);
    char* local_depth = itos(depth);
    memcpy(bucket, local_depth, strlen(local_depth));
    
    //Το 0 ειναι το counter των records
    memcpy(bucket + sizeof(char)*INT_SIZE, "0", strlen("0"));
    free(local_depth);
  }

  //Επεξεργαζομαστε το info block
  BF_GetBlock(index, 0, block);
  char* info = BF_Block_GetData(block);
  char* depth_str = itos(depth);
  memcpy(info, depth_str, strlen(depth_str));
  free(depth_str);

  dirty_unpin_all(index);
  BF_CloseFile(index);
  file_create_counter++;
  BF_Block_Destroy(&block);
  return HT_OK;
}

HT_ErrorCode HT_OpenIndex(const char *fileName, int *indexDesc){

  if(BF_OpenFile(fileName, indexDesc) != BF_OK)
    return HT_ERROR;

  strcpy(open_files[file_open_counter].file_name,fileName);
  open_files[file_open_counter].index = *indexDesc;
  file_open_counter++;
  return HT_OK;
}

HT_ErrorCode HT_CloseFile(int indexDesc) {

  int num_blocks;
  BF_GetBlockCounter(indexDesc, &num_blocks);

  BF_Block *block;
  BF_Block_Init(&block); 

  for(int nblock=0; nblock < num_blocks; nblock++){
    BF_GetBlock(indexDesc, nblock, block);
    if(BF_UnpinBlock(block) != BF_OK)
      return HT_ERROR;
  }

  for(int i=0; i<MAX_OPEN_FILES; i++){
    if(strcmp(open_files[i].file_name,"") != 0 && open_files[i].index == indexDesc){
      strcpy(open_files[i].file_name, "");
    }
  }
  
  if(BF_CloseFile(indexDesc) != BF_OK){
    file_open_counter--;
    return HT_ERROR;
  }
  BF_Block_Destroy(&block);
  return HT_OK;
}

HT_ErrorCode HT_InsertEntry(int indexDesc, Record record) {

  BF_Block* block;
  BF_Block_Init(&block);

  BF_GetBlock(indexDesc,0,block);
  char* info  = BF_Block_GetData(block);
  int global_depth = get_int(0,INT_SIZE,info);

  int id = record.id;
  char* hash_value = hashFunction(id,global_depth);

  BF_GetBlock(indexDesc,1,block);
  char* dir  = BF_Block_GetData(block);
  int pointer = get_bucket(hash_value,global_depth,dir);
  free(hash_value);

  BF_GetBlock(indexDesc, pointer, block);
  char* bucket = BF_Block_GetData(block);

  if(store_record(record,bucket) == -1){
    int local_depth = get_int(0,INT_SIZE,bucket);

    //case 1
    if(local_depth == global_depth){
      //ενημερωση του global depth
      char* new_global_depth = itos(global_depth + 1);
      memcpy(info,new_global_depth,strlen(new_global_depth));

      //ενημερωση του local depth
      char* new_local_depth = itos(local_depth + 1);
      memcpy(bucket,new_local_depth,strlen(new_local_depth));

      expand_dict(global_depth+1,dir,pointer,get_last_bucket(indexDesc));
      split(indexDesc,bucket,record,dir);

      free(new_global_depth);
      free(new_local_depth);
    }
    //case 2
    else{
      char* new_local_depth = itos(local_depth + 1);
      memcpy(bucket,new_local_depth,strlen(new_local_depth));

      pointers_adapt(global_depth+1,dir,pointer,get_last_bucket(indexDesc));
      split(indexDesc,bucket,record,dir);

      free(new_local_depth);
    }
  }
  dirty_unpin_all(indexDesc);
  BF_Block_Destroy(&block);
  return HT_OK;
}

HT_ErrorCode HT_PrintAllEntries(int indexDesc, int *id) {
  BF_Block *block;
  BF_Block_Init(&block);

  int num_blocks;
  BF_GetBlockCounter(indexDesc,&num_blocks);
  if(id == NULL){
    for(int i=0; i<num_blocks-2; i++){
      BF_GetBlock(indexDesc, i+2, block);
      char* data = BF_Block_GetData(block);
      printf("========== Block %d ============\n",i);
      print_block(data);
      printf("\n================================\n");
    }
  }
  else{
    printf("We want the record with id %d\n",*id);
    BF_GetBlock(indexDesc,0,block);
    char* info = BF_Block_GetData(block);
    int global_depth = get_int(0,INT_SIZE,info);
    char* hash_value = hashFunction(*id,global_depth);

    BF_GetBlock(indexDesc,1,block);
    char* dir = BF_Block_GetData(block);
    int pointer = get_bucket(hash_value,global_depth,dir);
    free(hash_value);
    BF_GetBlock(indexDesc,pointer,block);
    char* data = BF_Block_GetData(block);

    unsigned long offset = 0;
    int count = get_int(INT_SIZE,INT_SIZE,data);
    for(int i=0; i<count; i++){
      offset = sizeof(char)*INT_SIZE*2 + sizeof(char)*RECORD_SIZE*i;
      int temp_id = get_int(offset,INT_SIZE,data);
      if(temp_id == *id){
        offset += sizeof(char)*INT_SIZE;
        char* name = get_string(offset,NAME_SIZE,data);
        offset += sizeof(char)*NAME_SIZE;
        char* surname = get_string(offset,NAME_SIZE,data);
        offset += sizeof(char)*SURNAME_SIZE;
        char* city = get_string(offset,NAME_SIZE,data);
        printf("Id %d\nName: %s \nSurname: %s \nCity: %s\n",temp_id,name,surname,city);
        free(name);
        free(surname);
        free(city);
      }
    }
  }
  BF_Block_Destroy(&block);
  return HT_OK;
}

HT_ErrorCode HashStatistics(const char *filename){
  BF_Block *block;
  BF_Block_Init(&block);

  for(int i=0; i<file_open_counter; i++){
    if(strcmp(open_files[i].file_name,filename) == 0){
      int index = open_files[i].index;
      int num_blocks;
      BF_GetBlockCounter(index,&num_blocks);
      printf("The file has %d blocks\n",num_blocks);

      if(num_blocks == DICT_BLOCKS + 1){
        printf("No buckets in the file\n");
        return HT_ERROR;
      }

      int min = MAX_RECORDS + 1;
      int max = -1;
      int sum = 0;

      for(int j=2; j<num_blocks; j++){
        BF_GetBlock(index,j,block);
        char* data = BF_Block_GetData(block);
        unsigned long offset = 0;
        int count = get_int(INT_SIZE,INT_SIZE,data);
        if(count < min)
          min = count;
        if(count > max)
          max = count;
        sum += count;
      }
      sum = sum/(num_blocks-2);
      printf("Min:%d\nMax:%d\nAvarage:%d\n",min,max,sum);
    }
  }
  BF_Block_Destroy(&block);
  return HT_OK;
}