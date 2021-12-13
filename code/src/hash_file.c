#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

#include "bf.h"
#include "hash_file.h"
#define MAX_OPEN_FILES 20
#define INT_SIZE 5

#define CALL_BF(call)       \
{                           \
  BF_ErrorCode code = call; \
  if (code != BF_OK) {         \
    BF_PrintError(code);    \
    return HP_ERROR;        \
  }                         \
}

//make int to string
char* itos(int number){
  char* number_str = malloc(sizeof(char)*5);
  sprintf(number_str, "%d", number);
  number_str[4] = '\0';
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
    s[i]=s[new - 1 -i];
    s[new - 1 - i] = swap;
  }
  return s;
}

//binary representation of a int
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

    offset += sizeof(char)*INT_SIZE;
    int pointer = get_int(offset, INT_SIZE, dict);
    printf("pointer: %d\n",pointer);

    offset += sizeof(char)*INT_SIZE;      
  }
}

//our hash function
char* hashFunction(int id, int depth){
  char* binary = toBinary(id, depth);
  binary = reverse(binary);
  int new_id = (int) strtol(binary, NULL, 2);
  return toBinary(new_id,depth);
}

//gets a specific part from src string
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

    char* pointer = itos(i+2);
    memcpy(data + offset, pointer, strlen(pointer));
    offset += sizeof(char)*INT_SIZE;

    free(key);
    free(pointer);
  }
}

//expands the directory(hash table) when we encounter an overflow
void expand_dict(int new_depth, char* dict, int overflowed_bucket, int last){

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
        }
        else{
          memcpy(dict+bit_offset,pointer,strlen(pointer));
          bit_offset += sizeof(char)*INT_SIZE;
        }

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
  }
  return -1;
}


typedef struct info{
  char file_name[20];
  int index;
}Info;

Info open_files[MAX_OPEN_FILES];         //οσα files ειναι ανοικτα atm
int file_create_counter = 0;               //οσα files εχουν γινει create
int file_open_counter = 0;                //οσα files εχουν γινει open

HT_ErrorCode HT_Init() {
  //global table απο τα ανοικτα files
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
  if(BF_AllocateBlock(index, block) != BF_OK)
    return HT_ERROR;

  
  //Επεξεργασία και αρχικοποιηση του dict block
  BF_GetBlock(index, 1, block);
  char* dict = BF_Block_GetData(block);
  make_dict(depth,dict);
  print_hash_table(dict,depth);
  
  //Δεσμέυουμε 4 buckets blocks για το αρχείο και τα αρχικοποιούμε (local + counter)
  for(int bucket=0; bucket< pow(2,depth); bucket++){
    if(BF_AllocateBlock(index, block) != BF_OK)
      return HT_ERROR;
    char* block_data = BF_Block_GetData(block);
    char* local_depth = itos(depth);
    memcpy(block_data, local_depth, strlen(local_depth));
    
    //Το 0 ειναι το counter των records
    memcpy(block_data + sizeof(char)*INT_SIZE, "0", strlen("0"));
    free(local_depth);
  }

  //Επεξεργαζομαστε το info block
  BF_GetBlock(index, 0, block);
  char* data = BF_Block_GetData(block);
  char* depth_str = itos(depth);

  if(data != NULL && depth_str != NULL){
    memcpy(data, depth_str, strlen(depth_str));
    free(depth_str);
  }
  else{
    return HT_ERROR;
  }
  memcpy(data + sizeof(char)*INT_SIZE, "This is HT struct", strlen("This is HT struct"));

  int num_blocks;
  if(BF_GetBlockCounter(index,&num_blocks) != BF_OK)
    BF_PrintError(BF_GetBlockCounter(index,&num_blocks));
  else
    printf("Number of blocks:%d\n",num_blocks);

  //Τα κανουμε όλα dirty και unpin
  for(int i=0; i<num_blocks; i++){
    BF_GetBlock(index,i,block);
    BF_Block_SetDirty(block);
    BF_UnpinBlock(block);
  }

  BF_CloseFile(index);
  file_create_counter++;
  return HT_OK;
}

HT_ErrorCode HT_OpenIndex(const char *fileName, int *indexDesc){

  if(BF_OpenFile(fileName, indexDesc) != BF_OK)
    return HT_ERROR;

  strcpy(open_files[file_open_counter].file_name,fileName);
  open_files[file_open_counter].index = *indexDesc;
  file_open_counter++;

  if(indexDesc != NULL)
    return HT_OK;
  else
    return HT_ERROR;
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
    if(strcpy(open_files[i].file_name,"") != 0 && open_files[i].index == indexDesc){
      strcpy(open_files[i].file_name, "");
    }
  }
  
  if(BF_CloseFile(indexDesc) != BF_OK){
    file_open_counter--;
    return HT_ERROR;
  }
  return HT_OK;
}

//Χωράνε 5 records σε καθε bucket
int store_record(Record record, char* data){

  //Σιγουρα γραφουμε μετα το ΙΝΤ_ΜΑΧ => local depth
  int count = get_int(INT_SIZE, INT_SIZE, data);

  if(count == 5){
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

  return 0;
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
  char* dict  = BF_Block_GetData(block);

  int pointer = get_bucket(hash_value,global_depth,dict);

  BF_GetBlock(indexDesc, pointer, block);
  char* bucket = BF_Block_GetData(block);


  if(store_record(record,bucket) == 0)
    printf("record stored\n");
  else{
    printf("not stored\n");
    int local_depth = get_int(0,INT_SIZE,bucket);

    //case 1
    if(local_depth == global_depth){
      //ενημερωση του global depth
      char* new_global_depth = itos(global_depth + 1);
      memcpy(info,new_global_depth,strlen(new_global_depth));

      //ενημερωση του local depth
      char* new_local_depth = itos(local_depth + 1);
      memcpy(bucket,new_local_depth,strlen(new_local_depth));

      // split()
      expand_dict(global_depth+1,dict,pointer,get_last_bucket(indexDesc));

    }
  }
  expand_dict(global_depth+1,dict,2,get_last_bucket(indexDesc));
  print_hash_table(dict,global_depth+1);

  printf("\n\n");

  return HT_OK;
}

HT_ErrorCode HT_PrintAllEntries(int indexDesc, int *id) {
  //Kανε και για συγκεκριμενο ....
  BF_Block *block;
  BF_Block_Init(&block);

  int num_blocks;
  BF_GetBlockCounter(indexDesc,&num_blocks);
  
  for(int i=0; i<num_blocks-2; i++){
    BF_GetBlock(indexDesc, i+2, block);
    char* data = BF_Block_GetData(block);
    print_block(data);
  }
  return HT_OK;
}

