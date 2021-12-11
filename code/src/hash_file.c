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

char* itos(int number){
  char* number_str = malloc(sizeof(char)*5);
  sprintf(number_str, "%d", number);
  number_str[4] = '\0';
  return number_str;
}

void print_char(int start, int len, char* string){
  printf("Print string: %.*s\n", len, string + start);
}

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

char* toBinary(int n, int len)
{
    char* binary = (char*)malloc(sizeof(char) * len);
    int k = 0;
    for (unsigned i = (1 << len - 1); i > 0; i = i / 2) {
        binary[k++] = (n & i) ? '1' : '0';
    }
    binary[k] = '\0';
    return binary;
}

int hashFunction(int id, int depth){
  char* binary = toBinary(id, depth);
  binary = reverse(binary);
  return (int) strtol(binary, NULL, 2) + 2; //γιατι τα buckets ξεκινανε απο την θεση 2//
  //TODO το +2 μπορει να πρεπει να αλλαξει αν επεκταθει το dict
}

char* get_string(int start, int len, char* src){
  char* substring = malloc(sizeof(char)*len);
  strncpy(substring, &src[start], len);
  substring[len - 1] = '\0';
  return substring;
}

int get_int(int start, int len, char* src){
  char* data = get_string(start, len, src);
  int data_int = atoi(data);
  free(data);
  return data_int;
}

typedef struct info{
  int index;
}Info;

HT_ErrorCode HT_Init() {
  //global table απο τα ανοικτα files
  return HT_OK;
}

Info HT_info;

//1 block => 1 bucket
HT_ErrorCode HT_CreateIndex(const char *filename, int depth) {

  //Δίνουμε οντότητα στην δομή
  BF_Block *block;
  BF_Block_Init(&block);

  //Δημιουργούμε το file
  if(BF_CreateFile(filename) != BF_OK)
    return HT_ERROR;
  
  if(BF_OpenFile(filename, &HT_info.index) != BF_OK)
    return HT_ERROR;
  
  //Δεσμέυουμε info block για το αρχείο
  if(BF_AllocateBlock(HT_info.index, block) != BF_OK)
    return HT_ERROR;
  
  //Δεσμέυουμε directory block για το αρχείο
  if(BF_AllocateBlock(HT_info.index, block) != BF_OK)
    return HT_ERROR;

  //Επεξεργασία και αρχικοποιηση του dict block
  BF_GetBlock(HT_info.index, 1, block);
  char* dict = BF_Block_GetData(block);

  for (int i=0; i< pow(2,depth); i++){
    char* key  = itos(i+2);
    memcpy(dict + sizeof(char)*INT_SIZE*i, key, strlen(key));
    free(key);
  }

  //Δεσμέυουμε 4 buckets blocks για το αρχείο και τα αρχικοποιούμε (local + counter)
  for(int bucket=0; bucket< pow(2,depth); bucket++){
    if(BF_AllocateBlock(HT_info.index, block) != BF_OK)
      return HT_ERROR;
    char* block_data = BF_Block_GetData(block);
    char* local_depth = itos(depth);
    memcpy(block_data, local_depth, sizeof(local_depth));
    
    //Το 0 ειναι το counter των records
    memcpy(block_data + sizeof(char)*INT_SIZE, "0\0", strlen("0\0"));
    free(local_depth);
  }

  //Επεξεργαζομαστε το info block
  BF_GetBlock(HT_info.index, 0, block);
  char* data = BF_Block_GetData(block);
  char* depth_str = itos(depth);

  if(data != NULL && depth_str != NULL){
    memcpy(data, depth_str, strlen(depth_str));
    free(depth_str);
  }
  else{
    return HT_ERROR;
  }
  memcpy(data + sizeof(char)*INT_SIZE, "This is HT struct \0", strlen("This is HT struct \0"));

  int num_blocks;
  if(BF_GetBlockCounter(HT_info.index,&num_blocks) != BF_OK)
    BF_PrintError(BF_GetBlockCounter(HT_info.index,&num_blocks));
  else
    printf("Number of blocks:%d\n",num_blocks);

  //Τα κανουμε όλα dirty και unpin
  for(int i=0; i<num_blocks; i++){
    BF_GetBlock(HT_info.index,i,block);
    BF_Block_SetDirty(block);
    BF_UnpinBlock(block);
  }

  BF_CloseFile(HT_info.index);
  return HT_OK;
}

HT_ErrorCode HT_OpenIndex(const char *fileName, int *indexDesc){

  if(BF_OpenFile(fileName, &HT_info.index) != BF_OK)
    return HT_ERROR;

  *indexDesc = HT_info.index;

  if(indexDesc != NULL)
    return HT_OK;
  else
    return HT_ERROR;
}

HT_ErrorCode HT_CloseFile(int indexDesc) {

  int num_blocks;
  BF_GetBlockCounter(HT_info.index,&num_blocks);

  BF_Block *block;
  BF_Block_Init(&block); 

  for(int nblock=0; nblock < num_blocks; nblock++){
    BF_GetBlock(HT_info.index, nblock, block);
    if(BF_UnpinBlock(block) != BF_OK)
      return HT_ERROR;
  }

  if(BF_CloseFile(HT_info.index) != BF_OK)
    return HT_ERROR;
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

  BF_GetBlock(indexDesc,1,block);
  char* data  = BF_Block_GetData(block);
  int depth = get_int(0,INT_SIZE,data);

  int id = record.id;
  int hash_value = hashFunction(id,depth);

  BF_GetBlock(indexDesc, hash_value, block);
  data = BF_Block_GetData(block);
  if(store_record(record,data) == 0)
    printf("record stored\n");

  return HT_OK;
}

HT_ErrorCode HT_PrintAllEntries(int indexDesc, int *id) {
  //Kανε και για συγκεκριμενο ....
  BF_Block *block;
  BF_Block_Init(&block);

  int num_blocks;
  BF_GetBlockCounter(HT_info.index,&num_blocks);
  
  for(int i=0; i<num_blocks-2; i++){
    BF_GetBlock(indexDesc, i+2, block);
    char* data = BF_Block_GetData(block);
    print_block(data);
  }
  return HT_OK;
}

