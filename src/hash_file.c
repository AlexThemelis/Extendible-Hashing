#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "hash_file.h"
#define MAX_OPEN_FILES 20

#define CALL_BF(call)       \
{                           \
  BF_ErrorCode code = call; \
  if (code != BF_OK) {         \
    BF_PrintError(code);    \
    return HP_ERROR;        \
  }                         \
}

HT_ErrorCode HT_Init() {
  return HT_OK;
}

struct HTInfo{
  char* info;
  int global_depth;
};

struct HashBlock{
  BF_Block* bucket;
  int key;
};

struct HastTable{
  struct HashBlock* array;
  // int global_depth
};

struct Bucket{
  int local_depth;
  Record array_of_records[5];
};

//Το ίδιο θα ισχύει και για τα επόμενα, πχ block + 2*sizeof(Record) 
//θα βάλει το 3ο Record κοκ. Σε γενική περίπτωση block + (i - 1) * sizeof(Record)
HT_ErrorCode HT_CreateIndex(const char *filename, int depth) {

  BF_Block *info;
  BF_Block *directory;
  BF_Block_Init(&directory);
  BF_Block_Init(&info);

  int filedest;
  CALL_OR_DIE(BF_CreateFile(filename));

  CALL_OR_DIE(BF_AllocateBlock(filedest, info));
  char* info_ptr = BF_Block_GetData(info);        // το info έχει χώρο για ένα block

  struct HTInfo HTInfo;
  HTInfo.global_depth = depth;
  HTInfo.info = "This is a HT";

  memcpy(info_ptr,&HTInfo,sizeof(HTInfo));

  CALL_OR_DIE(BF_AllocateBlock(filedest, directory));
  char* directory_ptr = BF_Block_GetData(directory);  // δειχνει στην αρχή του directory

  struct HastTable HashTable;
  HashTable.array = malloc(sizeof(struct HashBlock) * depth);
  memcpy(directory_ptr, &HashTable, sizeof(HashTable));

  return HT_OK;
}

HT_ErrorCode HT_OpenIndex(const char *fileName, int *indexDesc){
  //insert code here
  return HT_OK;
}

HT_ErrorCode HT_CloseFile(int indexDesc) {
  //insert code here
  return HT_OK;
}

HT_ErrorCode HT_InsertEntry(int indexDesc, Record record) {
  //insert code here
  return HT_OK;
}

HT_ErrorCode HT_PrintAllEntries(int indexDesc, int *id) {
  //insert code here
  return HT_OK;
}

