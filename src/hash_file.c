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

//Το ίδιο θα ισχύει και για τα επόμενα, πχ block + 2*sizeof(Record) 
//θα βάλει το 3ο Record κοκ. Σε γενική περίπτωση block + (i - 1) * sizeof(Record)
HT_ErrorCode HT_CreateIndex(const char *filename, int depth) {

  BF_Block *info;
  BF_Block *directory;
  BF_Block_Init(&directory);
  BF_Block_Init(&info);

  int filedest;
  BF_CreateFile(filename);

  BF_AllocateBlock(filedest, info);
  char* info_ptr = BF_Block_GetData(info);
  
  char* string = "5";

  char* dir_ptr = BF_Block_GetData(directory);
  dir_ptr = malloc(sizeof(char)*15);

  memcpy(dir_ptr,string,sizeof(char)*5);

  char* string2 = "10";
  memcpy(dir_ptr + sizeof(char)*5 , string2, sizeof(sizeof(char)*5));

  int x = atoi(dir_ptr + sizeof(char));

  printf("data = %d\n", x);
  return HT_OK;
}

HT_ErrorCode HT_OpenIndex(const char *fileName, int *indexDesc){
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

