#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

#include "bf.h"
#include "sht_file.h"

#define CALL_BF(call)       \
{                           \
  BF_ErrorCode code = call; \
  if (code != BF_OK) {         \
    BF_PrintError(code);    \
    return HP_ERROR;        \
  }                         \
}

void print_secondary_block(char* data){
  //Σιγουρα γραφουμε μετα το ΙΝΤ_ΜΑΧ => local depth
  int count = get_int(INT_SIZE, INT_SIZE, data);
  unsigned long offset = 0;

  printf("local depth: %s\n", get_string(0,INT_SIZE,data));
  offset += sizeof(char)*INT_SIZE;
  printf("counter: %.*s\n", INT_SIZE, data + offset);
  offset += sizeof(char)*INT_SIZE;
  printf("--------------------------------\n");
  for(int i=0; i<count; i++){
    printf("Index_key: %.*s\n", ATTR_NAME_SIZE, data + offset);
    offset += sizeof(char)*ATTR_NAME_SIZE;
    printf("Tuple id: %.*s\n", INT_SIZE, data + offset);
    offset += sizeof(char)*INT_SIZE;
  }
}

char* new_hash(char *str, int len)
{
    unsigned long hash = 5381;
    int c;

    while (c = *str++)
        hash = ((hash << 5) + hash) + c;

    return toBinary(hash,len);
}

int store_secondary_record(SecondaryRecord secondary_record, char* data){
  //Σιγουρα γραφουμε μετα το ΙΝΤ_ΜΑΧ => local depth
  int count = get_int(INT_SIZE, INT_SIZE, data);

  if(count == MAX_RECORDS){
    //Υπερχειλιση
    return -1;
  }

  //περνάμε στο bucket το record χρησιμοποιώντας το offset κατάλληλα
  char* key = secondary_record.index_key;
  unsigned long offset = sizeof(char)*INT_SIZE*2 + sizeof(char)*SECONDARY_RECORD_SIZE*count;
  memcpy(data + offset, key, strlen(key));

  offset += sizeof(char)*ATTR_NAME_SIZE;
  char* tupleId_string = itos(secondary_record.tupleId);
  memcpy(data + offset, tupleId_string, strlen(tupleId_string));
  free(tupleId_string);

  //πρεπει να ενημερώσουμε και να αποθηκευσουμε το count
  count++;
  char* count_string = itos(count);
  memcpy(data + sizeof(char)*INT_SIZE, count_string, strlen(count_string));
  free(count_string);
  return 0;
}

extern Info open_files[MAX_OPEN_FILES];
extern int file_create_counter;
extern int file_open_counter;

HT_ErrorCode SHT_Init() {
  //insert code here
  return HT_OK;
}

HT_ErrorCode SHT_CreateSecondaryIndex(const char *sfileName, char *attrName, int attrLength, int depth,char *fileName ) {
  //Δίνουμε οντότητα στην δομή
  BF_Block *block;
  BF_Block_Init(&block);

  int index;

  //Δημιουργούμε το file
  if(BF_CreateFile(sfileName) != BF_OK)
    return HT_ERROR;

  if(BF_OpenFile(sfileName, &index) != BF_OK)
    return HT_ERROR;

  //Δεσμέυουμε info block για το αρχείο
  if(BF_AllocateBlock(index, block) != BF_OK)
    return HT_ERROR;
  
  //Δεσμέυουμε directory block για το αρχείο
  for(int dir_block=0; dir_block<DIR_BLOCKS; dir_block++){
    if(BF_AllocateBlock(index, block) != BF_OK)
      return HT_ERROR;
  }

  //Επεξεργαζομαστε το info block
  //depth | attrName | attrLenght | fileName
  int offset = 0;
  BF_GetBlock(index, 0, block);
  char* info = BF_Block_GetData(block);
  char* depth_str = itos(depth);
  memcpy(info, depth_str, strlen(depth_str));
  offset += sizeof(char)*INT_SIZE;
  free(depth_str);
  memcpy(info + offset, attrName, strlen(attrName));
  offset += sizeof(char)*ATTR_NAME_SIZE;
  char* attr_length_string = itos(attrLength);
  memcpy(info + offset, attr_length_string, strlen(attr_length_string));
  offset += sizeof(char)*INT_SIZE;
  free(attr_length_string);
  memcpy(info + offset, fileName, strlen(fileName));

  //Επεξεργασία και αρχικοποιηση του dir block
  BF_GetBlock(index, 1, block);
  char* dir = BF_Block_GetData(block);
  make_dir(depth,dir);

  //Δεσμέυουμε κατάλληλα buckets blocks για το αρχείο και τα αρχικοποιούμε (local + counter)
  for(int bucket=0; bucket< pow(2,depth); bucket++){
    if(BF_AllocateBlock(index, block) != BF_OK)
      return HT_ERROR;

    char* bucket_data = BF_Block_GetData(block);
    char* local_depth = itos(depth);
    memcpy(bucket_data, local_depth, strlen(local_depth));
    
    //Το 0 ειναι το counter των records
    memcpy(bucket_data + sizeof(char)*INT_SIZE, "0", strlen("0"));
    free(local_depth);

    if(bucket+1 >= BF_BUFFER_SIZE - (DIR_ENDS+1))
      break;
  }

  dirty_unpin_all(index);
  BF_CloseFile(index);
  file_create_counter++;
  BF_Block_Destroy(&block);
  return HT_OK;
}

HT_ErrorCode SHT_OpenSecondaryIndex(const char *sfileName, int *indexDesc  ) {

  if(BF_OpenFile(sfileName, indexDesc) != BF_OK)
    return HT_ERROR;

  //αντιγράφουμε στο global πίνακά μας το filename και index από το αρχείο που ανοιγουμε
  strcpy(open_files[file_open_counter].file_name,sfileName);
  open_files[file_open_counter].index = *indexDesc;
  //και ενημερώνουμε τα ανοικτά αρχεία
  file_open_counter++;
  return HT_OK;
}

HT_ErrorCode SHT_CloseSecondaryIndex(int indexDesc) {

  int num_blocks;
  BF_GetBlockCounter(indexDesc, &num_blocks);

  BF_Block *block;
  BF_Block_Init(&block); 

  //κανουμε unpin όλα τα blocks
  for(int nblock=0; nblock < num_blocks; nblock++){
    BF_GetBlock(indexDesc, nblock, block);
    if(BF_UnpinBlock(block) != BF_OK)
      return HT_ERROR;
  }

  //και βρισκουμε το filename στον πινακά μας και το διαγράφουμε
  //και ενημερώνουμε το index
  for(int i=0; i<MAX_OPEN_FILES; i++){
    if(strcmp(open_files[i].file_name,"") != 0 && open_files[i].index == indexDesc){
      strcpy(open_files[i].file_name, "");
      open_files[i].index = -1;
    }
  }
  
  //το κανουμε close και μειώνουμε τον counter
  if(BF_CloseFile(indexDesc) != BF_OK){
    file_open_counter--;
    return HT_ERROR;
  }
  BF_Block_Destroy(&block);
  return HT_OK;
}

HT_ErrorCode SHT_SecondaryInsertEntry (int indexDesc,SecondaryRecord record  ) {
  BF_Block* block;
  BF_Block_Init(&block);

  //Παιρνουμε το global depth απο το info block
  BF_GetBlock(indexDesc,0,block);
  char* info  = BF_Block_GetData(block);
  int global_depth = get_int(0,INT_SIZE,info);

  //Παιρνουμε το dir
  BF_GetBlock(indexDesc,1,block);
  char* dir  = BF_Block_GetData(block);

  //Και παιρνουμε το bucket στο οποιο θα πρέπει να μπει το record
  char* key = record.index_key;
  char* hash_value = new_hash(key,global_depth);
  int pointer = get_bucket(hash_value,global_depth,dir);
  free(hash_value);

  BF_GetBlock(indexDesc, pointer, block);
  char* bucket = BF_Block_GetData(block);

  store_secondary_record(record, bucket);

  dirty_unpin_all(indexDesc);
  BF_Block_Destroy(&block);
  return HT_OK;
}

HT_ErrorCode SHT_SecondaryUpdateEntry (int indexDesc, UpdateRecordArray *updateArray ) {
  //insert code here
  return HT_OK;
}

HT_ErrorCode SHT_PrintAllEntries(int sindexDesc, char *index_key ) {
BF_Block *block;
  BF_Block_Init(&block);

  int num_blocks;
  BF_GetBlockCounter(sindexDesc,&num_blocks);

  //αμα δεν υπάρχει id τοτε εκτυπώνουμε όλες τις εγραφές των buckets
  if(index_key == NULL){
    //Για κάθε bucket εκτυπώνουμε τα περιεχόμενά του
    for(int i=DIR_BLOCKS+1; i<num_blocks; i++){
      BF_GetBlock(sindexDesc, i, block);
      char* data = BF_Block_GetData(block);

      printf("========== Secondary Bucket %d ===========\n",i-(DIR_BLOCKS+1));
      print_secondary_block(data);
      printf("================================\n\n");
    }
  }
  //αλλιως θα ψάξουμε τα records με το συγκεκριμένο id
  // else{
  //   printf("We want the record with id %d\n",*id);
  //   BF_GetBlock(indexDesc,0,block);
  //   char* info = BF_Block_GetData(block);

  //   int global_depth = get_int(0,INT_SIZE,info);
  //   char* hash_value = hashFunction(*id,global_depth);

  //   //παιρνουμε το directory
  //   BF_GetBlock(indexDesc,1,block);
  //   char* dir = BF_Block_GetData(block);

  //   //και το bucket
  //   int pointer = get_bucket(hash_value,global_depth,dir);
  //   free(hash_value);

  //   BF_GetBlock(indexDesc,pointer,block);
  //   char* data = BF_Block_GetData(block);

  //   unsigned long offset = 0;
  //   int count = get_int(INT_SIZE,INT_SIZE,data);    //το count είναι τα πόσα records υπάρχουν στο bucket
  //   int flag = 0;
  //   for(int i=0; i<count; i++){

  //     offset = sizeof(char)*INT_SIZE*2 + sizeof(char)*RECORD_SIZE*i;
  //     int temp_id = get_int(offset,INT_SIZE,data);

  //     //αν βρούμε το id τοτε εκτυπώνουμε τα δεδομένα του record
  //     if(temp_id == *id){
  //       flag = 1;
  //       offset += sizeof(char)*INT_SIZE;
  //       char* name = get_string(offset,NAME_SIZE,data);
  //       offset += sizeof(char)*NAME_SIZE;
  //       char* surname = get_string(offset,NAME_SIZE,data);
  //       offset += sizeof(char)*SURNAME_SIZE;
  //       char* city = get_string(offset,NAME_SIZE,data);
  //       printf("Id %d\nName: %s \nSurname: %s \nCity: %s\n\n",temp_id,name,surname,city);
  //       free(name);
  //       free(surname);
  //       free(city);
  //     }
  //   }
  //   if(flag == 0)
  //     printf("There is no record with id:%d\n",*id);
  // }
  BF_Block_Destroy(&block);
  return HT_OK;
}

HT_ErrorCode SHT_HashStatistics(char *filename ) {
  //insert code here
  return HT_OK;
}

HT_ErrorCode SHT_InnerJoin(int sindexDesc1, int sindexDesc2,  char *index_key ) {
  //insert code here
  return HT_OK;
}
