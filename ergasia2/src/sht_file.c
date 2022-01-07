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

  if(count == MAX_SECONDARY_RECORDS){
    //Υπερχειλιση
    return -1;
  }

  //περνάμε στο bucket το record χρησιμοποιώντας το offset κατάλληλα
  char* key = secondary_record.index_key;
  unsigned long offset = sizeof(char)*INT_SIZE*2 + sizeof(char)*SECONDARY_RECORD_SIZE*count;
  memcpy(data + offset, key, ATTR_NAME_SIZE);

  offset += sizeof(char)*ATTR_NAME_SIZE;
  char* tupleId_string = itos(secondary_record.tupleId);
  memcpy(data + offset, tupleId_string, INT_SIZE);
  free(tupleId_string);

  //πρεπει να ενημερώσουμε και να αποθηκευσουμε το count
  count++;
  char* count_string = itos(count);
  memcpy(data + sizeof(char)*INT_SIZE, count_string, INT_SIZE);
  free(count_string);
  return 0;
}

void secondary_split(int index, char* bucket, SecondaryRecord record, char* dir){
  BF_Block *block;
  BF_Block_Init(&block);
  BF_AllocateBlock(index, block);

  //Το καινούργιο block
  char* new_block_data = BF_Block_GetData(block);
  //Βαζουμε local depth 0 για να μπορούμε να το ξεχωρισουμε απο το overlowed bucket
  memcpy(new_block_data,"0", strlen("0"));
  memcpy(new_block_data + sizeof(char)*INT_SIZE, "0", strlen("0"));

  //Βάζουμε στο overflowed bucket 0 counter
  memcpy(bucket + INT_SIZE,"0",INT_SIZE);

  int counter_old = 0;  //ποσα records μπαίνουν στον overflowed bucket
  int counter_new = 0;  //ποσα records μπαίνουν στον κανουργιο bucket

  char* local_depth = get_string(0,INT_SIZE,bucket);      //παιρνουμε το local depth του overflowed bucket

  unsigned long bucket_offset = sizeof(char)*INT_SIZE*2;  //και κραταμε τα 2 offsets που χρειαζομαστε
  unsigned long old_offset = INT_SIZE;

  //Κανουμε MAX_RECORDS γιατί έχει γεμισει το bucket με records
  for(int rec=0; rec<MAX_SECONDARY_RECORDS; rec++){

    //Παίρνουμε το key των records
    char* key = get_string(bucket_offset,ATTR_NAME_SIZE,bucket);
    bucket_offset += ATTR_NAME_SIZE;

    //το Hash-αρουμε και παιρνουμε τον αριθμό του bucket που θα δειχνει σε αυτο το record
    //(το local depth είναι ήδη αυξημένο)
    char* hash_value = new_hash(key,atoi(local_depth));
    int pointer = get_bucket(hash_value,atoi(local_depth),dir);

    free(hash_value);
    
    //Και παιρνουμε το bucket στο οποιο θα αποθηκευσουμε το record
    //από το οποιο παίρνουμε το local depth
    //Aν το local depth ειναι 0 τοτε θα βαλουμε το record στο καινουργιο bucket αν οχι τοτε στο overflowed bucket
    BF_GetBlock(index, pointer, block);
    char* check_data = BF_Block_GetData(block);
    int check_depth = get_int(0,INT_SIZE,check_data);

    //παιρνουμε τα υπολοιπα στοιχεία του record και αλλάζουμε καταλληλα το offset
    char* tupleId = get_string(bucket_offset,INT_SIZE,bucket);
    bucket_offset += INT_SIZE;

    if(check_depth == 0){
      unsigned long new_offset = INT_SIZE;
      counter_new++;

      //Αποθηκευουμε το καινουργιο counter κάθε φορά
      char* string_counter = itos(counter_new);
      memcpy(new_block_data +sizeof(char)*new_offset,string_counter,strlen(string_counter));
      free(string_counter);

      new_offset += INT_SIZE + sizeof(char)*SECONDARY_RECORD_SIZE*(counter_new-1);    //προχωραμε μπροστά όσα records εχουμε ήδη αποθηκεύσει

      //και αποθηκευουme μετα τα υπολοιπα στοιχεία
      memcpy(new_block_data + new_offset,key,ATTR_NAME_SIZE);

      new_offset += ATTR_NAME_SIZE;
      memcpy(new_block_data + new_offset,tupleId,strlen(tupleId));

      new_offset += INT_SIZE;

    }
    else{
      //αλλιως βρισκομαστε στην περιπτωση που αποθηκευουμε το record στο ιδιο bucket(overflowed) που ηταν ηδη αποθηκευμένο
      //απλα σε διαφορετικη θέση
      
      /* DEN ELEGXOUME THN PERIPTWSH MHN GINEI HASH STO PALIO BLOCK KA8OLOU */

      unsigned long old_offset = INT_SIZE;
      counter_old++;
      char* string_counter = itos(counter_old);
      memcpy(bucket +sizeof(char)*old_offset,string_counter,INT_SIZE);
      free(string_counter);

      old_offset += INT_SIZE + sizeof(char)*SECONDARY_RECORD_SIZE*(counter_old-1);

      memcpy(bucket + old_offset,key,ATTR_NAME_SIZE);

      old_offset += ATTR_NAME_SIZE;
      memcpy(bucket + old_offset,tupleId,INT_SIZE);

      old_offset += INT_SIZE;

    }
    //κανουμε καταλληλα free
    free(key);
    free(tupleId);

    //Εδω ελέγχεται η περίπτωση που τελείωσαν οι αποθηκευσεις των παλιων records 
    //και αποθεκευουμε το καινουργιο record
    //επισης ενημερώνουμε και το local depth του καινουργιου bucket
    if(rec == MAX_SECONDARY_RECORDS - 1){
      memcpy(new_block_data,local_depth,strlen(local_depth));

      char* new_hash_value = new_hash(record.index_key,atoi(local_depth));
      int new_pointer = get_bucket(new_hash_value,atoi(local_depth),dir);

      //υπαρχουν θεσεις και στα 2 buckets οποτε απλα καλουμε την store_record()
      BF_GetBlock(index,new_pointer,block);
      char* new_bucket = BF_Block_GetData(block);
      store_secondary_record(record,new_bucket);

      free(new_hash_value);
    }
  }
  free(local_depth);
  BF_Block_Destroy(&block);
}

//expands the directory(hash table) when we encounter an overflow
void secondary_expand_dict(int new_depth, char* dir, int overflowed_bucket, int last){
  int len = pow(2,new_depth);

  int old_size = pow(2,new_depth-1);
  int temp_keys[old_size];
  int temp_pointers[old_size];
  int dict_offset = old_size;
  int bit_offset = 0;

  //αποθηκεύουμε τα κλειδία και τους pointers του dir που
  //υπαρχoυν ήδη και τα χρησιμοποιούμε για να φτιάξουμε το καινούργιο μέρος του dir
  for (int i=0; i< len; i++){
    if(i < old_size){
      //εδω αποθηκευουμε το "παλιό" μερος του dir σε temp πινακες
      int key = get_int(bit_offset,INT_SIZE,dir);
      temp_keys[i] = key;
      bit_offset += sizeof(char)*INT_SIZE;

      int pointer = get_int(bit_offset,INT_SIZE,dir);
      temp_pointers[i] = pointer;
      bit_offset += sizeof(char)*INT_SIZE;
    }
    else{
      //και εδω χρησιμοποιούμε αυτους τους πινακες για να φτιάξουμε το extend dir (το καινούργιο μέρος)
        int temp_key = temp_keys[i-dict_offset] + old_size;
        char* key = itos(temp_key);
        char* pointer = itos(temp_pointers[i-dict_offset]);

        memcpy(dir+bit_offset,key,strlen(key));
        bit_offset += sizeof(char)*INT_SIZE;

        //αν βρουμε τον overflowed bucket ως pointer τοτε δεν τον αποθηκευουμε
        //αλλά στη θέση του μπαίνει το bucket που δημιουργήθηκε τελευταιό
        if(atoi(pointer) == overflowed_bucket){
          char* new_bucket = itos(last);
          memcpy(dir+bit_offset,new_bucket,strlen(new_bucket));
          bit_offset += sizeof(char)*INT_SIZE;
          free(new_bucket);
        }
        else{
          memcpy(dir+bit_offset,pointer,strlen(pointer));
          bit_offset += sizeof(char)*INT_SIZE;
        }

        free(key);
        free(pointer);
    }
  }

}

extern Info open_files[MAX_OPEN_FILES];
extern int file_create_counter;
extern int file_open_counter;
extern UpdateRecordArray updates[MAX_RECORDS];
extern int updateflag;

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

  if(updateflag == 1)
    SHT_SecondaryUpdateEntry(indexDesc,updates);
  
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

  //Δοκιμάζουμε να αποθηκεύσουμε το record
  //Aν δεν τα καταφέρουμε υπάρχουν 2 περιπτώσεις
  if(store_secondary_record(record,bucket) == -1){

    //παιρνουμε το local depth
    int local_depth = get_int(0,INT_SIZE,bucket);

    //case 1
    if(local_depth == global_depth){

      //ενημερωση του global depth
      char* new_global_depth = itos(global_depth + 1);
      memcpy(info,new_global_depth,strlen(new_global_depth));

      //ενημερωση του local depth
      char* new_local_depth = itos(local_depth + 1);
      memcpy(bucket,new_local_depth,strlen(new_local_depth));

      //και πραγματοποιείται ο διπλασιασμός του dir με τις απαραίτητες αλλαγές
      secondary_expand_dict(global_depth+1,dir,pointer,get_last_bucket(indexDesc));
      secondary_split(indexDesc,bucket,record,dir);

      free(new_global_depth);
      free(new_local_depth);
    }
    //case 2
    else{
      //ενημερωση του local depth
      char* new_local_depth = itos(local_depth + 1);
      memcpy(bucket,new_local_depth,strlen(new_local_depth));

      //και πραγματοποιείται η καταλληλη ενημέρωση του dir
      pointers_adapt(global_depth+1,dir,pointer,get_last_bucket(indexDesc));
      secondary_split(indexDesc,bucket,record,dir);

      free(new_local_depth);
    }
  }
  dirty_unpin_all(indexDesc);
  BF_Block_Destroy(&block);
  return HT_OK;
}

HT_ErrorCode SHT_SecondaryUpdateEntry (int indexDesc, UpdateRecordArray *updateArray ) {
  //insert code here
  updateflag --;

  BF_Block* block;
  BF_Block_Init(&block);

  //Παιρνουμε το global depth απο το info block
  BF_GetBlock(indexDesc,0,block);
  char* info  = BF_Block_GetData(block);
  int global_depth = get_int(0,INT_SIZE,info);
  char* attr_name = get_string(INT_SIZE,ATTR_NAME_SIZE,info);

  //Παιρνουμε το dir
  BF_GetBlock(indexDesc,1,block);
  char* dir  = BF_Block_GetData(block);

  for(int rec=0; rec <MAX_RECORDS; rec++){

    //Και παιρνουμε το bucket στο οποιο θα πρέπει να μπει το record
    char* key;
    if(strcmp(attr_name,"City") == 0)
      key = updateArray[rec].city;
    else
      key = updateArray[rec].surname;

    char* hash_value = new_hash(key,global_depth);
    int pointer = get_bucket(hash_value,global_depth,dir);
    free(hash_value);

    BF_GetBlock(indexDesc, pointer, block);
    char* bucket = BF_Block_GetData(block);

    int offset = INT_SIZE*2;
    for(int inner_rec=0; inner_rec<MAX_RECORDS; inner_rec++){
      offset += ATTR_NAME_SIZE;
      int tupleid = get_int(offset,INT_SIZE,bucket);

      if(tupleid == updateArray[rec].oldTupleId){
        char* tupleid_string = itos(updateArray[rec].newTupleId);
        memcpy(bucket+offset,tupleid_string,INT_SIZE);
        free(tupleid_string);
        break;
      }
      offset += INT_SIZE;
    }
  }
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
