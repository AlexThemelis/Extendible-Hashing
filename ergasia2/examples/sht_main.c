#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "bf.h"
#include "hash_file.h"
#include "sht_file.h"

#define RECORDS_NUM 256 // you can change it if you want
#define GLOBAL_DEPT 2 // you can change it if you want
#define FILE_NAME "data.db"
#define SFILE_NAME "sdata.db"

#define SECOND_FILE_NAME "data2.db"
#define SECOND_SFILE_NAME "sdata2.db"

const char* names[] = {
  "Yannis",
  "Christofos",
  "Sofia",
  "Marianna",
  "Vagelis",
  "Maria",
  "Iosif",
  "Dionisis",
  "Konstantina",
  "Theofilos",
  "Giorgos",
  "Dimitris"
};

const char* surnames[] = {
  "Ioannidis",
  "Svingos",
  "Karvounari",
  "Rezkalla",
  "Nikolopoulos",
  "Berreta",
  "Koronis",
  "Gaitanis",
  "Oikonomou",
  "Mailis",
  "Michas",
  "Halatsis"
};

const char* cities[] = {
  "Athens",
  "San Francisco",
  "Los Angeles",
  "Amsterdam",
  "London",
  "New York",
  "Tokyo",
  "Hong Kong",
  "Munich",
  "Miami"
};

#define CALL_OR_DIE(call)     \
  {                           \
    HT_ErrorCode code = call; \
    if (code != HT_OK) {      \
      printf("Error\n");      \
      exit(code);             \
    }                         \
  }

int updateflag = 0;

//todo
/*print all entries
hash statistics
hash function
add block destroy
*/

int main() {
  BF_Init(LRU);
  
  //Πρώτο πρωτεύον ευρετήριο
  CALL_OR_DIE(HT_Init());
  int indexDesc;
  CALL_OR_DIE(HT_CreateIndex(FILE_NAME, GLOBAL_DEPT));
  CALL_OR_DIE(HT_OpenIndex(FILE_NAME, &indexDesc)); 

  int indexDesc2;
  CALL_OR_DIE(HT_CreateIndex(SECOND_FILE_NAME, GLOBAL_DEPT));
  CALL_OR_DIE(HT_OpenIndex(SECOND_FILE_NAME, &indexDesc2)); 

  //Πρώτο δευτερεύον ευρετήριο
  int sindexDesc;
  CALL_OR_DIE(SHT_Init());
  CALL_OR_DIE(SHT_CreateSecondaryIndex(SFILE_NAME,"City",ATTR_NAME_SIZE,GLOBAL_DEPT,FILE_NAME));
  CALL_OR_DIE(SHT_OpenSecondaryIndex(SFILE_NAME,&sindexDesc));

  int sindexDesc2;
  CALL_OR_DIE(SHT_CreateSecondaryIndex(SECOND_SFILE_NAME,"City",ATTR_NAME_SIZE,GLOBAL_DEPT,SECOND_FILE_NAME));
  CALL_OR_DIE(SHT_OpenSecondaryIndex(SECOND_SFILE_NAME,&sindexDesc2));

  Record record;
  srand(12569874);
  int r;
  printf("Insert Entries\n");

  int tupleId;
  UpdateRecordArray updateArray[MAX_RECORDS];

  for (int id = 0; id < 86; ++id) {
    // create a record
    record.id = id;
    r = rand() % 12;
    memcpy(record.name, names[r], strlen(names[r]) + 1);
    r = rand() % 12;
    memcpy(record.surname, surnames[r], strlen(surnames[r]) + 1);
    r = rand() % 10;
    memcpy(record.city, cities[r], strlen(cities[r]) + 1);

    if(id < 30){
      CALL_OR_DIE(HT_InsertEntry(indexDesc, record, &tupleId, updateArray));
      if(updateflag == 1){
        SHT_SecondaryUpdateEntry(sindexDesc,updateArray);
      }
    }
    else{
      CALL_OR_DIE(HT_InsertEntry(indexDesc2, record, &tupleId, updateArray));
      if(updateflag == 1){
        SHT_SecondaryUpdateEntry(sindexDesc2,updateArray);
      }
    }

    SecondaryRecord rec;
    rec.tupleId = tupleId;
    strcpy(rec.index_key,record.city);

    if(id < 30){
      CALL_OR_DIE(SHT_SecondaryInsertEntry(sindexDesc,rec));
    }
    else{
      CALL_OR_DIE(SHT_SecondaryInsertEntry(sindexDesc2,rec));
    }
  }

  CALL_OR_DIE(SHT_InnerJoin(sindexDesc,sindexDesc2,NULL));

  printf("RUN PrintAllEntries\n");
  int id = rand() % RECORDS_NUM;
  //CALL_OR_DIE(HT_PrintAllEntries(indexDesc, &id));
  //CALL_OR_DIE(HT_PrintAllEntries(indexDesc, NULL));
  //CALL_OR_DIE(HT_PrintAllEntries(indexDesc2, NULL));
  //printf("\n\n\n");
  //CALL_OR_DIE(SHT_PrintAllEntries(sindexDesc, NULL));
  //CALL_OR_DIE(SHT_PrintAllEntries(sindexDesc2, NULL));

  //closing files
  CALL_OR_DIE(SHT_CloseSecondaryIndex(sindexDesc));
  CALL_OR_DIE(HT_CloseFile(indexDesc));
  CALL_OR_DIE(HT_CloseFile(indexDesc2));
  CALL_OR_DIE(SHT_CloseSecondaryIndex(sindexDesc2));
  BF_Close();
}
