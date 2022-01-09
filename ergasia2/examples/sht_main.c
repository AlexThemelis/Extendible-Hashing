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
  "Miami",
  "Stockholm",
  "Paris",
  "Trikala",
  "Pikermi",
  "Madrid",
  "Seoul",
  "Lisbon",
  "Istanbul",
  "Smyrna",
  "Alexandria",
  "Volos"

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

/*
frees
inner join null
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

  int tupleId;
  UpdateRecordArray updateArray[MAX_RECORDS];

  for (int id = 0; id < 256; ++id) {
    // create a record
    record.id = id;
    r = rand() % 12;
    memcpy(record.name, names[r], strlen(names[r]) + 1);
    r = rand() % 12;
    memcpy(record.surname, surnames[r], strlen(surnames[r]) + 1);
    r = rand() % 21;
    memcpy(record.city, cities[r], strlen(cities[r]) + 1);

    if(id < 140){
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

    if(id < 140){
      CALL_OR_DIE(SHT_SecondaryInsertEntry(sindexDesc,rec));
    }
    else{
      CALL_OR_DIE(SHT_SecondaryInsertEntry(sindexDesc2,rec));
    }
  }

  //printf("Inner join with Null\n");
  //CALL_OR_DIE(SHT_InnerJoin(sindexDesc,sindexDesc2, NULL));

  printf("Inner join with London\n");
  CALL_OR_DIE(SHT_InnerJoin(sindexDesc,sindexDesc2, "London"));

  printf("All entries for London\n");
  CALL_OR_DIE(SHT_PrintAllEntries(sindexDesc,"London"));
  CALL_OR_DIE(SHT_PrintAllEntries(sindexDesc2,"London"));

  printf("Hash Statistics for both secondary directories\n");
  CALL_OR_DIE(SHT_HashStatistics(SFILE_NAME));
  CALL_OR_DIE(SHT_HashStatistics(SECOND_SFILE_NAME));

  //closing files
  CALL_OR_DIE(SHT_CloseSecondaryIndex(sindexDesc));
  CALL_OR_DIE(HT_CloseFile(indexDesc));
  CALL_OR_DIE(HT_CloseFile(indexDesc2));
  CALL_OR_DIE(SHT_CloseSecondaryIndex(sindexDesc2));
  BF_Close();
}
