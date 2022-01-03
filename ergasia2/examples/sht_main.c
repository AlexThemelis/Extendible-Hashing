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

int main() {
  BF_Init(LRU);
  
  CALL_OR_DIE(HT_Init());
  int indexDesc;
  CALL_OR_DIE(HT_CreateIndex(FILE_NAME, GLOBAL_DEPT));
  CALL_OR_DIE(HT_OpenIndex(FILE_NAME, &indexDesc)); 

  int sindexDesc;
  CALL_OR_DIE(SHT_Init());
  CALL_OR_DIE(SHT_CreateSecondaryIndex(SFILE_NAME,"Surname",ATTR_NAME_SIZE,GLOBAL_DEPT,FILE_NAME));
  CALL_OR_DIE(SHT_OpenSecondaryIndex(SFILE_NAME,&sindexDesc));

  Record record;
  srand(12569874);
  int r;
  printf("Insert Entries\n");

  int tupleId;
  UpdateRecordArray updateArray;

  for (int id = 0; id < 1; ++id) {
    // create a record
    record.id = id;
    r = rand() % 12;
    memcpy(record.name, names[r], strlen(names[r]) + 1);
    r = rand() % 12;
    memcpy(record.surname, surnames[r], strlen(surnames[r]) + 1);
    r = rand() % 10;
    memcpy(record.city, cities[r], strlen(cities[r]) + 1);

    CALL_OR_DIE(HT_InsertEntry(indexDesc, record, &tupleId, &updateArray));

    SecondaryRecord rec;
    rec.tupleId = tupleId;
    strcpy(rec.index_key,record.city);

    CALL_OR_DIE(SHT_SecondaryInsertEntry(sindexDesc,rec));

  }

  printf("RUN PrintAllEntries\n");
  int id = rand() % RECORDS_NUM;
  //CALL_OR_DIE(HT_PrintAllEntries(indexDesc, &id));
  CALL_OR_DIE(HT_PrintAllEntries(indexDesc, NULL));
  CALL_OR_DIE(SHT_PrintAllEntries(sindexDesc, NULL));
  //HashStatistics(FILE_NAME);

  CALL_OR_DIE(SHT_CloseSecondaryIndex(sindexDesc));
  CALL_OR_DIE(HT_CloseFile(indexDesc));
  BF_Close();
}
