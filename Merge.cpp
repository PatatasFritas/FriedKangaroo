/*
* This file is part of the BSGS distribution (https://github.com/JeanLucPons/Kangaroo).
* Copyright (c) 2020 Jean Luc PONS.
*
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation, version 3.
*
* This program is distributed in the hope that it will be useful, but
* WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
* General Public License for more details.
*
* You should have received a copy of the GNU General Public License
* along with this program. If not, see <http://www.gnu.org/licenses/>.
*/

#include "Kangaroo.h"
#include <fstream>
#include "SECPK1/IntGroup.h"
#include "Timer.h"
#include <string.h>
#define _USE_MATH_DEFINES
#include <math.h>
#include <algorithm>
#include <dirent.h>
#include <sys/stat.h>
#ifndef WIN64
#include <pthread.h>
#endif

using namespace std;

#define HASHENTRY_ADD_ENTRY(entry) {            \
  /* Shift the end of the index table */        \
  for (int i = he->nbItem; i > st; i--)         \
    he->items[i] = he->items[i - 1];            \
  he->items[st] = entry;                        \
  he->nbItem++;}


#define READ_ITEMS(FILE) {                      \
  fread(&nbItem,sizeof(uint32_t),1,FILE);       \
  fread(&maxItem,sizeof(uint32_t),1,FILE);      \
  for(uint32_t i = 0; i < nbItem; i++) {        \
    ENTRY* e = (ENTRY*)malloc(sizeof(ENTRY));   \
    fread(&(e->x),16,1,FILE);                   \
    fread(&(e->d),16,1,FILE);                   \
    int addStatus = hashentry_Add(&he,e);       \
    switch(addStatus) {                         \
      case ADD_OK:                              \
        break;                                  \
      case ADD_DUPLICATE:                       \
        free(e);                                \
        collisionInSameHerd++;                  \
        break;                                  \
      case ADD_COLLISION:                       \
        break;                                  \
    }                                           \
  }                                             \
}

#define HASHENTRY_GET(id) he->items[id]

void Kangaroo::hashentry_ReAllocate(HASH_ENTRY *he,uint32_t add) {

  he->maxItem += add;
  ENTRY** nitems = (ENTRY**)malloc(sizeof(ENTRY*) * he->maxItem);
  memcpy(nitems,he->items,sizeof(ENTRY*) * he->nbItem);
  free(he->items);
  he->items = nitems;

}

int Kangaroo::hashentry_compare(int128_t *i1,int128_t *i2) {

  uint64_t *a = i1->i64;
  uint64_t *b = i2->i64;

  if(a[1] == b[1]) {
    if(a[0] == b[0]) {
      return 0;
    } else {
      return (a[0] > b[0]) ? 1 : -1;
    }
  } else {
    return (a[1] > b[1]) ? 1 : -1;
  }

}

bool Kangaroo::hashentry_CollisionCheck(Int *distTame , Int *distWild) {

  Int Td;
  Int Wd;

  Td.Set(distTame);
  Wd.Set(distWild);

  endOfSearch = CheckKey(Td,Wd,0) || CheckKey(Td,Wd,1) || CheckKey(Td,Wd,2) || CheckKey(Td,Wd,3);

  if(!endOfSearch) {
    // Should not happen, reset the kangaroo
    ::printf("\n Unexpected wrong collision, reset kangaroo !\n");
    if((int64_t)(Td.bits64[3])<0) {
      Td.ModNegK1order();
      ::printf("Found: Td-%s\n",Td.GetBase16().c_str());
    } else {
      ::printf("Found: Td %s\n",Td.GetBase16().c_str());
    }
    if((int64_t)(Wd.bits64[3])<0) {
      Wd.ModNegK1order();
      ::printf("Found: Wd-%s\n",Wd.GetBase16().c_str());
    } else {
      ::printf("Found: Wd %s\n",Wd.GetBase16().c_str());
    }
    return false;
  }

  return true;

}

int Kangaroo::hashentry_Add(HASH_ENTRY *he,ENTRY* e) {

  if(he->maxItem == 0) {
    he->maxItem = 16;
    he->items = (ENTRY **)malloc(sizeof(ENTRY *) * he->maxItem);
  }

  if(he->nbItem == 0) {
    he->items[0] = e;
    he->nbItem = 1;
    return ADD_OK;
  }

  if(he->nbItem >= he->maxItem - 1) {
    // We need to reallocate
    hashentry_ReAllocate(he,4);
  }

  // Search insertion position
  int st,ed,mi;
  st = 0; ed = he->nbItem - 1;
  while(st <= ed) {
    mi = (st + ed) / 2;
    int comp = hashentry_compare(&e->x,&HASHENTRY_GET(mi)->x);
    if(comp<0) {
      ed = mi - 1;
    } else if (comp==0) {

      if((e->d.i64[0] == HASHENTRY_GET(mi)->d.i64[0]) && (e->d.i64[1] == HASHENTRY_GET(mi)->d.i64[1])) {
        // Same point added 2 times or collision in same herd !
        return ADD_DUPLICATE;
      }

      int128_t d = HASHENTRY_GET(mi)->d;
      uint32_t kType = (d.i64[1] & 0x4000000000000000ULL) != 0;
      int sign = (d.i64[1] & 0x8000000000000000ULL) != 0;
      d.i64[1] &= 0x3FFFFFFFFFFFFFFFULL;

      Int kDist;
      kDist.SetInt32(0);
      kDist.bits64[0] = d.i64[0];
      kDist.bits64[1] = d.i64[1];
      if(sign) kDist.ModNegK1order();

      Int dist;
      dist.SetInt32(0);
      uint32_t kType2 = (e->d.i64[1] & 0x4000000000000000ULL) != 0;
      int sign2 = (e->d.i64[1] & 0x8000000000000000ULL) != 0;
      dist.bits64[0] = e->d.i64[0];
      dist.bits64[1] = e->d.i64[1];
      dist.bits64[1] &= 0x3FFFFFFFFFFFFFFFULL;
      if(sign2) dist.ModNegK1order();
      if(kType2==TAME)
        hashentry_CollisionCheck(&dist,&kDist);
      else
        hashentry_CollisionCheck(&kDist,&dist);

      return ADD_COLLISION;

    } else {
      st = mi + 1;
    }
  }

  HASHENTRY_ADD_ENTRY(e);
  return ADD_OK;

}


void Kangaroo::hashentry_Save(FILE* f,HASH_ENTRY *he) {

  fwrite(&he->nbItem,sizeof(uint32_t),1,f);
  fwrite(&he->maxItem,sizeof(uint32_t),1,f);
  for(uint32_t i = 0; i < he->nbItem; i++) {
    fwrite(&(he->items[i]->x),16,1,f);
    fwrite(&(he->items[i]->d),16,1,f);
  }

}


void Kangaroo::hashentry_Reset(HASH_ENTRY *he) {

  if(he->items) {
    for(uint32_t i = 0; i<he->nbItem; i++)
      free(he->items[i]);
  }
  safe_free(he->items);
  he->maxItem = 0;
  he->nbItem = 0;

}


void Kangaroo::MergeWork(std::string& file1,std::string& file2,std::string& dest) {

  double t0;
  double t1;
  uint32_t v1;
  uint32_t v2;

  t0 = Timer::get_tick();

  // ---------------------------------------------------

  FILE* f1 = ReadHeader(file1,&v1);
  if(f1 == NULL)
    return;

  uint32_t dp1;
  Point k1;
  uint64_t count1;
  double time1;
  Int RS1;
  Int RE1;

  // Read global param
  ::fread(&dp1,sizeof(uint32_t),1,f1);
  ::fread(&RS1.bits64,32,1,f1); RS1.bits64[4] = 0;
  ::fread(&RE1.bits64,32,1,f1); RE1.bits64[4] = 0;
  ::fread(&k1.x.bits64,32,1,f1); k1.x.bits64[4] = 0;
  ::fread(&k1.y.bits64,32,1,f1); k1.y.bits64[4] = 0;
  ::fread(&count1,sizeof(uint64_t),1,f1);
  ::fread(&time1,sizeof(double),1,f1);

  k1.z.SetInt32(1);
  if(!secp->EC(k1)) {
    ::printf("MergeWork: key1 does not lie on elliptic curve\n");
    fclose(f1);
    return;
  }


  // ---------------------------------------------------

  FILE* f2 = ReadHeader(file2,&v2);
  if(f2 == NULL) {
    fclose(f1);
    return;
  }

  uint32_t dp2;
  Point k2;
  uint64_t count2;
  double time2;
  Int RS2;
  Int RE2;

  // Read global param
  ::fread(&dp2,sizeof(uint32_t),1,f2);
  ::fread(&RS2.bits64,32,1,f2); RS2.bits64[4] = 0;
  ::fread(&RE2.bits64,32,1,f2); RE2.bits64[4] = 0;
  ::fread(&k2.x.bits64,32,1,f2); k2.x.bits64[4] = 0;
  ::fread(&k2.y.bits64,32,1,f2); k2.y.bits64[4] = 0;
  ::fread(&count2,sizeof(uint64_t),1,f2);
  ::fread(&time2,sizeof(double),1,f2);

  if(v1 != v2) {
    ::printf("MergeWork: cannot merge workfile of different version\n");
    fclose(f1);
    fclose(f2);
    return;
  }

  k2.z.SetInt32(1);
  if(!secp->EC(k2)) {
    ::printf("MergeWork: key2 does not lie on elliptic curve\n");
    fclose(f1);
    fclose(f2);
    return;
  }

  if(!RS1.IsEqual(&RS2) || !RE1.IsEqual(&RE2)) {
    ::printf("MergeWork: File range differs\n");
    ::printf("RS1: %s\n",RS1.GetBase16().c_str());
    ::printf("RE1: %s\n",RE1.GetBase16().c_str());
    ::printf("RS2: %s\n",RS2.GetBase16().c_str());
    ::printf("RE2: %s\n",RE2.GetBase16().c_str());
    fclose(f1);
    fclose(f2);
    return;
  }

  if(!k1.equals(k2)) {
    ::printf("MergeWork: key differs, multiple keys not yet supported\n");
    fclose(f1);
    fclose(f2);
    return;
  }

  // Read hashTable (Only for STATS)
  HashTable* h2 = new HashTable();
  hashTable.SeekNbItem(f1,true);
  h2->SeekNbItem(f2,true);
  uint64_t nb1 = hashTable.GetNbItem();
  uint64_t nb2 = h2->GetNbItem();
  uint64_t totalItem = nb1+nb2;
  ::printf("%s: 2^%.2f DP [DP%d]\n",file1.c_str(),log2((double)nb1),dp1);
  ::printf("%s: 2^%.2f DP [DP%d]\n",file2.c_str(),log2((double)nb2),dp2);
  h2->Reset();

  endOfSearch = false;

  // Set starting parameters
  keysToSearch.clear();
  keysToSearch.push_back(k1);
  keyIdx = 0;
  collisionInSameHerd = 0;
  rangeStart.Set(&RS1);
  rangeEnd.Set(&RE1);
  InitRange();
  InitSearchKey();

  t0 = Timer::get_tick();

  ::printf("Merging");

  // Open output file
  string tmpName = dest + ".tmp";
  FILE* f = fopen(tmpName.c_str(),"wb");
  if(f == NULL) {
    ::printf("\nMergeWork: Cannot open %s for writing\n",tmpName.c_str());
    ::printf("%s\n",::strerror(errno));
    fclose(f1);
    fclose(f2);
    return;
  }
  dpSize = (dp1 < dp2) ? dp1 : dp2;
  if( !SaveHeader(tmpName,f,count1 + count2,time1 + time2) ) {
    fclose(f1);
    fclose(f2);
    return;
  }

  uint32_t nbItem;
  uint32_t maxItem;
  HASH_ENTRY he;
  he.nbItem = 0;
  he.maxItem = 0;

  for(uint32_t h = 0; h < HASH_SIZE && !endOfSearch; h++) {

    READ_ITEMS(f1);
    READ_ITEMS(f2);

    hashentry_Save(f,&he);
    hashentry_Reset(&he);
  }

  fclose(f1);
  fclose(f2);
  fclose(f);

  t1 = Timer::get_tick();

  if(!endOfSearch) {
    remove(dest.c_str());
    rename(tmpName.c_str(),dest.c_str());
    ::printf("Done [%s]\n",GetTimeStr(t1-t0).c_str());
  } else {
    // remove tmp file
    remove(tmpName.c_str());
  }

  ::printf("Dead kangaroo: %d\n",collisionInSameHerd);
  ::printf("Total f1+f2: count 2^%.2f [%s]\n",log2((double)count1 + (double)count2),GetTimeStr(time1 + time2).c_str());

}

typedef struct File {
    std::string name;
    std::string fullpath;
    uint64_t size;
    FILE* f;
} File;
bool sortBySize(const File &lhs, const File &rhs) { return lhs.size > rhs.size; }

void Kangaroo::MergeDir(std::string& dirname,std::string& dest) {

  uint32_t numMerged = 0;
  bool firstFile = true;
  uint32_t loadedItems = 0;
  double t0;
  double t1;
  uint32_t v, v1;
  std::string file;

  uint32_t dp;
  Point k;
  Int RS;
  Int RE;

  uint32_t dp1;
  Point k1;
  uint64_t count1;
  double time1;
  Int RS1;
  Int RE1;

  uint64_t sumCount = 0;
  double sumTime = 0;

  collisionInSameHerd = 0;
  endOfSearch = false;

  int nbCore = Timer::getCoreNumber();
  int l2 = (int)log2(nbCore);
  int nbThread = (int)pow(2.0,l2);
  int stride = HASH_SIZE / nbThread;

  TH_PARAM* params = (TH_PARAM*)malloc(nbThread * sizeof(TH_PARAM));
  THREAD_HANDLE* thHandles = (THREAD_HANDLE*)malloc(nbThread * sizeof(THREAD_HANDLE));

  // ---------------------------------------------------

  ::printf("[+] Loading directory: %s\n",dirname.c_str());


  DIR *dir;
  std::vector<File> files;
  File fileobj;
  struct dirent *ent;
  if ((dir = opendir(dirname.c_str())) != NULL) {
    while ((ent = readdir(dir)) != NULL) {
      if ( ent->d_type != 0x8) continue;
      fileobj.name = ent->d_name;
      fileobj.fullpath = dirname + "/" + ent->d_name;

      struct stat stat_buf;
      int rc = stat(fileobj.fullpath.c_str(), &stat_buf);
      fileobj.size = rc == 0 ? stat_buf.st_size : 0;
      files.push_back(fileobj);

    }
    std::sort(files.begin(), files.end(), sortBySize);

    for(int nFile = 0; nFile < files.size(); nFile++) {
      ::printf("Loading file: %s (%dMB)\n",files[nFile].name.c_str(),files[nFile].size/1024/1024);

      t0 = Timer::get_tick();

      files[nFile].f = ReadHeader(files[nFile].fullpath,&v1);
      if(files[nFile].f == NULL) {
        ::printf("MergeWork: Error opening %s\n",files[nFile].name.c_str());
        continue;
      }

      // Read global param
      ::fread(&dp1,sizeof(uint32_t),1,files[nFile].f);
      ::fread(&RS1.bits64,32,1,files[nFile].f); RS1.bits64[4] = 0;
      ::fread(&RE1.bits64,32,1,files[nFile].f); RE1.bits64[4] = 0;
      ::fread(&k1.x.bits64,32,1,files[nFile].f); k1.x.bits64[4] = 0;
      ::fread(&k1.y.bits64,32,1,files[nFile].f); k1.y.bits64[4] = 0;
      k1.z.SetInt32(1);
      ::fread(&count1,sizeof(uint64_t),1,files[nFile].f);
      ::fread(&time1,sizeof(double),1,files[nFile].f);

      if(firstFile) {
          dp = dp1;
          v = v1;
          RS = RS1;
          RE = RE1;
          k = k1;

          // Set starting parameters
          keysToSearch.clear();
          keysToSearch.push_back(k1);
          keyIdx = 0;
          rangeStart.Set(&RS1);
          rangeEnd.Set(&RE1);
          InitRange();
          InitSearchKey();
      }

      if(!firstFile && v1 != v) {
        ::printf("MergeWork %s: cannot merge workfile of different version\n",file.c_str());
        fclose(files[nFile].f);
        continue;
      }

      k1.z.SetInt32(1);
      if(!secp->EC(k1)) {
        ::printf("MergeWork %s: key1 does not lie on elliptic curve\n",file.c_str());
        fclose(files[nFile].f);
        continue;
      }

      if(!firstFile && (!RS.IsEqual(&RS1) || !RE.IsEqual(&RE1))) {
        ::printf("MergeWork: File range differs\n");
        ::printf("RS: %s\n",RS.GetBase16().c_str());
        ::printf("RE: %s\n",RE.GetBase16().c_str());
        ::printf("RS1: %s\n",RS1.GetBase16().c_str());
        ::printf("RE1: %s\n",RE1.GetBase16().c_str());
        fclose(files[nFile].f);
        continue;
      }

      if(!firstFile && !k.equals(k1)) {
        ::printf("MergeWork %s: key differs, multiple keys not yet supported\n",file.c_str());
        fclose(files[nFile].f);
        continue;
      }

      dpSize = (dpSize < dp1) ? dpSize : dp1;
      sumCount += count1;
      sumTime += time1;
      firstFile = false;
    }

    // Open output file
    string tmpName = dest + ".tmp";
    ::printf("\n[+] Merging files from %s into %s\n",dirname.c_str(),tmpName.c_str());
    FILE* f = fopen(tmpName.c_str(),"wb");
    if(f == NULL) {
      ::printf("\nMergeWork: Cannot open %s for writing\n",tmpName.c_str());
      ::printf("%s\n",::strerror(errno));
      for(int nFile = 0; nFile < files.size(); nFile++)
        fclose(files[nFile].f);
      return;
    }

    if( !SaveHeader(tmpName,f,sumCount,sumTime) ) {
      for(int nFile = 0; nFile < files.size(); nFile++)
        fclose(files[nFile].f);
      return;
    }

    uint32_t nbItem;
    uint32_t maxItem;
    HASH_ENTRY he;
    he.nbItem = 0;
    he.maxItem = 0;

    for(uint32_t h = 0; h < HASH_SIZE && !endOfSearch; h++) {
      for(int nFile = 0; nFile < files.size(); nFile++) {
        if(files[nFile].f == NULL)
          continue;

        READ_ITEMS(files[nFile].f);
      }
      hashentry_Save(f,&he);
      hashentry_Reset(&he);
    }

    t1 = Timer::get_tick();

    if(!endOfSearch) {
      remove(dest.c_str());
      rename(tmpName.c_str(),dest.c_str());
      ::printf("Done [%s]\n",GetTimeStr(t1-t0).c_str());
    } else {
      // remove tmp file
      remove(tmpName.c_str());
    }

    ::printf("Dead kangaroo: %d\n",collisionInSameHerd);
    ::printf("Total: count 2^%.2f [%s]\n",log2((double)sumCount),GetTimeStr(sumTime).c_str());

    for(int nFile = 0; nFile < files.size(); nFile++)
      fclose(files[nFile].f);
    closedir(dir);

  } else {
    perror ("");
    return;
  }

}
