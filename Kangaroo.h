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

#ifndef KANGAROOH
#define KANGAROOH

#ifdef WIN64
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <winsock2.h>
#else
typedef int SOCKET;
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <sys/types.h>
#include <unistd.h>
#include <sys/time.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <signal.h>
#endif

#include <string>
#include <vector>
#include "SECPK1/SECP256k1.h"
#include "HashTable.h"
#include "SECPK1/IntGroup.h"
#include "GPU/GPUEngine.h"

#ifdef WIN64
typedef HANDLE THREAD_HANDLE;
#define LOCK(mutex) WaitForSingleObject(mutex,INFINITE);
#define UNLOCK(mutex) ReleaseMutex(mutex);
#else
typedef pthread_t THREAD_HANDLE;
#define LOCK(mutex)  pthread_mutex_lock(&(mutex));
#define UNLOCK(mutex) pthread_mutex_unlock(&(mutex));
#endif

class Kangaroo;

// Input thread parameters
typedef struct {

  Kangaroo *obj;
  int  threadId;
  bool isRunning;
  bool hasStarted;
  bool isWaiting;
  uint64_t nbKangaroo;

#ifdef WITHGPU
  int  gridSizeX;
  int  gridSizeY;
  int  gpuId;
#endif

  Int *px; // Kangaroo position
  Int *py; // Kangaroo position
  Int *distance; // Travelled distance

#ifdef USE_SYMMETRY
  uint64_t *symClass; // Last jump
#endif

  SOCKET clientSock;
  char  *clientInfo;

  HashTable *h2;
  uint32_t hStart;
  uint32_t hStop;

} TH_PARAM;


// DP transfered over the network
typedef struct {

  uint32_t kIdx;
  uint32_t h;
  int128_t x;
  int128_t d;

} DP;

// DP cache
typedef struct {
  uint32_t nbDP;
  DP *dp;
} DP_CACHE;

// Wrok file type
#define HEADW 0xFA6A8001  // Full work file
#define HEADK 0xFA6A8002  // Kangaroo only file

class Kangaroo {

public:

  Kangaroo(Secp256K1 *secp,int32_t initDPSize,bool useGpu,std::string &workFile,std::string &iWorkFile,
           uint32_t savePeriod,bool saveKangaroo,double maxStep,int wtimeout,int sport,int ntimeout,
           std::string serverIp,std::string outputFile,bool splitWorkfile,std::string prvFile);
  void Run(int nbThread,std::vector<int> gpuId,std::vector<int> gridSize);
  void RunServer();
  bool ParseConfigFile(std::string &fileName);
  bool LoadWork(std::string &fileName);
  void Check(std::vector<int> gpuId,std::vector<int> gridSize);
  bool MergeWork(std::string &file1,std::string &file2,std::string &dest);
  void WorkInfo(std::string &fileName);

  // Threaded procedures
  void SolveKeyCPU(TH_PARAM *p);
  void SolveKeyGPU(TH_PARAM *p);
  bool HandleRequest(TH_PARAM *p);
  bool MergeTable(TH_PARAM* p);
  void ProcessServer();

  void AddConnectedClient();
  void RemoveConnectedClient();
  void RemoveConnectedKangaroo(uint64_t nb);

private:

  bool IsDP(uint64_t x);
  void SetDP(int size);
  void CreateHerd(int nbKangaroo,Int *px, Int *py, Int *d, int firstType,bool lock=true);
  void CreateJumpTable();
  bool AddToTable(uint64_t h,int128_t *x,int128_t *d);
  bool AddToTable(Int *pos,Int *dist,uint32_t kType);
  bool SendToServer(std::vector<ITEM> &dp);
  bool CheckKey(Int d1,Int d2,uint8_t type);
  bool CollisionCheck(Int *dist,uint32_t kType);
  bool savePrivkey(Int *pk);
  void ComputeExpected(double dp,double *op,double *ram,double* overHead = NULL);
  void InitRange();
  void InitSearchKey();
  std::string GetTimeStr(double s);
  bool Output(Int* pk,char sInfo,int sType);

  // Backup stuff
  void SaveWork(std::string fileName,FILE *f,int type,uint64_t totalCount,double totalTime);
  void SaveWork(uint64_t totalCount,double totalTime,TH_PARAM *threads,int nbThread);
  void SaveServerWork();
  void FetchWalks(uint64_t nbWalk,Int *x,Int *y,Int *d);
  void FectchKangaroos(TH_PARAM *threads);
  FILE *ReadHeader(std::string fileName,uint32_t *version,int type);
  bool  SaveHeader(std::string fileName,FILE* f,int type,uint64_t totalCount,double totalTime);
  int FSeek(FILE *stream,uint64_t pos);
  uint64_t FTell(FILE *stream);

  // Network stuff
  void AcceptConnections(SOCKET server_soc);
  int WaitFor(SOCKET sock,int timeout,int mode);
  int Write(SOCKET sock,char *buf,int bufsize,int timeout);
  int Read(SOCKET sock,char *buf,int bufsize,int timeout);
  bool GetConfigFromServer();
  bool ConnectToServer(SOCKET *retSock);
  void InitSocket();
  void WaitForServer();
  int32_t GetServerStatus();

#ifdef WIN64
  HANDLE ghMutex;
  HANDLE saveMutex;
  THREAD_HANDLE LaunchThread(LPTHREAD_START_ROUTINE func,TH_PARAM *p);
#else
  pthread_mutex_t  ghMutex;
  pthread_mutex_t  saveMutex;
  THREAD_HANDLE LaunchThread(void *(*func) (void *), TH_PARAM *p);
#endif

  void JoinThreads(THREAD_HANDLE *handles, int nbThread);
  void FreeHandles(THREAD_HANDLE *handles, int nbThread);
  void Process(TH_PARAM *params,std::string unit);

  uint64_t getCPUCount();
  uint64_t getGPUCount();
  bool isAlive(TH_PARAM *p);
  bool hasStarted(TH_PARAM *p);
  bool isWaiting(TH_PARAM *p);

  Secp256K1 *secp;
  HashTable hashTable;
  uint64_t counters[256];
  int  nbCPUThread;
  int  nbGPUThread;
  double startTime;

  // Range
  int rangePower;
  Int rangeStart;
  Int rangeEnd;
  Int rangeWidth;
  Int rangeWidthDiv2;
  Int rangeWidthDiv4;
  Int rangeWidthDiv8;

  uint64_t dMask;
  uint32_t dpSize;
  int32_t initDPSize;
  int collisionInSameHerd;
  std::vector<Point> keysToSearch;
  Point keyToSearch;
  Point keyToSearchNeg;
  uint32_t keyIdx;
  bool endOfSearch;
  bool useGpu;
  double expectedNbOp;
  double expectedMem;
  double maxStep;
  uint64_t totalRW;

  Int jumpDistance[NB_JUMP];
  Int jumpPointx[NB_JUMP];
  Int jumpPointy[NB_JUMP];

  int CPU_GRP_SIZE;

  // Backup stuff
  std::string outputFile;
  std::string prvFile;
  FILE *fRead;
  uint64_t offsetCount;
  double offsetTime;
  int64_t nbLoadedWalk;
  std::string workFile;
  std::string inputFile;
  int  saveWorkPeriod;
  bool saveRequest;
  bool saveKangaroo;
  int wtimeout;
  int ntimeout;
  bool splitWorkfile;

  // Network stuff
  int port;
  std::string lastError;
  std::string serverIp;
  char *hostInfo;
  int   hostInfoLength;
  int   hostAddrType;
  bool  clientMode;
  bool  isConnected;
  SOCKET serverConn;
  std::vector<DP_CACHE> recvDP;
  std::vector<DP_CACHE> localCache;
  std::string serverStatus;
  int connectedClient;

};

#endif // KANGAROOH
