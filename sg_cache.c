////////////////////////////////////////////////////////////////////////////////
//
//  File           : sg_driver.c
//  Description    : This file contains the driver code to be developed by
//                   the students of the 311 class.  See assignment details
//                   for additional information.
//
//   Author        : YOUR NAME
//   Last Modified : 
//

// Include Files
#include <stdlib.h>

#include <cmpsc311_log.h>

#include <string.h>

// Project Includes
#include <sg_cache.h>

// Defines

// Functional Prototypes
#define blocksize 1024

//
// Functions
uint32_t maxE; //max elements

struct cache {
  int accessedtime; //track of time since last accessed
  int accessed; //0 if empty
  unsigned long nodeid;
  unsigned long blockid;
  char data[blocksize]; //data copied in 
};
struct cache CacheArray[SG_MAX_CACHE_ELEMENTS];

int cacheline = 0;
int timer = 0; //time of accessed
int LRU; //check for the last used 
int LU; //last used instance in array
////////////////////////////////////////////////////////////////////////////////
//
// Function     : initSGCache
// Description  : Initialize the cache of block elements
//
// Inputs       : maxElements - maximum number of elements allowed
// Outputs      : 0 if successful, -1 if failure

int initSGCache(uint16_t maxElements) {
  for (int i = 0; i < maxElements; i++) {
    CacheArray[i].accessedtime = 0;
    CacheArray[i].nodeid = 0;
    CacheArray[i].blockid = 0;
    CacheArray[i].accessed = 0;

  }
  if (maxE != maxElements) {
    return (-1);
  }
  logMessage(LOG_ERROR_LEVEL, "Cache is initialized");
  // Return successfully
  return (0);
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : closeSGCache
// Description  : Close the cache of block elements, clean up remaining data
//
// Inputs       : none
// Outputs      : 0 if successful, -1 if failure

int closeSGCache(void) {
  for (int i = 0; i < SG_MAX_CACHE_ELEMENTS; i++) {
    CacheArray[i].accessedtime = 0;
    CacheArray[i].nodeid = 0;
    CacheArray[i].blockid = 0;
    CacheArray[i].accessed = 0;
    memset(CacheArray[i].data, 0, 1024);
  }
  // Return successfully
  return (0);
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : getSGDataBlock
// Description  : Get the data block from the block cache
//
// Inputs       : nde - node ID to find
//                blk - block ID to find
// Outputs      : pointer to block or NULL if not found

char * getSGDataBlock(SG_Node_ID nde, SG_Block_ID blk) {
  timer++;
  void * cacheptr = NULL;
  for (int i = 0; i < SG_MAX_CACHE_ELEMENTS; i++) {
    if (CacheArray[i].nodeid == nde && CacheArray[i].blockid == blk) {
      CacheArray[i].accessedtime = timer;
      cacheptr = & CacheArray[i].data;
      return (cacheptr);
    }
  }
  return (NULL);
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : putSGDataBlock
// Description  : Get the data block from the block cache
//
// Inputs       : nde - node ID to find
//                blk - block ID to find
//                block - block to insert into cache
// Outputs      : 0 if successful, -1 if failure

int putSGDataBlock(SG_Node_ID nde, SG_Block_ID blk, char * block) {
  int full;
  timer++; //timer incremented each time 
  for (int i = 0; i < SG_MAX_CACHE_ELEMENTS; i++) { //if it is in cache already, update the data 

    if (CacheArray[i].nodeid == nde && CacheArray[i].blockid == blk) {
      full = 0;
      CacheArray[i].nodeid = nde;
      CacheArray[i].blockid = blk;
      memcpy(CacheArray[i].data, block, 1024);
      CacheArray[i].accessedtime = timer;
      return (0);
    }
  }

  for (int i = 0; i < SG_MAX_CACHE_ELEMENTS; i++) { //if cache is not full find the first open slot and place it there
    if (CacheArray[i].accessed == 0) {
      full = 0; //if accessed is 0 it is not accessed yet
      CacheArray[i].accessed = 1; //now accessed
      CacheArray[i].accessedtime = timer; //update timer for cache line
      CacheArray[i].nodeid = nde;
      CacheArray[i].blockid = blk;
      memcpy(CacheArray[i].data, block, 1024);
      return (0);
    }
  }
  //if cache is full and there is not an exiating slot find LRU and replace it 
  //initialize least recently used
  LRU = CacheArray[0].accessedtime;//set LRU equal to an item in cache..if any were accessed sooner theyre removed, if not [0] is the LRU
  for (int i = 0; i < SG_MAX_CACHE_ELEMENTS; i++) {
    if (CacheArray[i].accessedtime <= LRU) {
      full = 1;
      LRU = CacheArray[i].accessedtime;
      LU = i;
    }

  }
  if (full == 1) {
    CacheArray[LU].accessedtime = timer;
    CacheArray[LU].nodeid = nde;
    CacheArray[LU].blockid = blk;
    memcpy(CacheArray[LU].data, block, 1024);
    //return(0);
  }

  return (0);
}
