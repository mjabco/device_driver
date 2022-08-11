////////////////////////////////////////////////////////////////////////////////
//
//  File           : sg_driver.c
//  Description    : This file contains the driver code to be developed by
//                   the students of the 311 class.  See assignment details
//                   for additional information.
//
//   Author        : Mike Jabco
//   Last Modified : 
//

// Include Files

// Project Includes
#include <sg_driver.h>

#include <sg_service.h>

#include <string.h>

#include <sg_cache.h>
 // Defines
#define BUFFSIZE 1065

//
// Global Data
uint32_t magic = SG_MAGIC_VALUE;

// Driver file entry

// Global data
int sgDriverInitialized = 0; // The flag indicating the driver initialized
SG_Block_ID sgLocalNodeId; // The local node identifier
SG_SeqNum sgLocalSeqno; // The local sequence number
char initpacket[1024]; //init packet of 1024
// Driver support functions
int sgInitEndpoint(void); // Initialize the endpoint

struct File { //data structure to handle file data 
  int fh; //filehandle
  int open; //read or not 0/1
  int filepointer; //current position in file
  int length; //length or size of file
  struct Block { //blocks per file..handles the block data in each file
    char datablock[1024];
    unsigned long nodeid;
    unsigned long blockid;
  };
  struct Block BlockArray[20];
  int blocks; //max 20 per file
};
struct File FileArray[1024];



struct node{
	unsigned long nodeid;
	int rseq;
	};
	struct node nodelist[500];
int nodelistpointer = 0; 
int nodelistend =0;
//int remoteseqno=10000;
int cacheinit = 0; //cache init or not 
int filecounter = 0; //file handle 
int blockcounter = 0;
float cachehits = 0.0;
float cachemisses = 0.0;
char * cachebuf;

// File system interface implementation

////////////////////////////////////////////////////////////////////////////////
//
// Function     : sgopen
// Description  : Open the file for for reading and writing
//
// Inputs       : path - the path/filename of the file to be read
// Outputs      : file handle if successful test, -1 if failure

SgFHandle sgopen(const char * path) {
  if (cacheinit == 0) {

    initSGCache(SG_MAX_CACHE_ELEMENTS);       //init cache if not already
  

    logMessage(LOG_ERROR_LEVEL, "CACHE INITIALIZED");
    cacheinit = 1;
  }

  // First check to see if we have been initialized
  if (!sgDriverInitialized) {

    // Call the endpoint initialization 
    if (sgInitEndpoint()) {
      logMessage(LOG_ERROR_LEVEL, "sgopen: Scatter/Gather endpoint initialization failed.");
      return (-1);
    }

    // Set to initialized
    sgDriverInitialized = 1;
  }

  // FILL IN THE REST OF THE CODE
  
  
  filecounter++; //increase file handle
  FileArray[filecounter].fh = filecounter;
  FileArray[filecounter].open = 1;
  FileArray[filecounter].filepointer = 0;
  FileArray[filecounter].length = 0;
  FileArray[filecounter].blocks = 0;
  //FileArray[filecounter].Block

  // Return the file handle 
  return (FileArray[filecounter].fh);

}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : sgread
// Description  : Read data from the file
//
// Inputs       : fh - file handle for the file to read from
//                buf - place to put the data
//                len - the length of the read
// Outputs      : number of bytes read, -1 if failure

int sgread(SgFHandle fh, char * buf, size_t len) {
  if (FileArray[fh].open == 0) {
    logMessage(LOG_ERROR_LEVEL, "SG READ FILE ALREADY CLOSED"); //check if file closed
    return (-1);
  }
  if (FileArray[fh].filepointer == FileArray[fh].length) {
    logMessage(LOG_ERROR_LEVEL, "SG READ filepointer is at end of file. reading beyond end"); //check if reading beyond file end
    return (-1);

  }
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Local variables
  char initPacket[SG_BASE_PACKET_SIZE], recvPacket[BUFFSIZE];
  size_t pktlen, rpktlen;
  SG_Node_ID loc, rem;
  SG_Block_ID blkid;
  SG_SeqNum sloc, srem;
  SG_System_OP op;
  SG_Packet_Status ret;
  //char block[SG_BLOCK_SIZE]="hello";

  // Setup the packet
  pktlen = SG_BASE_PACKET_SIZE;

  int blocknumber;
  blocknumber = FileArray[fh].filepointer / 1024;
  int used=0;
  for(int i=0; i<500;i++){
    	if(nodelist[i].nodeid==FileArray[fh].BlockArray[blocknumber].nodeid){
    		logMessage( LOG_ERROR_LEVEL, "match found" );
    		logMessage( LOG_ERROR_LEVEL, "this is rem %d",srem );
    		logMessage( LOG_ERROR_LEVEL, "this is node id from block %lu",FileArray[fh].BlockArray[blocknumber].nodeid );
    		nodelistpointer=i;
    		used=1;
    		break;
    		}
    		}
    	if(used==0){
    	logMessage( LOG_ERROR_LEVEL, "read NOT FOUND!!!!!!!!!!!!!!!!!!!!!!!!!!!" );}
  if ((ret = serialize_sg_packet(sgLocalNodeId, // Local ID
      FileArray[fh].BlockArray[blocknumber].nodeid, // Remote ID
      FileArray[fh].BlockArray[blocknumber].blockid, // Block ID
      SG_OBTAIN_BLOCK, // Operation obtain block
      sgLocalSeqno++, // Sender sequence number
      nodelist[nodelistpointer].rseq++, // Receiver sequence number
      NULL, initPacket, & pktlen)) != SG_PACKT_OK) {
    logMessage(LOG_ERROR_LEVEL, "sgInitEndpoint: READ! failed serialization of packet [%d].", ret);
    return (-1);
  }
 
  // Send the packet
  rpktlen = BUFFSIZE; //size 1065
  if (sgServicePost(initPacket, & pktlen, recvPacket, & rpktlen)) {
    logMessage(LOG_ERROR_LEVEL, "sgInitEndpoint: failed packet post");
    return (-1);
  }
  char block[1024]; //test block to copy
  
  // Unpack the recieived data
  if ((ret = deserialize_sg_packet( & loc, & rem, & blkid, & op, & sloc, &
      srem, block, recvPacket, rpktlen)) != SG_PACKT_OK) {
    logMessage(LOG_ERROR_LEVEL, "sgInitEndpoint: failed deserialization of packet [%d]", ret);
    return (-1);
  }
  int posinfile = FileArray[fh].filepointer % 1024;
 logMessage(LOG_ERROR_LEVEL, "this is block [%s]", block);
 logMessage(LOG_ERROR_LEVEL, "this is buf [%s]", buf);
 logMessage(LOG_ERROR_LEVEL, "psoinfile is [%d]", posinfile);
  
  if (posinfile == 0) {   
  //logMessage(LOG_ERROR_LEVEL, "psoinfile is [%d]", posinfile);                              //if at block edge

    memcpy(buf, block, 256);
    logMessage(LOG_ERROR_LEVEL, "this is buf [%s]", buf);
  }
  if (posinfile == 256) {				//if in middle of block
     //logMessage(LOG_ERROR_LEVEL, "psoinfile is [%d]", posinfile);
    memcpy(buf, block+256 , 256);//copy data to buffer
    logMessage(LOG_ERROR_LEVEL, "this is buf [%s]", buf);
    
  } 
  if (posinfile == 512) {				//if in middle of block
     //logMessage(LOG_ERROR_LEVEL, "posinfile is  [%d]", posinfile);
    memcpy(buf, block + 512, 256);//copy data to buffer
    logMessage(LOG_ERROR_LEVEL, "this is buf[%s]", buf);
  } 
   if (posinfile == 768) {				//if in middle of block
     //logMessage(LOG_ERROR_LEVEL, "posinfile is  [%d]", posinfile);
    memcpy(buf, block + 768, 256);//copy data to buffer
    logMessage(LOG_ERROR_LEVEL, "this is buf [%s]", buf);
  } 
  //call for get and put block, check hit or miss
  cachebuf = getSGDataBlock(FileArray[fh].BlockArray[blocknumber].nodeid, FileArray[fh].BlockArray[blocknumber].blockid);
  if (cachebuf == NULL) {
    cachemisses++;
  } else {
    cachehits++;
  }
  putSGDataBlock(FileArray[fh].BlockArray[blocknumber].nodeid, FileArray[fh].BlockArray[blocknumber].blockid, buf);
  
  FileArray[fh].filepointer += len; //filepointer now is increased len
  logMessage(LOG_ERROR_LEVEL, "this is block after read[%s]", block);
 logMessage(LOG_ERROR_LEVEL, "this is buf after read[%s]", buf);
  // Return the bytes processed
  return (len);
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : sgwrite
// Description  : write data to the file
//
// Inputs       : fh - file handle for the file to write to
//                buf - pointer to data to write
//                len - the length of the write
// Outputs      : number of bytes written if successful test, -1 if failure

int sgwrite(SgFHandle fh, char * buf, size_t len) {
  char initpacket[BUFFSIZE];
  size_t pktlen, rpktlen;
  char updaterecvPacket[BUFFSIZE];
  char recvPacket[SG_BASE_PACKET_SIZE];
  
  SG_Packet_Status ret;
  SG_Node_ID loc, rem;
  SG_Block_ID blkid;
  SG_SeqNum sloc, srem;
  SG_System_OP op;
  char block[SG_BLOCK_SIZE];
  int blocknumber = FileArray[fh].filepointer / 1024;
  char obtaininitPacket[SG_BASE_PACKET_SIZE];

  if (FileArray[fh].open == 0) {
    logMessage(LOG_ERROR_LEVEL, "SG_WRITE FILE ALREADY CLOSED");
    return (-1);
  }
 

  int posinfile = FileArray[fh].filepointer % 1024;
  
    logMessage(LOG_ERROR_LEVEL, "this is original buf in write [%s]", buf);
    logMessage(LOG_ERROR_LEVEL, "posinfile is %d", posinfile);
  if (FileArray[fh].filepointer < FileArray[fh].length || posinfile == 256 || posinfile == 512 || posinfile == 768) {
  
    int used =0;
    for(int i=0; i<500;i++){
    	if(nodelist[i].nodeid==FileArray[fh].BlockArray[blocknumber].nodeid){
    		logMessage( LOG_ERROR_LEVEL, "match found" );
    		nodelistpointer=i;
    		used =1;
    		}
    		}
    		
    	if(used==0){
    	logMessage( LOG_ERROR_LEVEL, "write NOT FOUND!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!" );	}
    		
    		
    //nodelist[nodelistpointer].rseq++;
    logMessage( LOG_ERROR_LEVEL, "we are updating a block");
    if ((ret = serialize_sg_packet(sgLocalNodeId, // Local ID
        FileArray[fh].BlockArray[blocknumber].nodeid, // Remote ID
        FileArray[fh].BlockArray[blocknumber].blockid, // Block ID
        SG_OBTAIN_BLOCK, // Operation obtain block
        sgLocalSeqno++, // Sender sequence number
        nodelist[nodelistpointer].rseq++, // Receiver sequence number
        NULL, obtaininitPacket, & pktlen)) != SG_PACKT_OK) {
      logMessage( LOG_ERROR_LEVEL, "sgInitEndpoint: WRITE! failed serialization of packet [%d].", ret );
      return (-1);
    }

    // Send the packet
    rpktlen = BUFFSIZE; //size 1065
    if (sgServicePost(obtaininitPacket, & pktlen, updaterecvPacket, & rpktlen)) {
      logMessage(LOG_ERROR_LEVEL, "sgInitEndpoint: failed packet post");
      return (-1);
    }
    char block[SG_BLOCK_SIZE] = "hello"; //test block to copy
    // Unpack the recieived data
    if ((ret = deserialize_sg_packet( & loc, & rem, & blkid, & op, & sloc, &
        srem, block, updaterecvPacket, rpktlen)) != SG_PACKT_OK) {
      logMessage(LOG_ERROR_LEVEL, "sgInitEndpoint: failed deserialization of packet [%d]", ret);
      return (-1);
    }
    char test[1024];
    logMessage(LOG_ERROR_LEVEL, "this is block in write [%s]", block);
    logMessage(LOG_ERROR_LEVEL, "this is original buf in write [%s]", buf);
    logMessage(LOG_ERROR_LEVEL, "posinfile is %d", posinfile);
    if (posinfile ==  256) {
    //logMessage(LOG_ERROR_LEVEL, "posinfile is %d", posinfile);
    //logMessage(LOG_ERROR_LEVEL, "this is buf [%s]", buf);
      //logMessage(LOG_ERROR_LEVEL, "this is block [%s]", block);
      memcpy(test, block, 1024);//save correct half of block
      //logMessage(LOG_ERROR_LEVEL, "this is test [%s]", test);
      memcpy(test + 256, buf, len);//put new corect half into test block
      //logMessage(LOG_ERROR_LEVEL, "this is test [%s]", test);
      //logMessage(LOG_ERROR_LEVEL, "this is block [%s]", block);
      memcpy(buf, test, 1024);//copy over correct 1024 to buf
      logMessage(LOG_ERROR_LEVEL, "this is buf in write [%s]", buf);
      //logMessage(LOG_ERROR_LEVEL, "this is buf [%s]", buf);
     }
    if (posinfile ==  768) {
    //logMessage(LOG_ERROR_LEVEL, "posinfile is %d", posinfile);
      //logMessage(LOG_ERROR_LEVEL, "this is buf [%s]", buf);
      //logMessage(LOG_ERROR_LEVEL, "this is block [%s]", block);
      memcpy(test, block, 1024);//save correct half of block
      //logMessage(LOG_ERROR_LEVEL, "this is test [%s]", test);
      memcpy(test+768, buf, len);//put new corect half into test block
      //logMessage(LOG_ERROR_LEVEL, "this is test [%s]", test);
      //logMessage(LOG_ERROR_LEVEL, "this is block [%s]", block);
      memcpy(buf, test, 1024);//copy over correct 1024 to buf
      logMessage(LOG_ERROR_LEVEL, "this is buf in write[%s]", buf);
     } 
   
    if (posinfile ==  512) {
    //logMessage(LOG_ERROR_LEVEL, "posinfile is %d", posinfile);
      //logMessage(LOG_ERROR_LEVEL, "this is buf [%s]", buf);
      //logMessage(LOG_ERROR_LEVEL, "this is block [%s]", block);
      memcpy(test, block, 1024);//save correct half of block
      //logMessage(LOG_ERROR_LEVEL, "this is test [%s]", test);
      memcpy(test+512, buf, len);//put new corect half into test block
      //logMessage(LOG_ERROR_LEVEL, "this is test [%s]", test);
      //logMessage(LOG_ERROR_LEVEL, "this is block [%s]", block);
      memcpy(buf, test, 1024);//copy over correct 1024 to buf
      logMessage(LOG_ERROR_LEVEL, "this is buf in write[%s]", buf);
     }
     
    if (posinfile == 0) { //if on the edge of block copy new data over and save old 
     //logMessage(LOG_ERROR_LEVEL, "posinfile is %d", posinfile);
     //logMessage(LOG_ERROR_LEVEL, "this is buf [%s]", buf);
     // logMessage(LOG_ERROR_LEVEL, "this is block [%s]", block);
      memcpy(test, buf, len);//save correct half of block
      //logMessage(LOG_ERROR_LEVEL, "this is test [%s]", test);
      memcpy(test+256, block+256, 1024);//put new corect half into test block
      //logMessage(LOG_ERROR_LEVEL, "this is test [%s]", test);
      //logMessage(LOG_ERROR_LEVEL, "this is block [%s]", block);
      memcpy(buf, test, 1024);//copy over correct 1024 to buf
      logMessage(LOG_ERROR_LEVEL, "this is buf in write [%s]", buf);
    }
    //nodelist[nodelistpointer].rseq++;
    if ((ret = serialize_sg_packet(sgLocalNodeId, // Local ID
        FileArray[fh].BlockArray[blocknumber].nodeid, // Remote ID
        FileArray[fh].BlockArray[blocknumber].blockid, // Block ID
        SG_UPDATE_BLOCK, // Operation
        sgLocalSeqno++, // Sender sequence number
        nodelist[nodelistpointer].rseq++, // Receiver sequence number
        buf, initpacket, & pktlen)) != SG_PACKT_OK) {
        logMessage(LOG_ERROR_LEVEL, "sgInitEndpoint: READ failed deserialization of packet [%d]", ret);
     
      return (-1);
    }
    //call for get and put block, check hit or miss
     cachebuf = getSGDataBlock(FileArray[fh].BlockArray[blocknumber].nodeid, FileArray[fh].BlockArray[blocknumber].blockid);
  if (cachebuf == NULL) {
    cachemisses++;
  } else {
    cachehits++;
  }
  putSGDataBlock(FileArray[fh].BlockArray[blocknumber].nodeid, FileArray[fh].BlockArray[blocknumber].blockid, buf);
   

    // Send the packet
    rpktlen = SG_BASE_PACKET_SIZE;
    if (sgServicePost(initpacket, & pktlen, recvPacket, & rpktlen)) {
      logMessage(LOG_ERROR_LEVEL, "sgInitEndpoint: failed packet post");
      return (-1);
    }
    if ((ret = deserialize_sg_packet( & loc, & rem, & blkid, & op, & sloc, &
        srem, NULL, recvPacket, rpktlen)) != SG_PACKT_OK) {
      logMessage(LOG_ERROR_LEVEL, "sgInitEndpoint: READ failed deserialization of packet [%d]", ret);
      return (-1);

    }

   
//call for get and put block, check hit or miss 
    cachebuf = getSGDataBlock(FileArray[fh].BlockArray[blocknumber].nodeid, FileArray[fh].BlockArray[blocknumber].blockid);
    if (cachebuf == NULL) {
      cachemisses++;

    } else {
      cachehits++;

    }
    putSGDataBlock(FileArray[fh].BlockArray[blocknumber].nodeid, FileArray[fh].BlockArray[blocknumber].blockid, buf);//cpy final buf

    if (FileArray[fh].filepointer == FileArray[fh].length) {
      FileArray[fh].length += len;
    }

    FileArray[fh].filepointer += len;
    
  } 
  
  
  
  //Create block if not updating block
  else if (posinfile == 0 && FileArray[fh].filepointer == FileArray[fh].length) {
    

    //..set op value to create block value listed. pass buf in as block*/
    pktlen = BUFFSIZE;
    if ((ret = serialize_sg_packet(sgLocalNodeId, // Local ID
        SG_NODE_UNKNOWN, // Remote ID
        SG_BLOCK_UNKNOWN, // Block ID
        SG_CREATE_BLOCK, // Operation
        sgLocalSeqno++, // Sender sequence number
        SG_SEQNO_UNKNOWN, // Receiver sequence number
        buf, initpacket, & pktlen)) != SG_PACKT_OK) {
      logMessage(LOG_ERROR_LEVEL, "sgInitEndpointzz: failed serialization of packet [%d].", ret);
      return (-1);
    }
    memcpy(block, buf, 1024);
    logMessage(LOG_ERROR_LEVEL, "this is block in write [%s]", block);
    logMessage(LOG_ERROR_LEVEL, "this is original buf in write [%s]", buf);
    logMessage(LOG_ERROR_LEVEL, "posinfile is %d", posinfile);
    //logMessage( LOG_ERROR_LEVEL, "this is the block currently after create[%s]", block);
    // Send the packet
    rpktlen = SG_BASE_PACKET_SIZE;
    if (sgServicePost(initpacket, & pktlen, recvPacket, & rpktlen)) {
      logMessage(LOG_ERROR_LEVEL, "sgInitEndpoint: failed packet post");
      return (-1);
    }
    if ((ret = deserialize_sg_packet( & loc, & rem, & blkid, & op, & sloc, &
        srem, NULL, recvPacket, rpktlen)) != SG_PACKT_OK) {
      logMessage(LOG_ERROR_LEVEL, "sgInitEndpoint: READ failed deserialization of packet [%d]", ret);
      return (-1);
    }
    
    int inlist=0;
    for(int i =0; i<500; i++){
    	if(nodelist[i].nodeid == rem){
    	logMessage(LOG_ERROR_LEVEL, "this is nodeid rem %lu", rem);
    	logMessage(LOG_ERROR_LEVEL, "this is .nodeid %lu", nodelist[i].nodeid);
    		nodelist[i].rseq++;
    	logMessage(LOG_ERROR_LEVEL, "this is nodelist[i].rseq %d", nodelist[i].rseq);
    		
    		inlist = 1;
    		//nodelistpointer=i;
    		break;
    		}
    		}
    	if(inlist==0){
    	nodelist[nodelistend].nodeid=rem;
    	nodelist[nodelistend].rseq=srem;
    	nodelist[nodelistend].rseq++;
    	logMessage(LOG_ERROR_LEVEL, "THIS IS RSEQ %d", srem); //check if file closed
    	logMessage(LOG_ERROR_LEVEL, "THIS IS node.RSEQ %d", nodelist[nodelistpointer].rseq); //check if file closed
    	nodelistend++;
    	nodelistpointer=nodelistend;
    	logMessage(LOG_ERROR_LEVEL, "THIS IS len %d", nodelistend);
    	}
    	
     //remoteseqno = srem;
    //increase file size by length,
    FileArray[fh].length += len;
    FileArray[fh].filepointer += len;
    FileArray[fh].BlockArray[blocknumber].nodeid = rem;
    FileArray[fh].BlockArray[blocknumber].blockid = blkid;
    
    
  } else {
    logMessage(LOG_ERROR_LEVEL, "FAILURE");
    logMessage(LOG_ERROR_LEVEL, "this is filepointer %d", FileArray[fh].filepointer);
    logMessage(LOG_ERROR_LEVEL, "this is posinfile %d", posinfile);
    logMessage(LOG_ERROR_LEVEL, "this is length %d", FileArray[fh].length);
    return (-1);
  }
  
  // Log the write, return bytes written
  return (len);
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : sgseek
// Description  : Seek to a specific place in the file
//
// Inputs       : fh - the file handle of the file to seek in
//                off - offset within the file to seek to
// Outputs      : new position if successful, -1 if failure

int sgseek(SgFHandle fh, size_t off) {
  if (FileArray[fh].open == 0) {
    logMessage(LOG_ERROR_LEVEL, "SG_SEEK FILE ALREADY CLOSED");
    return (-1);
  }
  if (off <= FileArray[fh].length) { //if filepointer greater than offset..set equal

    FileArray[fh].filepointer = off;
  } else {
    logMessage(LOG_ERROR_LEVEL, "SG_SEEK offset > length");
    return (-1);
  }

  // Return new position
  return (off);
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : sgclose
// Description  : Close the file
//
// Inputs       : fh - the file handle of the file to close
// Outputs      : 0 if successful test, -1 if failure

int sgclose(SgFHandle fh) {
  if (FileArray[fh].open == 0) {
    logMessage(LOG_ERROR_LEVEL, "File is already closed");
    return (-1);
  }
  FileArray[fh].open = 0;
  // Return successfully
  return (0);
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : sgshutdown
// Description  : Shut down the filesystem
//
// Inputs       : none
// Outputs      : 0 if successful test, -1 if failure

int sgshutdown(void) {

  //send close endpoint request and close all files open
  char initPacket[SG_BASE_PACKET_SIZE], recvPacket[SG_BASE_PACKET_SIZE];
  size_t pktlen, rpktlen;
  SG_Node_ID loc, rem;
  SG_Block_ID blkid;
  SG_SeqNum sloc, srem;
  SG_System_OP op;
  SG_Packet_Status ret;
  pktlen = SG_BASE_PACKET_SIZE;
  if ((ret = serialize_sg_packet(sgLocalNodeId, // Local ID
      SG_NODE_UNKNOWN, // Remote ID
      SG_BLOCK_UNKNOWN, // Block ID
      SG_STOP_ENDPOINT, // Operation
      sgLocalSeqno++, // Sender sequence number
      SG_SEQNO_UNKNOWN, // Receiver sequence number
      NULL, initPacket, & pktlen)) != SG_PACKT_OK) {
    logMessage(LOG_ERROR_LEVEL, "sgInitEndpointzz: failed serialization of packet [%d].", ret);
    return (-1);
  }

  // Send the packet
  rpktlen = SG_BASE_PACKET_SIZE;
  if (sgServicePost(initPacket, & pktlen, recvPacket, & rpktlen)) {
    logMessage(LOG_ERROR_LEVEL, "sgInitEndpoint: failed packet post");
    return (-1);
  }

  // Unpack the recieived data
  if ((ret = deserialize_sg_packet( & loc, & rem, & blkid, & op, & sloc, &
      srem, NULL, recvPacket, rpktlen)) != SG_PACKT_OK) {
    logMessage(LOG_ERROR_LEVEL, "sgInitEndpoint: failed deserialization of packet [%d]", ret);
    return (-1);
  }

  // Log, return successfully
  logMessage(LOG_INFO_LEVEL, "Shut down Scatter/Gather driver.");
  
  
  closeSGCache(); //close cache clear data
  logMessage(LOG_OUTPUT_LEVEL, "Closing cache: %.0f queries, %.0f hits, %.0f misses (%.2f hit rate)", cachehits + cachemisses, cachehits, cachemisses, ((cachehits) / (cachehits + cachemisses) * 100)); //calc hit rate and close cache
  return (0);
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : serialize_sg_packet
// Description  : Serialize a ScatterGather packet (create packet)
//
// Inputs       : loc - the local node identifier
//                rem - the remote node identifier
//                blk - the block identifier
//                op - the operation performed/to be performed on block
//                sseq - the sender sequence number
//                rseq - the receiver sequence number
//                data - the data block (of size SG_BLOCK_SIZE) or NULL
//                packet - the buffer to place the data
//                plen - the packet length (int bytes)
// Outputs      : 0 if successfully created, -1 if failure

SG_Packet_Status serialize_sg_packet(SG_Node_ID loc, SG_Node_ID rem, SG_Block_ID blk,
  SG_System_OP op, SG_SeqNum sseq, SG_SeqNum rseq, char * data,
  char * packet, size_t * plen) {

  if (loc == 0) {

    return (1);
  }
  if (rem == 0) {

    return (2);
  }
  if (blk == 0) {

    return (3);
  }
  if (op < 0 || op > 6) {

    return (4);
  }
  if (sseq == 0) {

    return (5);
  }
  if (rseq == 0) {

    return (6);
  }

  memcpy( & packet[0], & magic, sizeof(magic));
  memcpy( & packet[4], & loc, sizeof(loc));
  memcpy( & packet[12], & rem, sizeof(rem));
  memcpy( & packet[20], & blk, sizeof(blk));
  memcpy( & packet[28], & op, sizeof(op));
  memcpy( & packet[32], & sseq, sizeof(sseq));
  memcpy( & packet[34], & rseq, sizeof(rseq));

  if (data == NULL) {
    * plen = 41;
    packet[36] = 0;

    memcpy( & packet[37], & magic, sizeof(magic));
  } else {
    packet[36] = 1;
    * plen = 1065;

    memcpy( & packet[37], data, 1024);
    memcpy( & packet[37 + 1024], & magic, sizeof(magic));
  }

  return (0);
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : deserialize_sg_packet
// Description  : De-serialize a ScatterGather packet (unpack packet)
//
// Inputs       : loc - the local node identifier
//                rem - the remote node identifier
//                blk - the block identifier
//                op - the operation performed/to be performed on block
//                sseq - the sender sequence number
//                rseq - the receiver sequence number
//                data - the data block (of size SG_BLOCK_SIZE) or NULL
//                packet - the buffer to place the data
//                plen - the packet length (int bytes)
// Outputs      : 0 if successfully created, -1 if failure

SG_Packet_Status deserialize_sg_packet(SG_Node_ID * loc, SG_Node_ID * rem, SG_Block_ID * blk,
  SG_System_OP * op, SG_SeqNum * sseq, SG_SeqNum * rseq, char * data,
  char * packet, size_t plen) {

  // REPLACE THIS CODE WITH YOU ASSIGNMENT #2

  memcpy( & magic, & packet[0], sizeof(magic));
  memcpy(loc, & packet[4], sizeof( * loc));

  memcpy(rem, & packet[12], sizeof( * rem));
  memcpy(blk, & packet[20], sizeof( * blk));
  memcpy(op, & packet[28], sizeof( * op));
  memcpy(sseq, & packet[32], sizeof( * sseq));
  memcpy(rseq, & packet[34], sizeof( * rseq));

  if (data == NULL) {
    memcpy( & magic, & packet[37], sizeof(magic));
    plen = 41;

  } else {
    memcpy(data, & packet[37], 1024);
    memcpy( & magic, & packet[37 + 1024], sizeof(magic));
    plen = 1065;

  }
  if ( * loc == 0) {

    return (1);
  }
  if ( * rem == 0) {

    return (2);
  }
  if ( * blk == 0) {

    return (3);
  }
  if ( * op < 0 || * op > 6) {

    return (4);
  }
  if ( * sseq == 0) {

    return (5);
  }
  if ( * rseq == 0) {

    return (6);
  }

  return (0);
}

//
// Driver support functions

////////////////////////////////////////////////////////////////////////////////
//
// Function     : sgInitEndpoint
// Description  : Initialize the endpoint
//
// Inputs       : none
// Outputs      : 0 if successfull, -1 if failure

int sgInitEndpoint(void) {

  // Local variables
  char initPacket[SG_BASE_PACKET_SIZE], recvPacket[SG_BASE_PACKET_SIZE];
  size_t pktlen, rpktlen;
  SG_Node_ID loc, rem;
  SG_Block_ID blkid;
  SG_SeqNum sloc, srem;
  SG_System_OP op;
  SG_Packet_Status ret;

  // Local and do some initial setup
  logMessage(LOG_INFO_LEVEL, "Initializing local endpoint ...");
  sgLocalSeqno = SG_INITIAL_SEQNO;

  // Setup the packet
  pktlen = SG_BASE_PACKET_SIZE;
  if ((ret = serialize_sg_packet(SG_NODE_UNKNOWN, // Local ID
      SG_NODE_UNKNOWN, // Remote ID
      SG_BLOCK_UNKNOWN, // Block ID
      SG_INIT_ENDPOINT, // Operation
      sgLocalSeqno++, // Sender sequence number
      SG_SEQNO_UNKNOWN, // Receiver sequence number
      NULL, initPacket, & pktlen)) != SG_PACKT_OK) {
    logMessage(LOG_ERROR_LEVEL, "sgInitEndpointzz: failed serialization of packet [%d].", ret);
    return (-1);
  }

  // Send the packet
  rpktlen = SG_BASE_PACKET_SIZE;
  if (sgServicePost(initPacket, & pktlen, recvPacket, & rpktlen)) {
    logMessage(LOG_ERROR_LEVEL, "sgInitEndpoint: failed packet post");
    return (-1);
  }

  // Unpack the recieived data
  if ((ret = deserialize_sg_packet( & loc, & rem, & blkid, & op, & sloc, &
      srem, NULL, recvPacket, rpktlen)) != SG_PACKT_OK) {
    logMessage(LOG_ERROR_LEVEL, "sgInitEndpoint: failed deserialization of packet [%d]", ret);
    return (-1);
  }

  // Sanity check the return value
  if (loc == SG_NODE_UNKNOWN) {
    logMessage(LOG_ERROR_LEVEL, "sgInitEndpoint: bad local ID returned [%ul]", loc);
    return (-1);
  }

  // Set the local node ID, log and return successfully
  sgLocalNodeId = loc;
  logMessage(LOG_INFO_LEVEL, "Completed initialization of node (local node ID %lu", sgLocalNodeId);
  return (0);
}

