#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)                                              \
    {                                                          \
        if (!(c))                                              \
        {                                                      \
            cerr << "At line " << __LINE__ << ":" << endl      \
                    << "  ";                                      \
            cerr << "This condition should hold: " #c << endl; \
            exit(1);                                           \
        }                                                      \
    }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs)
{
    numBufs = bufs;

    bufTable = new BufDesc[bufs];
    memset(bufTable, 0, bufs * sizeof(BufDesc));
    for (int i = 0; i < bufs; i++)
    {
        bufTable[i].frameNo = i;
        bufTable[i].valid = false;
    }

    bufPool = new Page[bufs];
    memset(bufPool, 0, bufs * sizeof(Page));

    int htsize = ((((int)(bufs * 1.2)) * 2) / 2) + 1;
    hashTable = new BufHashTbl(htsize); // allocate the buffer hash table

    clockHand = bufs - 1;
}

BufMgr::~BufMgr()
{

    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++)
    {
        BufDesc *tmpbuf = &bufTable[i];
        if (tmpbuf->valid == true && tmpbuf->dirty == true)
        {

#ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo
                    << " from frame " << i << endl;
#endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete[] bufTable;
    delete[] bufPool;
}

const Status BufMgr::allocBuf(int &frame)
{
    int count = 0;
    Status status;

    // Begin the clock algorithm
    while (count < 2 * (int)numBufs) { // While the count is less than 2 * the number of buffers
        advanceClock(); // Advance the clock hand

        if (bufTable[clockHand].valid == false) { // If the frame is not valid
            frame = clockHand;

            return OK;
        } else { // If it is valid Use clock algorithm
            if (bufTable[clockHand].refbit == true) { // If the refbit is true
                bufTable[clockHand].refbit = false;
                count++;

                continue;
            } else { // If the refbit is false
                if (bufTable[clockHand].pinCnt != 0) {
                    count++;

                    continue;
                } else { // If the pin count is 0
                    if (bufTable[clockHand].dirty == true) { // If the page is dirty
                        status = bufTable[clockHand].file->writePage(bufTable[clockHand].pageNo, &(bufPool[clockHand])); // Write the page to disk
                        
                        if (status != OK) { // If the status is not OK
                            return UNIXERR;
                        }

                        // Remove the entry from the hash table
                        bufTable[clockHand].dirty = false;
                        bufStats.diskwrites++;
                    }
                        frame = clockHand; // Set the frame to the clock hand
                        hashTable->remove(bufTable[clockHand].file, bufTable[clockHand].pageNo); // Remove the entry from the hash table
                        bufTable[clockHand].Clear(); // Clear the buffer table

                        return OK;
                }
            }
        }
    }

    return BUFFEREXCEEDED;
}

const Status BufMgr::readPage(File *file, const int PageNo, Page *&page)
{
    // Variable initializations
    Status fileStatus = OK;
    Status hashStatus = OK;
    Status insertStatus = OK;
    Status allocStatus = OK;
    int freeframe;
    int frameNo = 0;

    hashStatus = hashTable->lookup(file, PageNo, frameNo); // Check if the page is in the buffer pool

    if (hashStatus == HASHNOTFOUND) { // Case 1: If the page is not in the buffer pool
        allocStatus = allocBuf(freeframe); // Allocate a free frame

        if (allocStatus == BUFFEREXCEEDED) { // If the buffer is exceeded
            return BUFFEREXCEEDED;
        } else if (allocStatus == UNIXERR) { // If the status is not OK
            return UNIXERR;
        } else { // If the status is OK
            fileStatus = file->readPage(PageNo, &bufPool[freeframe]); // Read the page from the file

            if (fileStatus != OK) { // If the status is not OK
                return fileStatus;
            }

            bufStats.diskreads++; // Increment the disk reads
            bufTable[freeframe].Set(file, PageNo); // Set the buffer table
            insertStatus = hashTable->insert(file, PageNo, freeframe); // Insert the page into the hash table

            if (insertStatus == HASHTBLERROR) { // If the status is not OK
                return HASHTBLERROR;
            } else { // If the status is OK
                page = &bufPool[freeframe];
                bufTable[freeframe].refbit = true;
                bufStats.accesses++;
            }
        }
    } else if (hashStatus == OK) { // Case 2: If the page is in the buffer pool
        page = &bufPool[frameNo];
        bufTable[frameNo].refbit = true;
        bufTable[frameNo].pinCnt++;
        bufStats.accesses++;
    }

    return OK;
}

const Status BufMgr::unPinPage(File *file, const int PageNo, const bool dirty)
{
    // Variable initializations
    int frame;
    Status status;

    status = hashTable->lookup(file, PageNo, frame); // Check if the page is in the buffer pool

    if (status == HASHNOTFOUND) { // If the page is not in the buffer pool
        return status;
    } 
    if (bufTable[frame].pinCnt == 0) { // If the pin count is 0
        return PAGENOTPINNED;
    }
    
    bufTable[frame].pinCnt--; // Decrement the pin count

    if (dirty) { // If the page is dirty
        bufTable[frame].dirty = true;
    }

    return OK;
}

const Status BufMgr::allocPage(File *file, int &pageNo, Page *&page)
{
    // Variable initializations
    file->allocatePage(pageNo);
    int freeFrame = 0;
    Status allocStatus = OK;
    Status insertStatus = OK;
    allocStatus = allocBuf(freeFrame);

    if (allocStatus == BUFFEREXCEEDED) { // If the buffer is exceeded
        return BUFFEREXCEEDED;
    } else if (allocStatus == UNIXERR) { // If the status is not OK
        return UNIXERR;
    }

    page = &bufPool[freeFrame]; // Set the page to the free frame
    bufTable[freeFrame].Set(file, pageNo); // Set the buffer table
    insertStatus = hashTable->insert(file, pageNo, freeFrame); // Insert the page into the hash table

    if (insertStatus == HASHTBLERROR) { // If the status is not OK
        return HASHTBLERROR;
    }

    return OK;
}

const Status BufMgr::disposePage(File *file, const int pageNo)
{
    // see if it is in the buffer pool
    Status status = OK;
    int frameNo = 0;
    status = hashTable->lookup(file, pageNo, frameNo);
    if (status == OK)
    {
        // clear the page
        bufTable[frameNo].Clear();
    }
    status = hashTable->remove(file, pageNo);

    // deallocate it in the file
    return file->disposePage(pageNo);
}

const Status BufMgr::flushFile(const File *file)
{
    Status status;

    for (int i = 0; i < numBufs; i++)
    {
        BufDesc *tmpbuf = &(bufTable[i]);
        if (tmpbuf->valid == true && tmpbuf->file == file)
        {

            if (tmpbuf->pinCnt > 0)
                return PAGEPINNED;

            if (tmpbuf->dirty == true)
            {
#ifdef DEBUGBUF
                cout << "flushing page " << tmpbuf->pageNo
                        << " from frame " << i << endl;
#endif
                if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
                                                        &(bufPool[i]))) != OK)
                    return status;

                tmpbuf->dirty = false;
            }

            hashTable->remove(file, tmpbuf->pageNo);

            tmpbuf->file = NULL;
            tmpbuf->pageNo = -1;
            tmpbuf->valid = false;
        }

        else if (tmpbuf->valid == false && tmpbuf->file == file)
            return BADBUFFER;
    }

    return OK;
}

void BufMgr::printSelf(void)
{
    BufDesc *tmpbuf;

    cout << endl
            << "Print buffer...\n";
    for (int i = 0; i < numBufs; i++)
    {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char *)(&bufPool[i])
                << "\tpinCnt: " << tmpbuf->pinCnt;

        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}
