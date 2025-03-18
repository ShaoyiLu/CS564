#include "heapfile.h"
#include "error.h"

// routine to create a heapfile
const Status createHeapFile(const string fileName)
{
    File* 		file;
    Status 		status;
    FileHdrPage*	hdrPage;
    int			hdrPageNo;
    int			newPageNo;
    Page*		newPage;

    // try to open the file. This should return an error
    status = db.openFile(fileName, file);
    if (status != OK)
    {
		// file doesn't exist. First create it and allocate
		// an empty header page and data page.
        status = db.createFile(fileName); // Create the file

        if (status == OK) { // If the file was created successfully
            status = db.openFile(fileName, file);

            if (status == OK) { // If the file was opened successfully
                status = bufMgr->allocPage(file, hdrPageNo, newPage);

                if (status == OK) { // If the page was allocated successfully
                    hdrPage = (FileHdrPage *)newPage; // Set the header page

                    strncpy(hdrPage->fileName, fileName.c_str(), MAXNAMESIZE); // Copy the file name to the header page

                    status = bufMgr->allocPage(file, newPageNo, newPage); // Allocate a new page

                    if (status == OK) { // If the page was allocated successfully
                        newPage->init(newPageNo); // Initialize the new page

                        // Initialize the header page
                        hdrPage->firstPage = newPageNo;
                        hdrPage->lastPage = newPageNo;
                        hdrPage->pageCnt = 1;
                        hdrPage->recCnt = 0;

                        // Unpin the pages
                        status = bufMgr->unPinPage(file, hdrPageNo, true);
                        status = bufMgr->unPinPage(file, newPageNo, true);
                    }
                }
            }
        }

        // Close the file
        status = bufMgr->flushFile(file);
        status = db.closeFile(file);
        
        return status;
    }

    return (FILEEXISTS);
}

// routine to destroy a heapfile
const Status destroyHeapFile(const string fileName)
{
	return (db.destroyFile (fileName));
}


// constructor opens the underlying file
HeapFile::HeapFile(const string & fileName, Status& returnStatus)
{
    Status 	status;
    Page*	pagePtr;

    cout << "opening file " << fileName << endl;

    // open the file and read in the header page and the first data page
    if ((status = db.openFile(fileName, filePtr)) == OK)
    {
        status = filePtr->getFirstPage(headerPageNo); // Get the first page

        if (status != OK) { // If the status is not OK
            returnStatus = status;
        }

        status = bufMgr->readPage(filePtr, headerPageNo, pagePtr); // Read the page

        if (status != OK) { // If the status is not OK
            returnStatus = status;
        }

        // Set the header page
        headerPage = (FileHdrPage *)pagePtr;
        hdrDirtyFlag = false;

        // Set the current page
        curPageNo = headerPage->firstPage;
        status = bufMgr->readPage(filePtr, curPageNo, curPage);

        if (status != OK) { // If the status is not OK
            returnStatus = status;
        }

        // Set the current record
        curDirtyFlag = false;
        curRec = NULLRID;
        returnStatus = status;
    }
    else
    {
        cerr << "open of heap file failed\n";
		returnStatus = status;
		return;
    }
}

// the destructor closes the file
HeapFile::~HeapFile()
{
    Status status;
    cout << "invoking heapfile destructor on file " << headerPage->fileName << endl;

    // see if there is a pinned data page. If so, unpin it 
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
		curPage = NULL;
		curPageNo = 0;
		curDirtyFlag = false;
		if (status != OK) cerr << "error in unpin of date page\n";
    }
	
	 // unpin the header page
    status = bufMgr->unPinPage(filePtr, headerPageNo, hdrDirtyFlag);
    if (status != OK) cerr << "error in unpin of header page\n";
	
	// status = bufMgr->flushFile(filePtr);  // make sure all pages of the file are flushed to disk
	// if (status != OK) cerr << "error in flushFile call\n";
	// before close the file
	status = db.closeFile(filePtr);
    if (status != OK)
    {
		cerr << "error in closefile call\n";
		Error e;
		e.print (status);
    }
}

// Return number of records in heap file

const int HeapFile::getRecCnt() const
{
    return headerPage->recCnt;
}

// retrieve an arbitrary record from a file.
// if record is not on the currently pinned page, the current page
// is unpinned and the required page is read into the buffer pool
// and pinned.  returns a pointer to the record via the rec parameter

const Status HeapFile::getRecord(const RID & rid, Record & rec)
{
    Status status;

    // cout<< "getRecord. record (" << rid.pageNo << "." << rid.slotNo << ")" << endl;

    if (curPage == NULL) { // If the current page is null
        status = bufMgr->readPage(filePtr, rid.pageNo, curPage); // Read the page

        if (status != OK) { // If the status is not OK
            return status;
        }

        // Set the current page
        curPageNo = rid.pageNo;
        curDirtyFlag = false;
        curRec = rid;
        status = curPage->getRecord(rid, rec);

        return status;
    }

    if (rid.pageNo == curPageNo) { // If the rid page number is the same as the current page number
        // Set the current record
        status = curPage->getRecord(rid, rec);
        curRec = rid;

        return OK;
    }

    status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag); // Unpin the current page

    if (status != OK) { // If the status is not OK
        // Initialize the current page
        curPage = NULL;
        curPageNo = -1;
        curDirtyFlag = false;

        return status;
    }

    status = bufMgr->readPage(filePtr, rid.pageNo, curPage); // Read the page

    if (status != OK) { // If the status is not OK
        return status;
    }

    // Set the current page
    curPageNo = rid.pageNo;
    curDirtyFlag = false;
    curRec = rid;
    status = curPage->getRecord(rid, rec);

    return status;
}

HeapFileScan::HeapFileScan(const string & name, Status & status) : HeapFile(name, status)
{
    filter = NULL;
}

const Status HeapFileScan::startScan(const int offset_,
                    const int length_,
                    const Datatype type_, 
                    const char* filter_,
                    const Operator op_)
{
    if (!filter_) {                        // no filtering requested
        filter = NULL;
        return OK;
    }
    
    if ((offset_ < 0 || length_ < 1) ||
        (type_ != STRING && type_ != INTEGER && type_ != FLOAT) ||
        ((type_ == INTEGER && length_ != sizeof(int))
        || (type_ == FLOAT && length_ != sizeof(float))) ||
        (op_ != LT && op_ != LTE && op_ != EQ && op_ != GTE && op_ != GT && op_ != NE))
    {
        return BADSCANPARM;
    }

    offset = offset_;
    length = length_;
    type = type_;
    filter = filter_;
    op = op_;

    return OK;
}


const Status HeapFileScan::endScan()
{
    Status status;
    // generally must unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        curPage = NULL;
        curPageNo = 0;
		curDirtyFlag = false;
        return status;
    }
    return OK;
}

HeapFileScan::~HeapFileScan()
{
    endScan();
}

const Status HeapFileScan::markScan()
{
    // make a snapshot of the state of the scan
    markedPageNo = curPageNo;
    markedRec = curRec;
    return OK;
}

const Status HeapFileScan::resetScan()
{
    Status status;
    if (markedPageNo != curPageNo) 
    {
		if (curPage != NULL)
		{
			status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
			if (status != OK) return status;
		}
		// restore curPageNo and curRec values
		curPageNo = markedPageNo;
		curRec = markedRec;
		// then read the page
		status = bufMgr->readPage(filePtr, curPageNo, curPage);
		if (status != OK) return status;
		curDirtyFlag = false; // it will be clean
    }
    else curRec = markedRec;
    return OK;
}


const Status HeapFileScan::scanNext(RID& outRid)
{
    Status 	status = OK;
    RID		nextRid;
    RID		tmpRid;
    int 	nextPageNo;
    Record      rec;

    bool matchFound = false; // Flag to indicate if a matching record is found

    if (curPageNo == -1) { // If the current page number is -1
        return FILEEOF;
    }

    if (curPage == NULL) { // If the current page is null
        curPageNo = headerPage->firstPage; // Set the current page number to the first page

        if (curPageNo == -1) { // If the current page number is -1
            return FILEEOF;
        }

        status = bufMgr->readPage(filePtr, curPageNo, curPage); // Read the page

        if (status != OK) { // If the status is not OK
            return status;
        } else { // If the status is OK
            // Set the current page
            curDirtyFlag = false;
            curRec = NULLRID;

            // Get the first record
            status = curPage->firstRecord(tmpRid);
            curRec = tmpRid;

            if (status == NORECORDS) { // If there are no records
                status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag); // Unpin the page

                if (status != OK) { // If the status is not OK
                    return status;
                }

                // Initialize the current page
                curPage = NULL;
                curPageNo = -1;
                curDirtyFlag = false;

                return FILEEOF;
            }

            status = curPage->getRecord(tmpRid, rec); // Get the record

            if (status != OK) { // If the status is not OK
                return status;
            }

            if (matchRec(rec)) { // If the record matches
                outRid = tmpRid;
                matchFound = true;
            }
        }
    }

    while (!matchFound) { // While a matching record is not found
        status = curPage->nextRecord(curRec, nextRid); // Get the next record

        if (status == OK) { // If the status is OK
            curRec = nextRid;
        }

        while (status != OK) { // While the status is not OK
            curPage->getNextPage(nextPageNo); // Get the next page number

            if (nextPageNo == -1) { // If the next page number is -1
                return FILEEOF;
            }

            // Unpin the current page
            status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
            curPage = NULL;
            curPageNo = -1;
            curDirtyFlag = false;

            if (status != OK) { // If the status is not OK
                return status;
            }

            // Read the next page
            curPageNo = nextPageNo;
            curDirtyFlag = false;
            status = bufMgr->readPage(filePtr, curPageNo, curPage);

            if (status != OK) { // If the status is not OK
                return status;
            }

            status = curPage->firstRecord(curRec); // Get the first record
        }

        status = curPage->getRecord(curRec, rec); // Get the record

        if (status != OK) { // If the status is not OK
            return status;
        }

        if (matchRec(rec)) { // If the record matches
            outRid = curRec;
            matchFound = true;
        }
    }

    if (matchFound) { // If a matching record is found
        return OK;
    } else { // If a matching record is not found
        return status;
    }
}


// returns pointer to the current record.  page is left pinned
// and the scan logic is required to unpin the page 

const Status HeapFileScan::getRecord(Record & rec)
{
    return curPage->getRecord(curRec, rec);
}

// delete record from file. 
const Status HeapFileScan::deleteRecord()
{
    Status status;

    // delete the "current" record from the page
    status = curPage->deleteRecord(curRec);
    curDirtyFlag = true;

    // reduce count of number of records in the file
    headerPage->recCnt--;
    hdrDirtyFlag = true; 
    return status;
}


// mark current page of scan dirty
const Status HeapFileScan::markDirty()
{
    curDirtyFlag = true;
    return OK;
}

const bool HeapFileScan::matchRec(const Record & rec) const
{
    // no filtering requested
    if (!filter) return true;

    // see if offset + length is beyond end of record
    // maybe this should be an error???
    if ((offset + length -1 ) >= rec.length)
	return false;

    float diff = 0;                       // < 0 if attr < fltr
    switch(type) {

    case INTEGER:
        int iattr, ifltr;                 // word-alignment problem possible
        memcpy(&iattr,
               (char *)rec.data + offset,
                length);
        memcpy(&ifltr,
                filter,
                length);
        diff = iattr - ifltr;
        break;

    case FLOAT:
        float fattr, ffltr;               // word-alignment problem possible
        memcpy(&fattr,
               (char *)rec.data + offset,
                length);
        memcpy(&ffltr,
                filter,
                length);
        diff = fattr - ffltr;
        break;

    case STRING:
        diff = strncmp((char *)rec.data + offset,
                        filter,
                        length);
        break;
    }

    switch(op) {
    case LT:  if (diff < 0.0) return true; break;
    case LTE: if (diff <= 0.0) return true; break;
    case EQ:  if (diff == 0.0) return true; break;
    case GTE: if (diff >= 0.0) return true; break;
    case GT:  if (diff > 0.0) return true; break;
    case NE:  if (diff != 0.0) return true; break;
    }

    return false;
}

InsertFileScan::InsertFileScan(const string & name,
                                Status & status) : HeapFile(name, status)
{
  //Do nothing. Heapfile constructor will bread the header page and the first
  // data page of the file into the buffer pool
}

InsertFileScan::~InsertFileScan()
{
    Status status;
    // unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, true);
        curPage = NULL;
        curPageNo = 0;
        if (status != OK) cerr << "error in unpin of data page\n";
    }
}

// Insert a record into the file
const Status InsertFileScan::insertRecord(const Record & rec, RID& outRid)
{
    Page*	newPage;
    int		newPageNo;
    Status	status, unpinstatus;
    RID		rid;

    // check for very large records
    if ((unsigned int) rec.length > PAGESIZE-DPFIXED)
    {
        // will never fit on a page, so don't even bother looking
        return INVALIDRECLEN;
    }

    if (curPage == NULL) { // If the current page is NULL
        status = bufMgr->readPage(filePtr, headerPage->lastPage, curPage); // Read the last page
        
        if (status != OK) { // If the status is not OK
            return status;
        }

        curPageNo = headerPage->lastPage; // Set the current page number
    }

    status = curPage->insertRecord(rec, rid); // Insert the record into the current page

    if (status == NOSPACE) { // If the status is NOSPACE
        status = bufMgr->allocPage(filePtr, newPageNo, newPage); // Allocate a new page
        
        if (status != OK) { // If the status is not OK
            return status;
        }

        // Initialize the new page
        newPage->init(newPageNo);
        newPage->setNextPage(-1);

        // Set the next page of the current page to the new page
        int tmpRid;

        // Get the next page of the current page
        curPage->getNextPage(tmpRid);
        curPage->setNextPage(newPageNo);
        newPage->setNextPage(tmpRid);

        if (tmpRid == -1) { // If the next page of the current page is -1
            headerPage->lastPage = newPageNo;
        }

        // Insert the record into the new page
        headerPage->pageCnt++;
        unpinstatus = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        
        if (unpinstatus != OK) { // If the unpin status is not OK
            return status;
        }

        // Set the current page to the new page
        curPage = newPage;
        curPageNo = newPageNo;
        status = curPage->insertRecord(rec, rid);
        
        if (status != OK) { // If the status is not OK
            return status;
        }
    }

    // Update the header page
    headerPage->recCnt++;
    hdrDirtyFlag = true;
    curDirtyFlag = true;
    outRid = rid;

    return status;
}