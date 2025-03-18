#include "catalog.h"
#include "query.h"
#include "stdio.h"
#include "stdlib.h"

// forward declaration
const Status ScanSelect(const string & result, 
							const int projCnt, 
							const AttrDesc projNames[],
							const AttrDesc *attrDesc, 
							const Operator op, 
							const char *filter,
							const int reclen);

/*
 * Selects records from the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Select(const string & result, 
						const int projCnt, 
						const attrInfo projNames[],
						const attrInfo *attr, 
						const Operator op, 
						const char *attrValue)
{
   	// Qu_Select sets up things and then calls ScanSelect to do the actual work
    cout << "Doing QU_Select " << endl;
	
	Status status;
    AttrDesc attrDesc;
    int reclen = 0;
    AttrDesc projNamesDesc[projCnt];
	const char* filter;
	int intVal;
    float floatVal;

    for (int i = 0; i < projCnt; i++) {
    	status = attrCat->getInfo(string(projNames[i].relName),string(projNames[i].attrName), projNamesDesc[i]);
    	
		if (status != OK) {  
			return status; 
		}

    	reclen += projNamesDesc[i].attrLen;
    }

    if (attr != NULL) {
    	status = attrCat->getInfo(string(attr->relName), string(attr->attrName), attrDesc);

    	if (status != OK) {
			return status; 
		}

    	switch (attr->attrType) {
    		case STRING:
    			filter = attrValue;

    			break;
            case INTEGER:
            	intVal = atoi(attrValue);
                filter = (char*)&intVal;

                break;
            case FLOAT:
            	floatVal = atof(attrValue);
                filter = (char*)&floatVal;

                break;           
        }	
    } else {
    	attrDesc.attrOffset = 0;
        attrDesc.attrLen = 0;
    	attrDesc.attrType = STRING;
    	filter = NULL;
    }
	
    return ScanSelect(result, projCnt, projNamesDesc, &attrDesc, op, filter, reclen);
}


const Status ScanSelect(const string & result, 
#include "stdio.h"
#include "stdlib.h"
							const int projCnt, 
							const AttrDesc projNames[],
							const AttrDesc *attrDesc, 
							const Operator op, 
							const char *filter,
							const int reclen)
{
    cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;

	char recordData[reclen];
	Record record;
	record.data = (void *) recordData;
	record.length = reclen;
	Status status;
	RID currRID;
	Record currRec;
	int tupleCount = 0;
	RID newRID;

	InsertFileScan resultRel(result,status);

	if (status != OK){
		return status;
	}
	
	HeapFileScan heapfileobj(string(projNames[0].relName), status);

	if (status != OK) {
		return status;
	}

	status = heapfileobj.startScan(attrDesc->attrOffset, attrDesc->attrLen, (Datatype)attrDesc->attrType, filter, op);

	if (status != OK) { 
		return status;
	}

	while (heapfileobj.scanNext(currRID) == OK) {
		status = heapfileobj.getRecord(currRec);

		if (status == OK) {
			int recordOffset = 0;
			
			for (int i = 0; i < projCnt; i++) {
				memcpy(recordData + recordOffset,
				(char *)currRec.data + projNames[i].attrOffset,
				projNames[i].attrLen);

				recordOffset += projNames[i].attrLen;
			}
		} else {
			return status;
		}

		status = resultRel.insertRecord(record, newRID);
		ASSERT(status == OK);
		tupleCount++;
	}

	return OK;
}