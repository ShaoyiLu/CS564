#include "catalog.h"
#include "query.h"


/*
 * Inserts a record into the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Insert(const string & relation, 
						const int attrCnt, 
						const attrInfo attrList[])
{
	// part 6
	Status status;
    Record rec;
    RID rid;
    int relAttrCnt;
    AttrDesc *attrDesc;
    int recLen;
    int intVal;
    int floatVal;

	status = attrCat->getRelInfo(relation, relAttrCnt, attrDesc);

    if (status != OK) {
        return status;
    }

    if (relAttrCnt != attrCnt) {
        return OK;
    }

    for (int i = 0; i < attrCnt; i++) {
        recLen += attrDesc[i].attrLen;
    }

    InsertFileScan resultRel(relation, status);

    if (status != OK) {
        return status;
    }

    rec.length = recLen;
    rec.data = (char *)malloc(recLen);

    for (int i = 0; i < relAttrCnt; i++) { 
        for (int j = 0; j < attrCnt; j++) {
            if (strcmp(attrList[j].attrName, attrDesc[i].attrName) == 0) {
                switch (attrList[j].attrType) {
					case INTEGER:
						intVal = atoi((char *)attrList[j].attrValue);
						
						memcpy((char *)rec.data + attrDesc[i].attrOffset, (char *)&intVal, attrDesc[i].attrLen);
						
						break;

					case FLOAT:
						floatVal = atof((char *)attrList[j].attrValue);
						memcpy((char *)rec.data + attrDesc[i].attrOffset, (char *)&floatVal, attrDesc[i].attrLen);
						
						break;

					default:
						memcpy((char *)rec.data + attrDesc[i].attrOffset, attrList[j].attrValue, attrDesc[i].attrLen);
						
						break;
					}

                break;
            }
        }
    }

    resultRel.insertRecord(rec, rid);

	return OK;

}