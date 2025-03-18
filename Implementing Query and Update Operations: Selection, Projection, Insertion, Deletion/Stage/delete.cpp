#include "catalog.h"
#include "query.h"


/*
 * Deletes records from a specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Delete(const string & relation, 
						const string & attrName, 
						const Operator op,
						const Datatype type, 
						const char *attrValue)
{
	// part 6
	if (relation.empty()) {
		return BADCATPARM;
	}

	Status status;
	RID rid;
	AttrDesc attrD;
	HeapFileScan *scanner;
	int intVal;
	float floatVal;

	if (attrName.empty()) {
		scanner = new HeapFileScan(relation, status);

		if (status != OK) {
			return status;
		}

		scanner->startScan(0, 0, STRING, NULL, EQ);

		while ((status = scanner->scanNext(rid)) != FILEEOF) {
			status = scanner->deleteRecord();

			if (status != OK) {
				return status;
			}
		}
	} else {
		scanner = new HeapFileScan(relation, status);

		if (status != OK) {
			return status;
		}

		status = attrCat->getInfo(relation, attrName, attrD);

		if (status != OK) {
			return status;
		}

		switch (type) {
			case INTEGER:
				intVal = atoi(attrValue);
				status = scanner->startScan(attrD.attrOffset, attrD.attrLen, type, (char *)&intVal, op);
				
				break;

			case FLOAT:
				floatVal = atof(attrValue);
				status = scanner->startScan(attrD.attrOffset, attrD.attrLen, type, (char *)&floatVal, op);
				
				break;

			default:
				status = scanner->startScan(attrD.attrOffset, attrD.attrLen, type, attrValue, op);
				
				break;
		}

		if (status != OK) {
			return status;
		}

		while ((status = scanner->scanNext(rid)) == OK) {
			status = scanner->deleteRecord();

			if (status != OK) {
				return status;
			}
		}
	}

	if (status != FILEEOF) {
		return status;
	}

	scanner->endScan();
	delete scanner;

	return OK;
}