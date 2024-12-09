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
    Status status;
    HeapFileScan fileScan(relation, status);
    if (status != OK) {
        return status;
    }

    AttrDesc attrDesc;
    status = attrCat->getInfo(relation, attrName, attrDesc);
    if (status != OK) {
        return status;
    }

    char *filterValue = nullptr;
    switch (type) {
        case INTEGER:
            filterValue = (char*) new int(atoi(attrValue));
            break;
        case FLOAT:
            filterValue = (char*) new float(atof(attrValue));
            break;
        case STRING:
            filterValue = (char *)attrValue; // Assume filter is a null-terminated string
            break;
        default:
            return ATTRTYPEMISMATCH;
    }

    status = fileScan.startScan(attrDesc.attrOffset,
                                attrDesc.attrLen,
                                type,
                                nullptr,
                                op);
    if (status != OK) {
        return status;
    }

    RID rid;
    Record rec;
    while (fileScan.scanNext(rid) == OK) {
        status = fileScan.getRecord(rec);
        if (status != OK) {
            return status;
        }

        status = fileScan.deleteRecord();
        if (status != OK) {
            return status;
        }
    }

    fileScan.endScan();

    return OK;
}


