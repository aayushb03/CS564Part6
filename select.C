#include "catalog.h"
#include "query.h"


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
    cout << "Doing QU_Select with " << attrValue << endl;
    Status status;

    AttrDesc projDesc[projCnt];
    for (int i = 0; i < projCnt; ++i) {
        status = attrCat->getInfo(projNames[i].relName, projNames[i].attrName, projDesc[i]);
        if (status != OK) {
            return status;
        }
    }

    AttrDesc *attrDesc = nullptr;
    if (attr) {
        attrDesc = new AttrDesc;
        status = attrCat->getInfo(attr->relName, attr->attrName, *attrDesc);
        if (status != OK) {
            return status;
        }
    }

    int reclen = 0;
    for (int i = 0; i < projCnt; ++i) {
        reclen += projDesc[i].attrLen;
    }

    status = ScanSelect(result, projCnt, projDesc, attrDesc, op, attrValue, reclen);

    return status;
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
    Status status;

    HeapFileScan scan(projNames[0].relName, status);
    if (status != OK) {
        return status;
    }

    if (attrDesc) {
        Datatype type = static_cast<Datatype>(attrDesc->attrType);
        int offset = attrDesc->attrOffset;
        int length = attrDesc->attrLen;

        char *filterValue = nullptr;
        switch (type) {
            case INTEGER:
                filterValue = (char*) new int(atoi(filter));
                break;
            case FLOAT:
                filterValue = (char*) new float(atof(filter));
                break;
            case STRING:
                filterValue = (char *)filter;
                break;
            default:
                return ATTRTYPEMISMATCH;
        }

        status = scan.startScan(offset, length, type, filterValue, op);
        if (status != OK) {
            return status;
        }

    }

    InsertFileScan resultScan(result, status);
    if (status != OK) {
        return status;
    }

    RID rid;
    Record record;
    char *outputData = new char[reclen];
    Record outputRec = {outputData, reclen};

    while (scan.scanNext(rid) == OK) {
        scan.getRecord(record);

        int offset = 0;
        for (int i = 0; i < projCnt; ++i) {
            memcpy(outputData + offset,
                   (char *)record.data + projNames[i].attrOffset,
                   projNames[i].attrLen);
            offset += projNames[i].attrLen;
        }

        RID outRID;
        status = resultScan.insertRecord(outputRec, outRID);
        if (status != OK) {
            return status;
        }
    }

    return OK;
}
