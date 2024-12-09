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
   // Qu_Select sets up things and then calls ScanSelect to do the actual work
    cout << "Doing QU_Select with " << attrValue << endl;
    AttrDesc *projAttrs = new AttrDesc[projCnt];

    for (int i = 0; i < projCnt; i++) {
        Status status = attrCat->getInfo(projNames[i].relName, projNames[i].attrName, projAttrs[i]);
        if (status != OK) {
            delete [] projAttrs;
            return status;
        }
    }

    AttrDesc *attrDesc = nullptr;
    if (attr) {
        attrDesc = new AttrDesc;
        Status status = attrCat->getInfo(attr->relName, attr->attrName, *attrDesc);
        if (status != OK) {
            delete[] projAttrs;
            delete attrDesc;
            return status;
        }
    }

    int reclen = 0;
    for (int i = 0; i < projCnt; i++) {
        reclen += projAttrs[i].attrLen;
    }


    Status status = ScanSelect(result, projCnt, projAttrs, attrDesc, op, attrValue, reclen);

    delete [] projAttrs;
    if (attrDesc) {
        delete attrDesc;
    }

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
    HeapFileScan sourceScan(projNames[0].relName, status);
    if (status != OK) return status;

    if (attrDesc) {
        status = sourceScan.startScan(attrDesc->attrOffset, attrDesc->attrLen,
                                      (Datatype)attrDesc->attrType, filter, op);
        if (status != OK) return status;
    }

    InsertFileScan resultInsert(result, status);
    if (status != OK) return status;

    Record rec;
    RID rid;
    while (sourceScan.scanNext(rid) == OK) {
        status = sourceScan.getRecord(rec);
        if (status != OK) return status;

        char *projTuple = new char[reclen];
        int offset = 0;

        for (int i = 0; i < projCnt; i++) {
            memcpy(projTuple + offset,
                   rec.data + projNames[i].attrOffset,
                   projNames[i].attrLen);
            offset += projNames[i].attrLen;
        }

        Record projectedRec;
        projectedRec.data = projTuple;
        projectedRec.length = reclen;
        status = resultInsert.insertRecord(projectedRec, rid);
        delete[] projTuple;

        if (status != OK) return status;
    }

    sourceScan.endScan();
    return OK;
}
