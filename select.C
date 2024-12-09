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
    Status status;

    // Convert attrInfo to AttrDesc for projection
    AttrDesc projDesc[projCnt];
    for (int i = 0; i < projCnt; ++i) {
        status = attrCat->getInfo(projNames[i].relName, projNames[i].attrName, projDesc[i]);
        if (status != OK) {
            return status;
        }
    }

    // Convert attrInfo to AttrDesc for the selection attribute, if specified
    AttrDesc *attrDesc = nullptr;
    if (attr) {
        attrDesc = new AttrDesc;
        status = attrCat->getInfo(attr->relName, attr->attrName, *attrDesc);
        if (status != OK) {
            return status;
        }
    }

    // Calculate the length of the resulting records
    int reclen = 0;
    for (int i = 0; i < projCnt; ++i) {
        reclen += projDesc[i].attrLen;
    }

    // Call ScanSelect to perform the actual selection
    status = ScanSelect(result, projCnt, projDesc, attrDesc, op, attrValue, reclen);

    // Clean up
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

    // Set up the HeapFileScan for the input relation
    HeapFileScan scan(projNames[0].relName, status);
    if (status != OK) {
        return status;
    }

    // If attrDesc is not null, set up the filter
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
                filterValue = (char *)filter; // Assume filter is a null-terminated string
                break;
            default:
                return ATTRTYPEMISMATCH;
        }

        status = scan.startScan(offset, length, type, filterValue, op);
        if (status != OK) {
            return status;
        }

    }

    // Set up the output relation
    InsertFileScan resultScan(result, status);
    if (status != OK) {
        return status;
    }

    // Perform the scan and projection
    RID rid;
    Record record;
    char *outputData = new char[reclen];
    Record outputRec = {outputData, reclen};

    while (scan.scanNext(rid) == OK) {
        scan.getRecord(record);

        // Project the fields into the output record
        int offset = 0;
        for (int i = 0; i < projCnt; ++i) {
            memcpy(outputData + offset,
                   (char *)record.data + projNames[i].attrOffset,
                   projNames[i].attrLen);
            offset += projNames[i].attrLen;
        }

        // Insert the projected record into the result relation
        RID outRID;
        status = resultScan.insertRecord(outputRec, outRID);
        if (status != OK) {
            return status;
        }
    }

    return OK;
}
