#include "catalog.h"
#include "query.h"
#include "heapfile.h"


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
    RelDesc relDesc;
    Status status = relCat->getInfo(relation, relDesc);
    if (status != OK) {
        return status; // Relation not found
    }

    // Retrieve attributes of the relation
    AttrDesc *attrDesc;
    int relAttrCnt;
    status = attrCat->getRelInfo(relation, relAttrCnt, attrDesc);
    if (status != OK) {
        return status; // Error retrieving attributes
    }

    // Validate attribute count
    if (attrCnt != relAttrCnt) {
        return ATTRNOTFOUND;
    }

    // Allocate memory for the record
    int recordLength = 0;
    for (int i = 0; i < relAttrCnt; ++i) {
        recordLength += attrDesc[i].attrLen;
    }
    char *recordData = new char[recordLength];
    memset(recordData, 0, recordLength); // Initialize memory

    // Copy attribute values into the record
    for (int i = 0; i < attrCnt; ++i) {
        const attrInfo &attr = attrList[i];
        bool found = false;

        // Find the attribute in the relation schema
        for (int j = 0; j < relAttrCnt; ++j) {
            if (attrDesc[j].attrName == attr.attrName) {
                // Validate attribute type
                if (attrDesc[j].attrType != attr.attrType) {
                    delete[] recordData;
                    return ATTRTYPEMISMATCH;
                }

                // Copy the attribute value into the record
                memcpy(recordData + attrDesc[j].attrOffset, attr.attrValue, attrDesc[j].attrLen);
                found = true;
                break;
            }
        }

        if (!found) {
            delete[] recordData;
            return ATTRNOTFOUND; // Attribute not in schema
        }
    }

    // Open heap file for the relation
    Status heapStatus;
    InsertFileScan heapFile(relation, heapStatus);
    if (heapStatus != OK) {
        delete[] recordData;
        return heapStatus; // Error opening heap file
    }

    // Insert the record into the heap file
    RID rid;
    Record newRecord = {recordData, recordLength};
    heapStatus = heapFile.insertRecord(newRecord, rid);
    delete[] recordData; // Free allocated memory

    if (heapStatus != OK) {
        return heapStatus; // Error inserting record
    }

    return OK;
}

