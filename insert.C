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
    cout << "Inserting record into relation " << relation << endl;
    // Step 1: Validate input
    if (relation.empty() || attrCnt <= 0 || attrList == nullptr) {
        return BADCATPARM; // Return error code for bad input
    }

    // Step 2: Fetch the relation metadata from the catalog
    RelDesc relDesc;
    Status status = relCat->getInfo(relation, relDesc);
    if (status != OK) {
        return status; // Return the error if relation doesn't exist
    }

    // Step 3: Fetch the attributes for the relation
    AttrDesc *attrs;
    int attrCount;
    status = attrCat->getRelInfo(relation, attrCount, attrs);
    if (status != OK) {
        return status; // Return the error if attributes can't be fetched
    }

    // Step 4: Validate the number of attributes
    if (attrCnt != attrCount) {
        delete[] attrs; // Clean up allocated memory
        return ATTRTYPEMISMATCH; // Mismatch in attribute count
    }

    // Step 5: Create the record
    int recordSize = 0; // Calculate the record size dynamically
    for (int i = 0; i < attrCnt; i++) {
        recordSize += attrs[i].attrLen;
    }
    char *recordData = new char[recordSize]; // Allocate memory for the record
    memset(recordData, 0, recordSize); // Initialize memory

    for (int i = 0; i < attrCnt; i++) {
        // Locate the corresponding attribute in the catalog
        int j;
        for (j = 0; j < attrCount; j++) {
            cout << "Comparing " << attrs[j].attrName << " with " << attrList[i].attrName << endl;
            if (strcmp(attrs[j].attrName, attrList[i].attrName) == 0) {
                break;
            }
        }
        if (j == attrCount) { // Attribute not found
            delete[] recordData; // Free allocated memory
            delete[] attrs; // Clean up
            return ATTRTYPEMISMATCH;
        }

        // Validate and copy the value into the record
        if (attrs[j].attrType != attrList[i].attrType) {
            delete[] recordData;
            delete[] attrs;
            return ATTRTYPEMISMATCH;
        }

        switch (attrs[j].attrType) {
            case INTEGER:
                memcpy(recordData + attrs[j].attrOffset, &attrList[i].attrValue, sizeof(int));
                break;
            case FLOAT:
                memcpy(recordData + attrs[j].attrOffset, &attrList[i].attrValue, sizeof(float));
                break;
            case STRING:
                memcpy(recordData + attrs[j].attrOffset, attrList[i].attrValue, attrs[j].attrLen);
                break;
        }
    }

    // Wrap the record data in a Record object
    Record rec;
    rec.data = recordData;
    rec.length = recordSize;

    // Step 6: Insert the record into the heap file
    InsertFileScan heapFile(relation, status);
    if (status != OK) {
        delete[] recordData; // Free allocated memory
        delete[] attrs;
        return status;
    }

    RID outRid;
    status = heapFile.insertRecord(rec, outRid);

    // Step 7: Clean up
    delete[] recordData;
    delete[] attrs;

    return status;
}

