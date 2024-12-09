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
    if (relation.empty() || attrCnt <= 0 || attrList == nullptr) {
        return BADCATPARM;
    }

    RelDesc relDesc;
    Status status = relCat->getInfo(relation, relDesc);
    if (status != OK) {
        return status;
    }

    AttrDesc *attrs;
    int attrCount;
    status = attrCat->getRelInfo(relation, attrCount, attrs);
    if (status != OK) {
        return status;
    }

    if (attrCnt != attrCount) {
        delete[] attrs;
        return ATTRTYPEMISMATCH;
    }

    int recordSize = 0;
    for (int i = 0; i < attrCnt; i++) {
        recordSize += attrs[i].attrLen;
    }
    char *recordData = new char[recordSize];
    memset(recordData, 0, recordSize);

    for (int i = 0; i < attrCnt; i++) {
        int j;
        for (j = 0; j < attrCount; j++) {
            if (strcmp(attrs[j].attrName, attrList[i].attrName) == 0) {
                break;
            }
        }
        if (j == attrCount) {
            delete[] recordData;
            delete[] attrs;
            return ATTRTYPEMISMATCH;
        }

        if (attrs[j].attrType != attrList[i].attrType) {
            delete[] recordData;
            delete[] attrs;
            return ATTRTYPEMISMATCH;
        }

        int intValue = 0;
        float floatValue = 0.0f;
        switch (attrs[j].attrType) {
            case INTEGER:
                intValue = atoi((const char*)attrList[i].attrValue);
                memcpy(recordData + attrs[j].attrOffset, &intValue, sizeof(int));
                break;
            case FLOAT:
                floatValue = atof((const char*)attrList[i].attrValue);
                memcpy(recordData + attrs[j].attrOffset, &floatValue, sizeof(float));
                break;
            case STRING:
                memcpy(recordData + attrs[j].attrOffset, attrList[i].attrValue, attrs[j].attrLen);
                break;
        }
    }

    Record rec;
    rec.data = recordData;
    rec.length = recordSize;

    InsertFileScan heapFile(relation, status);
    if (status != OK) {
        delete[] recordData;
        delete[] attrs;
        return status;
    }

    RID outRid;
    status = heapFile.insertRecord(rec, outRid);

    delete[] recordData;
    delete[] attrs;

    return status;
}

