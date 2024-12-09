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
    HeapFile heapFile(relation, status); // Open the heap file
    if (status != OK) return status; // Return if heap file can't be opened

    HeapFileScan scan(relation, status); // Initialize a scan for the heap file
    if (status != OK) return status; // Return if scan initialization fails

    // Set the scan filter with the given attribute name, operator, and value
    status = scan.startScan(0, 0, type, attrValue, op);
    if (status != OK) return status;

    RID rid; // Record identifier
    Record rec; // Record to hold fetched record

    // Loop through all records in the heap file
    while (true)
    {
        // Get the next record that matches the scan's filter
        status = scan.scanNext(rid);
        if (status == FILEEOF) break; // Exit if we've reached the end of the file
        if (status != OK) return status; // Return if there's an error during scan

        // Get the matching record
        status = scan.getRecord(rec);
        if (status != OK) return status;

        // Delete the matching record
        status = scan.deleteRecord();
        if (status != OK) return status;
    }

    // End the scan after finishing the deletion process
    status = scan.endScan();
    if (status != OK) return status;

    // Return the success status after deletion
    return OK;
}


