#include <stdio.h>
#include <stdlib.h>
#include <string.h>      // for memcpy, memset, etc.
#include <stdbool.h>
#include "record_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "dberror.h"
#include "expr.h"
#include "tables.h"

/*
 * Data structures used internally
 * ---------------------------------------------------------------
 * - RM_TableMgmtData: stored the buffer pool and table-level metadata.
 * - RM_ScanMgmtData : stored scan-related information (current page, slot, etc.).
 */

/* This structure stored the essential table metadata. */
typedef struct RM_TableMgmtData {
    BM_BufferPool bufferPool; // This had been the buffer pool used by the table
    int numTuples;            // This had been the total number of tuples present in the table
    int nextFreePage;         // This had been the first data page that might have free slots (-1 if none)
    int recordSize;           // This had been the size, in bytes, of each record
} RM_TableMgmtData;

/* This structure stored the state for a table scan in progress. */
typedef struct RM_ScanMgmtData {
    int currentPage;    // Which page was being scanned
    int currentSlot;    // Which slot within that page
    Expr *cond;         // The scan condition (NULL if no filtering)
} RM_ScanMgmtData;

/* --------------------------------------------------------------------------
   Helpers
   -------------------------------------------------------------------------- */

/* 
 * computeRecordSize
 * -----------------
 * Determined how many bytes a record required, based on its schema.
 * Summed up the byte sizes for each attribute's data type.
 */
static int
computeRecordSize(Schema *schema)
{
    int size = 0;
    for (int i = 0; i < schema->numAttr; i++)
    {
        switch (schema->dataTypes[i])
        {
            case DT_INT:    
                size += sizeof(int);    
                break;
            case DT_FLOAT:  
                size += sizeof(float);  
                break;
            case DT_BOOL:   
                size += sizeof(bool);   
                break;
            case DT_STRING: 
                size += schema->typeLength[i];  
                break;
        }
    }
    return size;
}

/* 
 * writeTableInfo
 * --------------
 * Wrote table metadata (numTuples, nextFreePage, schema info) into page 0.
 * Used the buffer manager to pin page 0, cleared it, and wrote lines describing
 * attribute data types, lengths, etc.
 */
static RC
writeTableInfo(RM_TableData *rel)
{
    RM_TableMgmtData *tblData = (RM_TableMgmtData *) rel->mgmtData;
    BM_PageHandle page;
    RC rc = pinPage(&tblData->bufferPool, &page, 0);
    if (rc != RC_OK) return rc;

    // Cleared out page 0
    memset(page.data, 0, PAGE_SIZE);

    // First line: numTuples, nextFreePage
    char buffer[512];
    sprintf(buffer, "%d %d\n", tblData->numTuples, tblData->nextFreePage);
    strcpy(page.data, buffer);
    int offset = (int) strlen(buffer);

    // Next line: number of attributes in the schema
    Schema *sc = rel->schema;
    sprintf(buffer, "%d\n", sc->numAttr);
    strcpy(page.data + offset, buffer);
    offset += (int) strlen(buffer);

    // For each attribute: wrote dataType, typeLength, and attrName
    for (int i = 0; i < sc->numAttr; i++)
    {
        sprintf(buffer, "%d %d %s\n",
                (int) sc->dataTypes[i],
                sc->typeLength[i],
                sc->attrNames[i]);
        strcpy(page.data + offset, buffer);
        offset += (int) strlen(buffer);
    }

    // Marked page as dirty, unpinned, and forced to disk
    markDirty(&tblData->bufferPool, &page);
    unpinPage(&tblData->bufferPool, &page);
    forcePage(&tblData->bufferPool, &page);

    return RC_OK;
}

/* 
 * readTableInfo
 * -------------
 * Read table metadata from page 0. This pinned page 0, parsed lines for
 * numTuples, nextFreePage, number of attributes, each attribute's data type,
 * name, etc.
 */
static RC
readTableInfo(RM_TableData *rel)
{
    RM_TableMgmtData *tblData = (RM_TableMgmtData *) rel->mgmtData;
    BM_PageHandle page;
    RC rc = pinPage(&tblData->bufferPool, &page, 0);
    if (rc != RC_OK) return rc;

    char *data = page.data;
    int numT, freeP;
    int offset = 0;

    // Parsed line 1: "numTuples nextFreePage"
    sscanf(data, "%d %d\n%n", &numT, &freeP, &offset);
    tblData->numTuples    = numT;
    tblData->nextFreePage = freeP;

    // Parsed line 2: number of attributes
    data += offset;
    int numAttr;
    int used = 0;
    sscanf(data, "%d\n%n", &numAttr, &used);
    data += used;

    // Allocated arrays for attribute info
    char **attrNames = (char **) malloc(numAttr * sizeof(char*));
    DataType *dataTypes = (DataType *) malloc(numAttr * sizeof(DataType));
    int *typeLength = (int *) malloc(numAttr * sizeof(int));

    for (int i = 0; i < numAttr; i++)
    {
        int dt, tLen;
        char nameBuf[128];
        int rLen = 0;
        sscanf(data, "%d %d %s\n%n", &dt, &tLen, nameBuf, &rLen);
        data += rLen;

        dataTypes[i]  = (DataType) dt;
        typeLength[i] = tLen;
        attrNames[i]  = (char*) malloc(strlen(nameBuf) + 1);
        strcpy(attrNames[i], nameBuf);
    }

    // Created a default "keys" array with one attribute (assumed index 0)
    int *keys = (int*) malloc(sizeof(int));
    keys[0] = 0;

    // Built the schema from these arrays
    Schema *sc = createSchema(numAttr, attrNames, dataTypes, typeLength, 1, keys);
    rel->schema = sc;

    // Computed record size
    tblData->recordSize = computeRecordSize(sc);

    unpinPage(&tblData->bufferPool, &page);
    return RC_OK;
}

/*
 * computeMaxSlots
 * ---------------
 * Calculated how many records (slots) could fit in one page. We used 4 bytes
 * to store "slotsUsed," plus 1 usage byte per slot, plus (recSize * #slots).
 * This solved N * (recSize+1) + 4 <= PAGE_SIZE to get N.
 */
static int
computeMaxSlots(int recSize)
{
    return (PAGE_SIZE - 4) / (recSize + 1);
}

/*
 * getSlotFlag / setSlotFlag
 * -------------------------
 * Provided quick access to the 1-byte usage flags for each slot.
 * If usage is 0 => free, 1 => used, etc.
 */
static int getSlotFlag(char *data, int slotNum) {
    return (unsigned char)(data[4 + slotNum]);
}
static void setSlotFlag(char *data, int slotNum, int val) {
    data[4 + slotNum] = (char) val;
}

/* --------------------------------------------------------------------------
   Record Manager Interface
   -------------------------------------------------------------------------- */

/* 
 * initRecordManager
 * -----------------
 * Performed global initialization if needed (here we just called initStorageManager).
 */
RC initRecordManager(void *mgmtData)
{
    initStorageManager();
    return RC_OK;
}

/*
 * shutdownRecordManager
 * ---------------------
 * Performed a global shutdown if needed. Here, it was just a stub.
 */
RC shutdownRecordManager()
{
    return RC_OK;
}

/*
 * createTable
 * -----------
 * Created a page file for the table, set up the mgmt data, wrote initial table
 * metadata, and then shut down the buffer manager. Freed the mgmt data after done.
 */
RC createTable(char *name, Schema *schema)
{
    RC rc = createPageFile(name);
    if (rc != RC_OK) return rc;

    // Allocated mgmt data for the table
    RM_TableMgmtData *tblData = (RM_TableMgmtData *) malloc(sizeof(RM_TableMgmtData));
    tblData->numTuples    = 0;
    tblData->nextFreePage = -1;
    tblData->recordSize   = computeRecordSize(schema);

    // Initialized a buffer manager for this table
    rc = initBufferPool(&tblData->bufferPool, name, /*numPages*/3, RS_FIFO, NULL);
    if (rc != RC_OK) return rc;

    // Built a temporary RM_TableData struct so we could call writeTableInfo
    RM_TableData tmp;
    tmp.name   = name;
    tmp.schema = schema;
    tmp.mgmtData = tblData;

    // Wrote out the table metadata to page 0
    rc = writeTableInfo(&tmp);
    if (rc != RC_OK) return rc;

    // Shut down the buffer manager
    rc = shutdownBufferPool(&tblData->bufferPool);
    if (rc != RC_OK) return rc;

    // Freed the mgmt data
    free(tblData);
    return RC_OK;
}

/*
 * openTable
 * ---------
 * Opened an existing table by creating new mgmt data, initing a buffer pool,
 * and reading table info from page 0. Set rel->schema and rel->mgmtData.
 */
RC openTable(RM_TableData *rel, char *name)
{
    RM_TableMgmtData *tblData = (RM_TableMgmtData *) malloc(sizeof(RM_TableMgmtData));

    RC rc = initBufferPool(&tblData->bufferPool, name, 3, RS_FIFO, NULL);
    if (rc != RC_OK) return rc;

    rel->name     = name;
    rel->schema   = NULL;
    rel->mgmtData = tblData;

    rc = readTableInfo(rel);
    if (rc != RC_OK) return rc;

    return RC_OK;
}

/*
 * closeTable
 * ----------
 * Wrote out metadata, shut down buffer pool, freed schema and mgmt data.
 */
RC closeTable(RM_TableData *rel)
{
    RM_TableMgmtData *tblData = (RM_TableMgmtData*) rel->mgmtData;
    RC rc = writeTableInfo(rel);
    if (rc != RC_OK) return rc;

    rc = shutdownBufferPool(&tblData->bufferPool);
    if (rc != RC_OK) return rc;

    freeSchema(rel->schema);
    rel->schema = NULL;

    free(tblData);
    rel->mgmtData = NULL;
    return RC_OK;
}

/*
 * deleteTable
 * -----------
 * Destroyed the page file on disk for the table.
 */
RC deleteTable(char *name)
{
    return destroyPageFile(name);
}

/*
 * getNumTuples
 * ------------
 * Returned the number of tuples stored in the table (from mgmt data).
 */
int getNumTuples(RM_TableData *rel)
{
    RM_TableMgmtData *tblData = (RM_TableMgmtData*) rel->mgmtData;
    return tblData->numTuples;
}

/* --------------------------------------------------------------------------
   Record-level operations
   -------------------------------------------------------------------------- */

/*
 * insertRecord
 * ------------
 * Inserted a new record into the table. If nextFreePage < 1, appended a new data page.
 * Found a free slot or created a new page. Copied record->data into the free slot,
 * updated usage array, incremented numTuples. 
 */
RC insertRecord(RM_TableData *rel, Record *record)
{
    RM_TableMgmtData *tblData = (RM_TableMgmtData*) rel->mgmtData;
    BM_PageHandle page;
    RC rc;
    int recSize = tblData->recordSize;
    int pageNum = tblData->nextFreePage;

    // If nextFreePage was not valid, appended a new data page
    if (pageNum < 1)
    {
        SM_FileHandle fHandle;
        rc = openPageFile(rel->name, &fHandle);
        if (rc != RC_OK) return rc;

        pageNum = fHandle.totalNumPages;  // The new data page
        ensureCapacity(pageNum + 1, &fHandle);
        closePageFile(&fHandle);

        // pinned that new page
        rc = pinPage(&tblData->bufferPool, &page, pageNum);
        if (rc != RC_OK) return rc;

        // zeroed out the entire page
        memset(page.data, 0, PAGE_SIZE);

        // stored slotsUsed = 0 in first 4 bytes
        int slotsUsed = 0;
        memcpy(page.data, &slotsUsed, sizeof(int));

        markDirty(&tblData->bufferPool, &page);
        unpinPage(&tblData->bufferPool, &page);

        tblData->nextFreePage = pageNum;
    }

    // pinned the nextFreePage
    rc = pinPage(&tblData->bufferPool, &page, tblData->nextFreePage);
    if (rc != RC_OK) return rc;

    char *data = page.data;
    int slotsUsed;
    memcpy(&slotsUsed, data, sizeof(int));

    int maxSlots = computeMaxSlots(recSize);
    int freeSlot = -1;

    // looked for a free slot
    for (int i = 0; i < maxSlots; i++)
    {
        if (getSlotFlag(data, i) == 0)
        {
            freeSlot = i;
            break;
        }
    }

    // if no slot was free, set nextFreePage = -1, unpin, reinsert
    if (freeSlot < 0)
    {
        tblData->nextFreePage = -1;
        markDirty(&tblData->bufferPool, &page);
        unpinPage(&tblData->bufferPool, &page);
        return insertRecord(rel, record);
    }

    // wrote record->data into the page
    int offset = 4 + maxSlots + (freeSlot * recSize);
    memcpy(data + offset, record->data, recSize);

    // updated usage
    setSlotFlag(data, freeSlot, 1);
    slotsUsed++;
    memcpy(data, &slotsUsed, sizeof(int));

    // assigned record->id
    record->id.page = tblData->nextFreePage;
    record->id.slot = freeSlot;

    markDirty(&tblData->bufferPool, &page);
    unpinPage(&tblData->bufferPool, &page);

    tblData->numTuples++;

    // If page was full, set nextFreePage = -1, else keep the same page
    if (slotsUsed == maxSlots)
        tblData->nextFreePage = -1;
    else
        tblData->nextFreePage = pageNum;

    return RC_OK;
}

/*
 * deleteRecord
 * ------------
 * Freed a slot by marking usage=0, decreased the number of used slots,
 * and set nextFreePage if needed.
 */
RC deleteRecord(RM_TableData *rel, RID id)
{
    RM_TableMgmtData *tblData = (RM_TableMgmtData*) rel->mgmtData;
    BM_PageHandle page;
    RC rc = pinPage(&tblData->bufferPool, &page, id.page);
    if (rc != RC_OK) return rc;

    char *data = page.data;
    int slotsUsed;
    memcpy(&slotsUsed, data, sizeof(int));

    // if usage was 1 => used, set it to 0 => free
    if (getSlotFlag(data, id.slot) == 1)
    {
        setSlotFlag(data, id.slot, 0);
        slotsUsed--;
        memcpy(data, &slotsUsed, sizeof(int));
        tblData->numTuples--;

        // if the page used to be full, we updated nextFreePage to this one
        int maxSlots = computeMaxSlots(tblData->recordSize);
        if (slotsUsed == maxSlots - 1)
        {
            tblData->nextFreePage = id.page;
        }
    }

    markDirty(&tblData->bufferPool, &page);
    unpinPage(&tblData->bufferPool, &page);
    return RC_OK;
}

/*
 * updateRecord
 * ------------
 * Overwrote the record data in an existing slot, if that slot usage was 1.
 */
RC updateRecord(RM_TableData *rel, Record *record)
{
    RM_TableMgmtData *tblData = (RM_TableMgmtData*) rel->mgmtData;
    BM_PageHandle page;
    int pageNum = record->id.page;
    int slotNum = record->id.slot;

    RC rc = pinPage(&tblData->bufferPool, &page, pageNum);
    if (rc != RC_OK) return rc;

    // if usage was 0 => cannot update
    if (getSlotFlag(page.data, slotNum) == 0)
    {
        unpinPage(&tblData->bufferPool, &page);
        return RC_READ_NON_EXISTING_PAGE;
    }

    int maxSlots = computeMaxSlots(tblData->recordSize);
    int offset = 4 + maxSlots + slotNum * tblData->recordSize;
    memcpy(page.data + offset, record->data, tblData->recordSize);

    markDirty(&tblData->bufferPool, &page);
    unpinPage(&tblData->bufferPool, &page);
    return RC_OK;
}

/*
 * getRecord
 * ---------
 * Copied the record data out of the slot if usage was 1; if usage was 0, returned RC_RM_NO_MORE_TUPLES.
 */
RC getRecord(RM_TableData *rel, RID id, Record *record)
{
    RM_TableMgmtData *tblData = (RM_TableMgmtData*) rel->mgmtData;
    BM_PageHandle page;
    RC rc = pinPage(&tblData->bufferPool, &page, id.page);
    if (rc != RC_OK) return rc;

    // check usage
    if (getSlotFlag(page.data, id.slot) == 0)
    {
        unpinPage(&tblData->bufferPool, &page);
        return RC_RM_NO_MORE_TUPLES;
    }

    int maxSlots = computeMaxSlots(tblData->recordSize);
    int offset   = 4 + maxSlots + (id.slot * tblData->recordSize);
    memcpy(record->data, page.data + offset, tblData->recordSize);

    record->id.page = id.page;
    record->id.slot = id.slot;

    unpinPage(&tblData->bufferPool, &page);
    return RC_OK;
}

/* --------------------------------------------------------------------------
   Scan operations
   -------------------------------------------------------------------------- */

/*
 * startScan
 * ---------
 * Allocated mgmt data for scanning: currentPage=1, currentSlot=0, stored the condition.
 */
RC startScan(RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{
    RM_ScanMgmtData *scanData = (RM_ScanMgmtData*) malloc(sizeof(RM_ScanMgmtData));
    scanData->currentPage = 1; 
    scanData->currentSlot = 0;
    scanData->cond        = cond;

    scan->rel      = rel;
    scan->mgmtData = scanData;
    return RC_OK;
}

/*
 * next
 * ----
 * Retrieved the next matching record by scanning pages from currentPage onward,
 * skipping free slots (usage=0), returning the first that satisfies the condition (if any).
 */
RC next(RM_ScanHandle *scan, Record *record)
{
    RM_TableData *rel         = scan->rel;
    RM_TableMgmtData *tblData = (RM_TableMgmtData*) rel->mgmtData;
    RM_ScanMgmtData *sdata    = (RM_ScanMgmtData*) scan->mgmtData;

    int recSize = tblData->recordSize;
    int maxSlots= computeMaxSlots(recSize);

    while (true)
    {
        if (sdata->currentPage < 1)
            return RC_RM_NO_MORE_TUPLES;

        BM_PageHandle page;
        // If pinPage fails => presumably no more pages exist
        if (pinPage(&tblData->bufferPool, &page, sdata->currentPage) != RC_OK)
            return RC_RM_NO_MORE_TUPLES;

        char *data = page.data;
        int slotsUsed;
        memcpy(&slotsUsed, data, sizeof(int));

        bool found = false;
        while (sdata->currentSlot < maxSlots)
        {
            if (getSlotFlag(data, sdata->currentSlot) == 1)
            {
                // Copied the record
                int offset = 4 + maxSlots + (sdata->currentSlot * recSize);
                memcpy(record->data, data + offset, recSize);
                record->id.page = sdata->currentPage;
                record->id.slot = sdata->currentSlot;

                // If there was a condition, we evaluated it
                if (sdata->cond != NULL)
                {
                    Value *res;
                    evalExpr(record, rel->schema, sdata->cond, &res);
                    bool pass = (res->v.boolV == TRUE);
                    freeVal(res);
                    if (pass)
                    {
                        found = true;
                        sdata->currentSlot++;
                        break;
                    }
                }
                else
                {
                    // No condition => matched by default
                    found = true;
                    sdata->currentSlot++;
                    break;
                }
            }
            sdata->currentSlot++;
        }

        unpinPage(&tblData->bufferPool, &page);

        if (found)
            return RC_OK;

        // Moved on to the next page
        sdata->currentPage++;
        sdata->currentSlot=0;

        // Checked if new page was beyond file size
        SM_FileHandle fHandle;
        if (openPageFile(rel->name, &fHandle) != RC_OK)
            return RC_RM_NO_MORE_TUPLES;
        bool beyond = (sdata->currentPage >= fHandle.totalNumPages);
        closePageFile(&fHandle);
        if (beyond)
            return RC_RM_NO_MORE_TUPLES;
    }
}

/*
 * closeScan
 * ---------
 * Freed the mgmt data for the scan.
 */
RC closeScan(RM_ScanHandle *scan)
{
    free(scan->mgmtData);
    scan->mgmtData = NULL;
    return RC_OK;
}

/* --------------------------------------------------------------------------
   Schema & Record manipulation
   -------------------------------------------------------------------------- */

/*
 * getRecordSize
 * -------------
 * Returned the size in bytes of a record for a given schema.
 */
int getRecordSize(Schema *schema)
{
    return computeRecordSize(schema);
}

/*
 * createSchema
 * ------------
 * Created a schema from the given arrays. Allocated and returned the pointer.
 */
Schema *createSchema(int numAttr, char **attrNames, DataType *dataTypes,
                     int *typeLength, int keySize, int *keys)
{
    Schema *sc = (Schema*) malloc(sizeof(Schema));
    sc->numAttr    = numAttr;
    sc->attrNames  = attrNames;
    sc->dataTypes  = dataTypes;
    sc->typeLength = typeLength;
    sc->keySize    = keySize;
    sc->keyAttrs   = keys;
    return sc;
}

/*
 * freeSchema
 * ----------
 * Freed the arrays inside the schema and then the schema object.
 */
RC freeSchema(Schema *schema)
{
    if (schema == NULL) return RC_OK;
    for (int i = 0; i < schema->numAttr; i++)
        free(schema->attrNames[i]);

    free(schema->attrNames);
    free(schema->dataTypes);
    free(schema->typeLength);
    free(schema->keyAttrs);
    free(schema);
    return RC_OK;
}

/*
 * createRecord
 * ------------
 * Allocated a Record struct, allocated enough data for the record->data,
 * and set page/slot to -1.
 */
RC createRecord(Record **record, Schema *schema)
{
    *record = (Record*) malloc(sizeof(Record));
    (*record)->data = (char*) calloc(getRecordSize(schema), sizeof(char));
    (*record)->id.page = -1;
    (*record)->id.slot = -1;
    return RC_OK;
}

/*
 * freeRecord
 * ----------
 * Freed the record->data and then the record object itself.
 */
RC freeRecord(Record *record)
{
    if (!record) return RC_OK;
    free(record->data);
    free(record);
    return RC_OK;
}

/*
 * getAttr
 * -------
 * Fetched a single attribute from record->data by computing the offset for that attribute.
 * Copied it into a newly allocated Value, based on the attribute's data type.
 */
RC getAttr(Record *record, Schema *schema, int attrNum, Value **value)
{
    char *base = record->data;
    int offset = 0;

    // Calculated offset by skipping the sizes of earlier attributes
    for (int i = 0; i < attrNum; i++)
    {
        switch (schema->dataTypes[i])
        {
            case DT_INT:    offset += sizeof(int);    break;
            case DT_FLOAT:  offset += sizeof(float);  break;
            case DT_BOOL:   offset += sizeof(bool);   break;
            case DT_STRING: offset += schema->typeLength[i]; break;
        }
    }

    // Allocated the Value object
    *value = (Value*) malloc(sizeof(Value));
    (*value)->dt = schema->dataTypes[attrNum];

    // Copied from record->data + offset
    switch ((*value)->dt)
    {
        case DT_INT:
        {
            int val;
            memcpy(&val, base + offset, sizeof(int));
            (*value)->v.intV = val;
        }
        break;
        case DT_FLOAT:
        {
            float f;
            memcpy(&f, base + offset, sizeof(float));
            (*value)->v.floatV = f;
        }
        break;
        case DT_BOOL:
        {
            bool b;
            memcpy(&b, base + offset, sizeof(bool));
            (*value)->v.boolV = b;
        }
        break;
        case DT_STRING:
        {
            int len = schema->typeLength[attrNum];
            (*value)->v.stringV = (char*) malloc(len + 1);
            memcpy((*value)->v.stringV, base + offset, len);
            // Null terminator
            (*value)->v.stringV[len] = '\0';
        }
        break;
    }
    return RC_OK;
}

/*
 * setAttr
 * -------
 * Computed the offset for the specified attribute, then wrote the new value's
 * bytes into record->data. For strings, we wrote up to 'len' characters and
 * padded with zero if needed.
 */
RC setAttr(Record *record, Schema *schema, int attrNum, Value *value)
{
    char *base = record->data;
    int offset = 0;

    // Found how many bytes to skip for earlier attributes
    for (int i = 0; i < attrNum; i++)
    {
        switch (schema->dataTypes[i])
        {
            case DT_INT:    offset += sizeof(int);    break;
            case DT_FLOAT:  offset += sizeof(float);  break;
            case DT_BOOL:   offset += sizeof(bool);   break;
            case DT_STRING: offset += schema->typeLength[i]; break;
        }
    }

    // Wrote the attribute
    switch (value->dt)
    {
        case DT_INT:
            memcpy(base + offset, &value->v.intV, sizeof(int));
            break;
        case DT_FLOAT:
            memcpy(base + offset, &value->v.floatV, sizeof(float));
            break;
        case DT_BOOL:
            memcpy(base + offset, &value->v.boolV, sizeof(bool));
            break;
        case DT_STRING:
        {
            int len = schema->typeLength[attrNum];
            memset(base + offset, 0, len);
            strncpy(base + offset, value->v.stringV, len);
        }
        break;
    }
    return RC_OK;
}