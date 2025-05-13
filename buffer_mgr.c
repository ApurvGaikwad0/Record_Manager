#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "dberror.h"
#include "dt.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/*
 * Data Structures
 * --------------------------------------------------------------------------
 * 
 * This file defined two main structures:
 *   1. PageFrame: Represented one page frame in memory, which included the
 *                 actual page data, page number, dirty status, fix count,
 *                 and a usage counter for LRU or CLOCK.
 *   2. BM_MgmtData: Managed the array of PageFrame objects and also tracked
 *                   read/write IO counts and a clock pointer if needed.
 */

/* This struct had represented one page frame in the buffer pool. */
typedef struct PageFrame
{
    char *data;         // This had pointed to actual page data
    PageNumber pageNum; // This had indicated which page in the file was stored
    bool dirty;         // This was set to true if the page had been modified
    int fixCount;       // This was the number of clients currently using the page
    // usage was used for LRU, CLOCK, or other replacement strategies
    int usage;          
} PageFrame;

/* This struct contained additional info for the entire buffer pool. */
typedef struct BM_MgmtData
{
    PageFrame *frames;  // This had been an array of PageFrame structures
    int readIO;         // This counted how many reads were performed
    int writeIO;        // This counted how many writes were performed
    int clockPointer;   // If using CLOCK, this was the pointer
} BM_MgmtData;

/*
 * HELPER PROTOTYPES
 * --------------------------------------------------------------------------
 * These static helper functions had been used internally for tasks like
 * initializing page frames, locating a page in memory, finding a free frame,
 * picking a victim, or writing a dirty page to disk.
 */

static RC initPageFrameArray(BM_MgmtData *mgmt, int numPages);
static int findPageFrame(BM_MgmtData *mgmt, int numPages, PageNumber pageNum);
static int findFreeFrame(BM_MgmtData *mgmt, int numPages);
static int findVictimFrame(BM_BufferPool *bm, BM_MgmtData *mgmt);
static RC writeDirtyPageToDisk(BM_BufferPool *bm, PageFrame *pf);

/* 
 * initBufferPool
 * --------------
 * This function initialized the buffer pool by:
 *  1) Checking if the page file was accessible
 *  2) Allocating BM_MgmtData
 *  3) Creating an array of PageFrame
 *  4) Setting up initial read/write counters and clockPointer
 */
RC initBufferPool(BM_BufferPool *const bm,
                  const char *const pageFileName,
                  const int numPages,
                  ReplacementStrategy strategy,
                  void *stratData)
{
    // Verified the file could be opened, to ensure it existed
    FILE *fp = fopen(pageFileName, "r");
    if (!fp)
        return RC_FILE_NOT_FOUND;
    fclose(fp);

    // Stored basic info about the buffer pool
    bm->pageFile = (char*)pageFileName;
    bm->numPages = numPages;
    bm->strategy = strategy;

    // Allocated management data
    BM_MgmtData *mgmt = (BM_MgmtData*) malloc(sizeof(BM_MgmtData));
    if (!mgmt)
        return RC_MEMORY_ALLOCATION_ERROR;

    mgmt->readIO       = 0;
    mgmt->writeIO      = 0;
    mgmt->clockPointer = 0;

    // Allocated and initialized an array of PageFrame
    RC rc = initPageFrameArray(mgmt, numPages);
    if (rc != RC_OK)
    {
        free(mgmt);
        return rc;
    }

    // Stored pointer to mgmt in bm->mgmtData
    bm->mgmtData = mgmt;
    return RC_OK;
}

/*
 * shutdownBufferPool
 * ------------------
 * This function:
 *  1) Called forceFlushPool to ensure all dirty pages were written
 *  2) Verified that no page remained pinned
 *  3) Freed all frames and mgmt data
 */
RC shutdownBufferPool(BM_BufferPool *const bm)
{
    if (!bm || !bm->mgmtData)
        return RC_ERROR;

    BM_MgmtData *mgmt = (BM_MgmtData*) bm->mgmtData;

    // Flushed all dirty pages
    RC rc = forceFlushPool(bm);
    if (rc != RC_OK)
        return rc;

    // Ensured no pinned pages remained
    for (int i=0; i<bm->numPages; i++)
    {
        if (mgmt->frames[i].fixCount > 0)
            return RC_ERROR; // or a specialized code if pinned pages are not allowed
    }

    // Freed each page's data
    for (int i=0; i<bm->numPages; i++)
    {
        if (mgmt->frames[i].data)
            free(mgmt->frames[i].data);
    }

    // Freed the frames array, then mgmt data
    free(mgmt->frames);
    free(mgmt);

    bm->mgmtData = NULL;
    return RC_OK;
}

/*
 * forceFlushPool
 * --------------
 * This wrote all dirty pages with fixCount=0 out to disk. For each
 * frame that was dirty and fixCount=0, it called writeDirtyPageToDisk.
 */
RC forceFlushPool(BM_BufferPool *const bm)
{
    if (!bm || !bm->mgmtData)
        return RC_ERROR;

    BM_MgmtData *mgmt = (BM_MgmtData*) bm->mgmtData;

    // Checked each frame
    for (int i=0; i<bm->numPages; i++)
    {
        PageFrame *pf = &mgmt->frames[i];
        // If the page was dirty and not pinned
        if (pf->dirty && pf->fixCount == 0)
        {
            RC rc = writeDirtyPageToDisk(bm, pf);
            if (rc != RC_OK)
                return rc;
            pf->dirty = false;
        }
    }
    return RC_OK;
}

/*
 * markDirty
 * ---------
 * Marked a given page as dirty in the buffer pool. It found which frame
 * stored page->pageNum, then set dirty=true.
 */
RC markDirty(BM_BufferPool *const bm, BM_PageHandle *const page)
{
    if (!bm || !bm->mgmtData || !page)
        return RC_ERROR;

    BM_MgmtData *mgmt = (BM_MgmtData*) bm->mgmtData;
    int index = findPageFrame(mgmt, bm->numPages, page->pageNum);
    if (index < 0)
        return RC_ERROR;

    mgmt->frames[index].dirty = true;
    return RC_OK;
}

/*
 * unpinPage
 * ---------
 * Decremented fixCount for a page in the buffer pool. It found the frame
 * with page->pageNum, then fixCount-- if it was >0.
 */
RC unpinPage(BM_BufferPool *const bm, BM_PageHandle *const page)
{
    if (!bm || !bm->mgmtData || !page)
        return RC_ERROR;

    BM_MgmtData *mgmt = (BM_MgmtData*) bm->mgmtData;
    int index = findPageFrame(mgmt, bm->numPages, page->pageNum);
    if (index < 0)
        return RC_ERROR;

    if (mgmt->frames[index].fixCount > 0)
        mgmt->frames[index].fixCount--;

    return RC_OK;
}

/*
 * forcePage
 * ---------
 * Wrote a single dirty page to disk. If dirty, it called
 * writeDirtyPageToDisk, then set dirty=false.
 */
RC forcePage(BM_BufferPool *const bm, BM_PageHandle *const page)
{
    if (!bm || !bm->mgmtData || !page)
        return RC_ERROR;

    BM_MgmtData *mgmt = (BM_MgmtData*) bm->mgmtData;
    int index = findPageFrame(mgmt, bm->numPages, page->pageNum);
    if (index < 0)
        return RC_ERROR;

    // If dirty, wrote out
    if (mgmt->frames[index].dirty)
    {
        RC rc = writeDirtyPageToDisk(bm, &mgmt->frames[index]);
        if (rc != RC_OK)
            return rc;
        mgmt->frames[index].dirty = false;
    }
    return RC_OK;
}

/*
 * pinPage
 * -------
 * Pinned the requested page into the buffer pool. If the page was found in memory,
 * fixCount++ and usage++ for LRU. If not found, found a free frame or victim,
 * wrote out if dirty, read from disk, updated readIO, and set fixCount=1, usage=1.
 */
RC pinPage(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum)
{
    if (!bm || !bm->mgmtData)
        return RC_ERROR;
    if (pageNum < 0)
        return RC_ERROR;

    BM_MgmtData *mgmt = (BM_MgmtData*) bm->mgmtData;

    // Checked if page was already in memory
    int idx = findPageFrame(mgmt, bm->numPages, pageNum);
    if (idx >= 0)
    {
        // Found it => fixCount++, usage++ (for LRU)
        mgmt->frames[idx].fixCount++;
        mgmt->frames[idx].usage++;
        page->data = mgmt->frames[idx].data;
        page->pageNum = pageNum;
        return RC_OK;
    }
    else
    {
        // Not in memory => find free or victim
        int freeIndex = findFreeFrame(mgmt, bm->numPages);
        if (freeIndex < 0)
            freeIndex = findVictimFrame(bm, mgmt);

        // If victim was dirty, wrote out
        if (mgmt->frames[freeIndex].dirty)
        {
            RC rc = writeDirtyPageToDisk(bm, &mgmt->frames[freeIndex]);
            if (rc != RC_OK)
                return rc;
            mgmt->frames[freeIndex].dirty = false;
        }

        // Read from disk
        SM_FileHandle fh;
        if (openPageFile(bm->pageFile, &fh) != RC_OK)
            return RC_ERROR;

        // If the frame had no data allocated yet, allocated
        if (!mgmt->frames[freeIndex].data)
            mgmt->frames[freeIndex].data = calloc(PAGE_SIZE, sizeof(char));

        // Ensured capacity, then read
        if (ensureCapacity(pageNum+1, &fh) != RC_OK)
            return RC_ERROR;

        fseek(fh.mgmtInfo, pageNum * PAGE_SIZE, SEEK_SET);
        size_t ret = fread(mgmt->frames[freeIndex].data, 1, PAGE_SIZE, fh.mgmtInfo);
        if (ret < PAGE_SIZE)
            memset(mgmt->frames[freeIndex].data + ret, 0, PAGE_SIZE - ret);
        mgmt->readIO++;
        closePageFile(&fh);

        // Updated the frame info
        mgmt->frames[freeIndex].pageNum  = pageNum;
        mgmt->frames[freeIndex].dirty    = false;
        mgmt->frames[freeIndex].fixCount = 1;
        mgmt->frames[freeIndex].usage    = 1;

        // Returned via page handle
        page->data    = mgmt->frames[freeIndex].data;
        page->pageNum = pageNum;
        return RC_OK;
    }
}

/*
 * getFrameContents
 * ----------------
 * Returned an array of PageNumber that indicated which page was in each frame.
 * NO_PAGE if no page loaded (pageNum == -1).
 */
PageNumber *getFrameContents(BM_BufferPool *const bm)
{
    if (!bm || !bm->mgmtData)
        return NULL;
    BM_MgmtData *mgmt = (BM_MgmtData*) bm->mgmtData;

    PageNumber *arr = malloc(sizeof(PageNumber) * bm->numPages);
    for (int i=0; i<bm->numPages; i++)
    {
        if (mgmt->frames[i].pageNum == -1)
            arr[i] = NO_PAGE;
        else
            arr[i] = mgmt->frames[i].pageNum;
    }
    return arr;
}

/*
 * getDirtyFlags
 * -------------
 * Returned an array of booleans indicating which frames were dirty.
 */
bool *getDirtyFlags(BM_BufferPool *const bm)
{
    if (!bm || !bm->mgmtData)
        return NULL;
    BM_MgmtData *mgmt = (BM_MgmtData*) bm->mgmtData;

    bool *arr = malloc(sizeof(bool)*bm->numPages);
    for (int i=0; i<bm->numPages; i++)
        arr[i] = mgmt->frames[i].dirty;
    return arr;
}

/*
 * getFixCounts
 * ------------
 * Returned an array of fixCounts for each frame in the buffer pool.
 */
int *getFixCounts(BM_BufferPool *const bm)
{
    if (!bm || !bm->mgmtData)
        return NULL;
    BM_MgmtData *mgmt = (BM_MgmtData*) bm->mgmtData;

    int *arr = malloc(sizeof(int)*bm->numPages);
    for (int i=0; i<bm->numPages; i++)
        arr[i] = mgmt->frames[i].fixCount;
    return arr;
}

/*
 * getNumReadIO
 * ------------
 * Returned how many read operations had occurred so far.
 */
int getNumReadIO(BM_BufferPool *const bm)
{
    if (!bm || !bm->mgmtData)
        return 0;
    BM_MgmtData *mgmt = (BM_MgmtData*) bm->mgmtData;
    return mgmt->readIO;
}

/*
 * getNumWriteIO
 * -------------
 * Returned how many write operations had occurred so far.
 */
int getNumWriteIO(BM_BufferPool *const bm)
{
    if (!bm || !bm->mgmtData)
        return 0;
    BM_MgmtData *mgmt = (BM_MgmtData*) bm->mgmtData;
    return mgmt->writeIO;
}

/*
 * HELPER IMPLEMENTATIONS
 * --------------------------------------------------------------------------
 * Internal functions for initializing frames, finding them, or writing them.
 */

/*
 * initPageFrameArray
 * ------------------
 * Allocated an array of PageFrame for mgmt->frames, set each pageNum=-1,
 * dirty=false, fixCount=0, usage=0. Returned RC_OK on success.
 */
static RC initPageFrameArray(BM_MgmtData *mgmt, int numPages)
{
    mgmt->frames = (PageFrame*) malloc(sizeof(PageFrame)*numPages);
    if (!mgmt->frames)
        return RC_MEMORY_ALLOCATION_ERROR;

    for (int i=0; i<numPages; i++)
    {
        mgmt->frames[i].data     = NULL;
        mgmt->frames[i].pageNum  = -1;
        mgmt->frames[i].dirty    = false;
        mgmt->frames[i].fixCount = 0;
        mgmt->frames[i].usage    = 0;
    }
    return RC_OK;
}

/*
 * findPageFrame
 * -------------
 * Looked in mgmt->frames for the given pageNum. Returned the index or -1 if not found.
 */
static int findPageFrame(BM_MgmtData *mgmt, int numPages, PageNumber pageNum)
{
    for (int i=0; i<numPages; i++)
    {
        if (mgmt->frames[i].pageNum == pageNum)
            return i;
    }
    return -1;
}

/*
 * findFreeFrame
 * -------------
 * Searched for a frame with pageNum == -1 (meaning it was free). Returned
 * that index or -1 if none free.
 */
static int findFreeFrame(BM_MgmtData *mgmt, int numPages)
{
    for (int i=0; i<numPages; i++)
    {
        if (mgmt->frames[i].pageNum == -1)
            return i;
    }
    return -1;
}

/*
 * findVictimFrame
 * ---------------
 * For a simple LRU: picked the frame with the smallest usage among those
 * with fixCount=0. If all pinned => returned 0 as fallback.
 */
static int findVictimFrame(BM_BufferPool *bm, BM_MgmtData *mgmt)
{
    int victimIndex = -1;
    int leastUsage = 2147483647; // a large sentinel
    for (int i=0; i < bm->numPages; i++)
    {
        if (mgmt->frames[i].fixCount == 0)
        {
            if (mgmt->frames[i].usage < leastUsage)
            {
                leastUsage = mgmt->frames[i].usage;
                victimIndex = i;
            }
        }
    }
    // if all pinned => fallback to index 0
    if (victimIndex < 0)
        victimIndex = 0;
    return victimIndex;
}

/*
 * writeDirtyPageToDisk
 * --------------------
 * Opened the file, sought to pf->pageNum * PAGE_SIZE, wrote pf->data, closed,
 * incremented mgmt->writeIO. Returned RC_OK if we wrote exactly PAGE_SIZE bytes,
 * else RC_ERROR.
 */
static RC writeDirtyPageToDisk(BM_BufferPool *bm, PageFrame *pf)
{
    SM_FileHandle fh;
    if (openPageFile(bm->pageFile, &fh) != RC_OK)
        return RC_FILE_NOT_FOUND;

    fseek(fh.mgmtInfo, pf->pageNum * PAGE_SIZE, SEEK_SET);
    size_t wrote = fwrite(pf->data, 1, PAGE_SIZE, fh.mgmtInfo);
    fclose(fh.mgmtInfo);

    BM_MgmtData *mgmt = (BM_MgmtData*) bm->mgmtData;
    mgmt->writeIO++;

    return (wrote == PAGE_SIZE) ? RC_OK : RC_ERROR;
}