
#include "storage_mgr.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "dberror.h"

/* Handling Page Files */

// Initialized Storage Manager
void initStorageManager(void)
{
  // Displayed initialization message
  printf("\n******************** Storage Manager Initialized Successfully ********************\n\n");
}

/* FILE HANDLING FUNCTIONS */

// Created new page file with given fileName
RC createPageFile(char *fileName)
{
    FILE *filePointer = fopen(fileName, "w");
    // Checked if file was opened successfully
    if (filePointer == NULL)
    {
      printf("Failed to create file.\n");
      return RC_WRITE_FAILED;
    }
    
    // Created initial page with zero bytes
    char *initialPage = (char *)calloc(PAGE_SIZE, sizeof(char));
    if (fwrite(initialPage, sizeof(char), PAGE_SIZE, filePointer) < PAGE_SIZE)
    {
      printf("Failed to initialize page.\n");
      fclose(filePointer);
      free(initialPage);
      return RC_WRITE_FAILED;
    }
    
    printf("File created successfully.\n");
    fclose(filePointer);
    free(initialPage);
    return RC_OK;
}

// Opened existing page file and initialized file handle
RC openPageFile(char *fileName, SM_FileHandle *fileHandle)
{
    FILE *filePointer = fopen(fileName, "r+");
    // Verified file existence
    if (!filePointer)
    {
      return RC_FILE_NOT_FOUND;
    }

    // Determined file size
    fseek(filePointer, 0, SEEK_END);
    fileHandle->totalNumPages = ftell(filePointer) / PAGE_SIZE;
    rewind(filePointer);

    // Initialized file handle properties
    fileHandle->fileName = fileName;
    fileHandle->curPagePos = 0;
    fileHandle->mgmtInfo = filePointer;

    printf("Opened file: %s\n", fileName);
    return RC_OK;
}

// Closed open page file and reset handle
RC closePageFile(SM_FileHandle *fileHandle)
{
  // Validated file handle state
  if (fileHandle == NULL || fileHandle->mgmtInfo == NULL) 
    return RC_FILE_HANDLE_NOT_INIT;

  FILE *filePointer = (FILE *)fileHandle->mgmtInfo;
  
  // Reset handle properties
  fileHandle->fileName = NULL;
  fileHandle->curPagePos = 0;
  fileHandle->totalNumPages = 0;
  fileHandle->mgmtInfo = NULL;

  fclose(filePointer);
  printf("Closed file successfully.\n");
  return RC_OK;
}

// Removed page file from storage
RC destroyPageFile(char *fileName)
{
  // Verified file existence before deletion
  FILE *filePointer = fopen(fileName, "r");
  if (!filePointer)
  {
    return RC_FILE_NOT_FOUND;
  }
  fclose(filePointer);

  // Attempted file deletion
  if (remove(fileName) != 0)
  {
    printf("Failed to delete file.\n");
    return RC_FILE_NOT_FOUND;
  }
  
  printf("Destroyed file: %s\n", fileName);
  return RC_OK;
}

/* READING BLOCKS FROM DISK FUNCTIONS */

// Retrieved specified block from disk
RC readBlock(int pageNum, SM_FileHandle *fileHandle, SM_PageHandle memPage)
{
  // Validated page number range
  if (pageNum < 0 || pageNum >= fileHandle->totalNumPages)
    return RC_READ_NON_EXISTING_PAGE;

  FILE *filePointer = (FILE *)fileHandle->mgmtInfo;
  long offset = pageNum * PAGE_SIZE;

  // Positioned file pointer and read data
  if (fseek(filePointer, offset, SEEK_SET) != 0 ||
      fread(memPage, sizeof(char), PAGE_SIZE, filePointer) != PAGE_SIZE)
  {
    return RC_READ_NON_EXISTING_PAGE;
  }

  fileHandle->curPagePos = pageNum;
  return RC_OK;
}

// Reported current block position
int getBlockPos(SM_FileHandle *fileHandle)
{
  return (fileHandle != NULL) ? fileHandle->curPagePos : RC_FILE_HANDLE_NOT_INIT;
}

// Retrieved first block in file
RC readFirstBlock(SM_FileHandle *fileHandle, SM_PageHandle memPage)
{
  return readBlock(0, fileHandle, memPage);
}

// Retrieved previous block relative to current position
RC readPreviousBlock(SM_FileHandle *fileHandle, SM_PageHandle memPage)
{
  int currentPos = getBlockPos(fileHandle);
  return (currentPos > 0) ? readBlock(currentPos-1, fileHandle, memPage) 
                          : RC_READ_NON_EXISTING_PAGE;
}

// Retrieved current block
RC readCurrentBlock(SM_FileHandle *fileHandle, SM_PageHandle memPage)
{
  return readBlock(getBlockPos(fileHandle), fileHandle, memPage);
}

// Retrieved next block in sequence
RC readNextBlock(SM_FileHandle *fileHandle, SM_PageHandle memPage)
{
  int currentPos = getBlockPos(fileHandle);
  return (currentPos < fileHandle->totalNumPages-1) ? readBlock(currentPos+1, fileHandle, memPage)
                                                    : RC_READ_NON_EXISTING_PAGE;
}

// Retrieved final block in file
RC readLastBlock(SM_FileHandle *fileHandle, SM_PageHandle memPage)
{
  return readBlock(fileHandle->totalNumPages-1, fileHandle, memPage);
}

/* WRITING BLOCKS TO DISK FUNCTIONS */

// Updated specified block on disk
RC writeBlock(int pageNum, SM_FileHandle *fileHandle, SM_PageHandle memPage)
{
  // Validated write conditions
  if (pageNum < 0 || pageNum >= fileHandle->totalNumPages)
    return RC_WRITE_FAILED;

  FILE *filePointer = (FILE *)fileHandle->mgmtInfo;
  long offset = pageNum * PAGE_SIZE;

  // Executed write operation
  if (fseek(filePointer, offset, SEEK_SET) != 0 ||
      fwrite(memPage, sizeof(char), PAGE_SIZE, filePointer) != PAGE_SIZE)
  {
    return RC_WRITE_FAILED;
  }

  fileHandle->curPagePos = pageNum;
  return RC_OK;
}

// Updated current block
RC writeCurrentBlock(SM_FileHandle *fileHandle, SM_PageHandle memPage)
{
  return writeBlock(getBlockPos(fileHandle), fileHandle, memPage);
}

// Added new empty block to end of file
RC appendEmptyBlock(SM_FileHandle *fileHandle)
{
  // Created zero-initialized page
  SM_PageHandle emptyPage = (char *)calloc(PAGE_SIZE, sizeof(char));
  if (!emptyPage) return RC_WRITE_FAILED;

  // Appended to file
  FILE *filePointer = (FILE *)fileHandle->mgmtInfo;
  fseek(filePointer, 0, SEEK_END);
  
  if (fwrite(emptyPage, sizeof(char), PAGE_SIZE, filePointer) != PAGE_SIZE)
  {
    free(emptyPage);
    return RC_WRITE_FAILED;
  }

  // Updated file metadata
  fileHandle->totalNumPages++;
  fileHandle->curPagePos = fileHandle->totalNumPages-1;
  free(emptyPage);
  return RC_OK;
}

// Guaranteed minimum file capacity
RC ensureCapacity(int numberOfPages, SM_FileHandle *fileHandle)
{
  // Calculated needed pages
  int pagesNeeded = numberOfPages - fileHandle->totalNumPages;
  if (pagesNeeded <= 0) return RC_OK;

  // Added required empty pages
  for (int i = 0; i < pagesNeeded; i++)
  {
    RC status = appendEmptyBlock(fileHandle);
    if (status != RC_OK) return status;
  }
  return RC_OK;
}