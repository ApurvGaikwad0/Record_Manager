# Record Manager Module Description

The Record Manager is responsible for **managing tables with fixed schemas, supporting operations such as record insertion, deletion, update, and scanning**. It builds upon the previously implemented Storage Manager and Buffer Manager modules and includes an innovative tombstone mechanism to track deleted records without wasting disk space.

## Programming Language Used: C

## System Architecture and Design

## Overview

### The Record Manager is structured into several interdependent modules

- **Storage Manager**: Handles low-level file operations (creation, reading, writing, and deletion of page files).
- **Buffer Manager**: Implements in-memory page buffering with support for page replacement strategies (FIFO, LRU, CLOCK) to optimize disk I/O.
- **Record Manager**: Provides higher-level abstractions to manage tables and records. It manages metadata storage, record serialization, and memory management, and integrates with the underlying Storage and Buffer Managers.

## Data Structure and Page Layout

- **_Table Metadata_**: Table information (including the number of tuples, schema details, and free-space management) is stored in the first page (page 0) of the file. This includes the attribute definitions and the “slots” usage bitmap.
- **_Record Storage_**: Each record is stored in fixed-length slots. The record layout is computed based on the schema; for example, an integer uses sizeof(int), while strings occupy a fixed number of bytes.
- **_Tombstone Mechanism_**: Deleted records are marked using a tombstone (a designated marker) to indicate that the slot is available for reuse. This optimizes space management without immediately shifting data.

## Implementation Details

## Table and Record Management:

- **_Initialization and Metadata Storage_**:
  The Record Manager initializes by invoking the Storage Manager to set up file access. When a new table is created, metadata (including the number of tuples, the pointer to the next free page, and schema details) is written to the first page (page 0).

- **_Record Storage and Slot Management_**:
  Records are inserted into data pages where a small portion of each page is reserved for a header (tracking the number of occupied slots and a bitmap for slot usage). The helper function computeMaxSlots determines the maximum number of records per page based on the record size.

## Buffer Management Integration:

- **Buffer Pool Usage**:
  The module leverages the Buffer Manager to pin pages in memory during operations. Modified pages are marked as “dirty” to ensure they are flushed to disk when necessary.

- **Page Replacement Strategies**:
  The Buffer Manager supports various strategies (FIFO, LRU, CLOCK). Each page frame tracks a fix count and a usage counter to help manage page replacement efficiently.

## Steps to run the Assignment:

This section will explain how t build and run the Record Manager and execute the test cases.

1. **Navigate to the project directory**: Use the terminal to go to the location where the `record_mgr.c` file is stored.

2. Cleaning the older artifacts:
   You need to execute the following command in the terminal.
   ```
   make clean
   ```
3. To compile the code and produce executable files from the test case you need to run the follwoing command.
   ```
   make
   ```
   This command will run the Makefile.
4. Run the test cases.
   Three executable files will be generated
   ```
   test1.exe
   test2.exe
   test3.exe
   ```
   Execute the following commands to run the test cases

```
    ./test1.exe
    ./test2.exe
    ./test3.exe
```

## 1. Record Manager Functions

This section outlines the primary functions responsible for managing tables and records. These functions facilitate tasks such as table creation, opening, closing, deletion, and record operations.

- **`initRecordManager(...)`**:

  - Sets up the Record Manager.
  - Invokes `initStorageManager(...)` to prepare the Storage Manager and initialize the internal structures necessary for managing records.

- **`createTable(...)`**:

  - Establishes a new table by specifying its name, attributes (name, datatype, size), and schema.
  - Configures the Buffer Pool using an **LRU (Least Recently Used)** page replacement policy for efficient in-memory page management.
  - Writes the schema and additional metadata to the page file.

- **`closeTable(...)`**:

  - Closes the active table and ensures that all changes are written back to disk.
  - Shuts down the Buffer Pool, releasing any resources associated with the table.

- **`shutdownRecordManager(...)`**:

  - Terminates the Record Manager by releasing all allocated resources.
  - Frees all memory and resets internal pointers to prevent further usage.

- **`openTable(...)`**:

  - Opens an existing table using the provided name and schema.
  - Prepares the table for operations such as record insertion and querying.

- **`deleteTable(...)`**:

  - Removes the specified table by deleting its underlying page file from disk.
  - Utilizes the `destroyPageFile(...)` function from the Storage Manager to perform the deletion.

- **`getNumTuples(...)`**:

  - Retrieves the total number of tuples (records) currently stored in the table.
  - This count is maintained in a dedicated metadata structure.

  ### 2. Record Functions

These functions enable efficient management of records within a table by handling insertion, retrieval, updates, and deletions.

- **`getRecord(...)`**:  
  Retrieves a record based on its Record ID (RID). This function pins the associated page in memory, reads the record's data, and copies it into the provided record structure.

- **`insertRecord(...)`**:  
  Adds a new record to a table by assigning a unique Record ID, pinning the relevant memory page, marking the page as modified (dirty), and placing the record data into the appropriate slot.

- **`updateRecord(...)`**:  
  Modifies an existing record by pinning the corresponding page, updating its data, and marking the page as dirty to ensure changes are written back to disk.

- **`deleteRecord(...)`**:  
  Removes a record by marking its slot as free. Instead of completely erasing the data, a tombstone marker (typically a `'-'` character) is inserted to signal deletion, allowing the space to be reused later.

---

### 3. Scan Functions

These functions support scanning through a table to retrieve records that meet specific criteria.

- **`next(...)`**:  
  Fetches the next record matching the scan condition. It pins the current page, evaluates the condition for each record, and returns the matching record. If no more records satisfy the condition, it returns `RC_RM_NO_MORE_TUPLES`.

- **`startScan(...)`**:  
  Begins a scan on a table by initializing the scan handle with the desired condition. If a condition is expected but not provided, the function returns an error (`RC_SCAN_CONDITION_NOT_FOUND`).

- **`closeScan(...)`**:  
  Terminates an active scan, releasing resources and resetting any scan-related state.

---

### 4. Schema Functions

These functions manage the structure (schema) of a table.

- **`getRecordSize(...)`**:  
  Calculates and returns the total size (in bytes) of a record according to the schema by summing the sizes of all attributes.

- **`createSchema(...)`**:  
  Builds a new schema in memory using specified attribute names, data types, and sizes.

- **`freeSchema(...)`**:  
  Deallocates the memory used by a schema, effectively removing it from memory.

---

### 5. Attribute Functions

These functions handle operations related to individual record attributes.

- **`createRecord(...)`**:  
  Allocates and initializes a new record based on a given schema, preparing it to store attribute data.

- **`setAttr(...)`**:  
  Assigns a new value to a specific attribute within a record.

- **`getAttr(...)`**:  
  Retrieves the current value of a designated attribute from a record.

- **`attrOffset(...)`**:  
  Computes and sets the byte offset for a particular attribute in a record, based on the record's layout.

- **`freeRecord(...)`**:  
  Frees the memory allocated for a record, ensuring that resources are properly released.

## Extra Test Cases Implementation

In addition to the standard tests, we have implemented an extra test suite to thoroughly validate the Record Manager's functionality. This suite includes the following tests:

- **simpleTableTest**:

  - Creates a table with a single integer attribute.
  - Inserts a record with the value `42`.
  - Retrieves the record and verifies that the stored value is `42`.

- **testRandomInsertsAndDeletes**:

  - Creates a table with two integer attributes.
  - Inserts 20 records with randomly generated values.
  - Randomly deletes 10 records.
  - Confirms that the total number of records matches the expected count after deletions.

- **testConditionalUpdates**:
  - Creates a table with three attributes (`id`, `name`, `salary`).
  - Inserts 20 records with random values.
  - Performs a conditional scan to select records with a `salary` of at least `800`.
  - Updates records with `id < 10` by increasing their `salary` by `100`.
  - Deletes records with `id ≥ 15`.
  - Validates that the remaining record count and updated values are correct.

## File Structure

- **test_assign3_1.c**: Contains the test cases for the Record Manager functions.
- **test_expr.c**: Contains test cases for the expression evaluation part of the Record Manager.

## Project Structure

The project directory is organized as follows:

```bash
assign2/

├── buffer_mgr.c
├── buffer_mgr.h
├── buffer_mgr_stat.c
├── buffer_mgr_stat.h
├── Contribution Table-3 G04.docx
├── dberror.c
├── dberror.h
├── dt.h
├── expr.c
├── expr.h
├── makefile
├── README.md
├── record_mgr.c
├── record_mgr.h
├── rm_serializer.c
├── storage_mgr.c
├── storage_mgr.h
├── tables.h
├── test_assign3_1.c
├── test_expr.c
├── test_expr.exe
├── test_helper.h
```

---

### Authors:


- ### Apurv Gaikwad (A20569178)
- ### Nishant Dalvi (A20556507)
- ### Satyam Borade (A20586631)
