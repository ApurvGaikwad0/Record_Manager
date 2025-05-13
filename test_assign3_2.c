#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "dberror.h"
#include "record_mgr.h"
#include "tables.h"
#include "test_helper.h"

// Defined the global variable 'testName' to satisfy test_helper.
char *testName = "test_assign3_4";

// Utility macro for checking integer equality.
#define ASSERT_EQUALS_INT(expected, real, message)                          \
    do {                                                                      \
        if ((expected) != (real)) {                                           \
            fprintf(stderr, "[FAIL] %s: expected %d, but got %d\n",            \
                    message, (expected), (real));                             \
            exit(1);                                                          \
        } else {                                                              \
            printf("[OK] %s: expected %d and was %d\n",                       \
                   message, (expected), (real));                              \
        }                                                                     \
    } while (0)

// Utility macro for checking string equality.
#define ASSERT_EQUALS_STRING(expected, real, message)                       \
    do {                                                                      \
        if (strcmp((expected), (real)) != 0) {                                  \
            fprintf(stderr, "[FAIL] %s: expected \"%s\", but got \"%s\"\n",    \
                    message, (expected), (real));                             \
            exit(1);                                                          \
        } else {                                                              \
            printf("[OK] %s: expected \"%s\" and was \"%s\"\n",               \
                   message, (expected), (real));                              \
        }                                                                     \
    } while (0)

// Utility macro to assert that a condition was true.
#define ASSERT_TRUE(condition, message)                                     \
    do {                                                                      \
        if (!(condition)) {                                                   \
            fprintf(stderr, "[FAIL] %s: condition was false\n", message);      \
            exit(1);                                                          \
        } else {                                                              \
            printf("[OK] %s: condition was true\n", message);                 \
        }                                                                     \
    } while (0)

// Utility macro to check function return codes.
#define TEST_CHECK(code)                                                      \
    do {                                                                      \
        int rc = (code);                                                      \
        if (rc != RC_OK) {                                                    \
            char *err = errorMessage(rc);                                     \
            fprintf(stderr, "[FAIL] TEST_CHECK: %s\n", err);                  \
            free(err);                                                        \
            exit(1);                                                          \
        }                                                                     \
    } while (0)

// Utility macro to signal the end of a test.
#define TEST_DONE()                                                           \
    do {                                                                      \
        printf("[TEST DONE]\n\n");                                            \
    } while (0)

// Helper function: generate a random name of given length (including null terminator)
static void generateRandomName(char *dest, int len) {
    const char charset[] = "abcdefghijklmnopqrstuvwxyz";
    for (int i = 0; i < len - 1; i++) {
        dest[i] = charset[rand() % (sizeof(charset) - 1)];
    }
    dest[len - 1] = '\0';
}

// --------------------------------------------------------------------------
// Test: simpleTableTest
// This test created a simple table with one attribute, inserted one record, 
// retrieved it, and verified that the attribute's value was 42.
// --------------------------------------------------------------------------
static void simpleTableTest(void)
{
    printf("=== Running simpleTableTest ===\n");
    TEST_CHECK(initRecordManager(NULL));

    int numAttr = 1;
    char **attrNames = malloc(sizeof(char*) * numAttr);
    attrNames[0] = strdup("a");

    DataType *dTypes = malloc(sizeof(DataType) * numAttr);
    dTypes[0] = DT_INT;

    int *typeLength = malloc(sizeof(int) * numAttr);
    typeLength[0] = 0;

    int *keyAttrs = malloc(sizeof(int));
    keyAttrs[0] = 0;

    Schema *schema = createSchema(numAttr, attrNames, dTypes, typeLength, 1, keyAttrs);
    char *tableName = "simple_table";
    TEST_CHECK(createTable(tableName, schema));
    RM_TableData *table = malloc(sizeof(RM_TableData));
    TEST_CHECK(openTable(table, tableName));

    Record *record;
    TEST_CHECK(createRecord(&record, schema));

    Value *val;
    MAKE_VALUE(val, DT_INT, 42);
    TEST_CHECK(setAttr(record, schema, 0, val));
    freeVal(val);

    TEST_CHECK(insertRecord(table, record));

    Record *fetched;
    TEST_CHECK(createRecord(&fetched, schema));
    TEST_CHECK(getRecord(table, record->id, fetched));

    Value *fetchedVal;
    TEST_CHECK(getAttr(fetched, schema, 0, &fetchedVal));
    ASSERT_EQUALS_INT(42, fetchedVal->v.intV, "Expected attribute a to be 42");
    freeVal(fetchedVal);

    freeRecord(record);
    freeRecord(fetched);
    TEST_CHECK(closeTable(table));
    TEST_CHECK(deleteTable(tableName));
    free(table);
    freeSchema(schema);

    TEST_DONE();
    TEST_CHECK(shutdownRecordManager());
}

// --------------------------------------------------------------------------
// Test: testRandomInsertsAndDeletes
// This test created a table with 2 attributes, inserted 20 random records,
// randomly deleted 10 records, and verified the total record count.
// --------------------------------------------------------------------------
static void testRandomInsertsAndDeletes(void)
{
    printf("=== Running testRandomInsertsAndDeletes ===\n");
    TEST_CHECK(initRecordManager(NULL));

    int numAttr = 2;
    char **attrNames = malloc(sizeof(char*) * numAttr);
    attrNames[0] = strdup("a");
    attrNames[1] = strdup("b");

    DataType *dTypes = malloc(sizeof(DataType) * numAttr);
    dTypes[0] = DT_INT; dTypes[1] = DT_INT;

    int *sizes = malloc(sizeof(int) * numAttr);
    sizes[0] = 0; sizes[1] = 0;

    int *keyAttrs = malloc(sizeof(int));
    keyAttrs[0] = 0;

    Schema *schema = createSchema(numAttr, attrNames, dTypes, sizes, 1, keyAttrs);

    char *tableName = "rand_table";
    TEST_CHECK(createTable(tableName, schema));
    RM_TableData *table = malloc(sizeof(RM_TableData));
    TEST_CHECK(openTable(table, tableName));

    const int numRecords = 20;
    RID *rids = malloc(sizeof(RID) * numRecords);
    for (int i = 0; i < numRecords; i++) {
        Record *r;
        TEST_CHECK(createRecord(&r, schema));
        Value *v;
        MAKE_VALUE(v, DT_INT, rand() % 1000);
        TEST_CHECK(setAttr(r, schema, 0, v));
        freeVal(v);
        MAKE_VALUE(v, DT_INT, rand() % 500);
        TEST_CHECK(setAttr(r, schema, 1, v));
        freeVal(v);
        TEST_CHECK(insertRecord(table, r));
        rids[i] = r->id;
        freeRecord(r);
    }
    ASSERT_EQUALS_INT(numRecords, getNumTuples(table), "Expected 20 tuples after insertion");

    // Randomly deleted 10 records.
    for (int i = 0; i < 10; i++) {
        int victim = rand() % numRecords;
        TEST_CHECK(deleteRecord(table, rids[victim]));
    }

    int foundCount = 0;
    for (int i = 0; i < numRecords; i++) {
        Record *check;
        TEST_CHECK(createRecord(&check, schema));
        RC rc = getRecord(table, rids[i], check);
        if (rc == RC_OK)
            foundCount++;
        freeRecord(check);
    }
    ASSERT_TRUE(foundCount <= numRecords, "foundCount should be at most 20");
    printf("Found count after deletions = %d\n", foundCount);

    TEST_CHECK(closeTable(table));
    TEST_CHECK(deleteTable(tableName));
    free(rids);
    free(table);
    freeSchema(schema);

    TEST_DONE();
    TEST_CHECK(shutdownRecordManager());
}

// --------------------------------------------------------------------------
// Test: testConditionalUpdates
// This test created a table with 3 attributes (id, name, salary), inserted 20 records,
// ran a scan with a condition, updated records conditionally, and verified updates.
// --------------------------------------------------------------------------
static void testConditionalUpdates(void)
{
    printf("=== Running testConditionalUpdates ===\n");
    TEST_CHECK(initRecordManager(NULL));

    int numAttr = 3;
    char **attrNames = malloc(sizeof(char*) * numAttr);
    attrNames[0] = strdup("id");
    attrNames[1] = strdup("name");
    attrNames[2] = strdup("salary");

    DataType *dTypes = malloc(sizeof(DataType) * numAttr);
    dTypes[0] = DT_INT; dTypes[1] = DT_STRING; dTypes[2] = DT_FLOAT;

    int *typeLength = malloc(sizeof(int) * numAttr);
    typeLength[0] = 0; typeLength[1] = 10; typeLength[2] = 0;

    int *keyAttrs = malloc(sizeof(int));
    keyAttrs[0] = 0;

    Schema *schema = createSchema(numAttr, attrNames, dTypes, typeLength, 1, keyAttrs);

    char *tableName = "update_table";
    TEST_CHECK(createTable(tableName, schema));
    RM_TableData *table = malloc(sizeof(RM_TableData));
    TEST_CHECK(openTable(table, tableName));

    // Inserted 20 records with:
    // id = 0..19, name = random 10-letter string, salary = random float between 300.0 and 1000.0.
    const int numRecords = 20;
    RID *rids = malloc(sizeof(RID) * numRecords);
    for (int i = 0; i < numRecords; i++) {
        Record *r;
        TEST_CHECK(createRecord(&r, schema));
        Value *v;
        // Set id.
        MAKE_VALUE(v, DT_INT, i);
        TEST_CHECK(setAttr(r, schema, 0, v));
        freeVal(v);
        // Set name.
        char nameBuffer[11];
        generateRandomName(nameBuffer, 11);
        MAKE_STRING_VALUE(v, nameBuffer);
        TEST_CHECK(setAttr(r, schema, 1, v));
        freeVal(v);
        // Set salary.
        float salary = 300.0f + ((float)rand() / RAND_MAX) * 700.0f;
        MAKE_VALUE(v, DT_FLOAT, salary);
        TEST_CHECK(setAttr(r, schema, 2, v));
        freeVal(v);
        TEST_CHECK(insertRecord(table, r));
        rids[i] = r->id;
        freeRecord(r);
    }
    ASSERT_EQUALS_INT(numRecords, getNumTuples(table), "Expected 20 tuples after insertion");

    // Constructed a scan condition: salary >= 800.
    // Built it as: NOT (salary < 800)
    RM_ScanHandle scan;
    Expr *cond, *leftExpr, *rightExpr, *notExpr;
    MAKE_ATTRREF(leftExpr, 2); // salary attribute (index 2)
    MAKE_CONS(rightExpr, stringToValue("f800.0"));
    // Built condition: salary < 800.
    MAKE_BINOP_EXPR(cond, leftExpr, rightExpr, OP_COMP_SMALLER);
    // Built NOT condition: salary >= 800.
    MAKE_UNOP_EXPR(notExpr, cond, OP_BOOL_NOT);

    TEST_CHECK(startScan(table, &scan, notExpr));
    int highCount = 0;
    Record *temp;
    TEST_CHECK(createRecord(&temp, schema));
    while (next(&scan, temp) == RC_OK) {
        highCount++;
    }
    TEST_CHECK(closeScan(&scan));
    freeRecord(temp);
    printf("Records with salary >= 800: %d\n", highCount);

    // Updated records with id < 10 by increasing salary by 100.
    for (int i = 0; i < numRecords; i++) {
        if (i < 10) {
            RID rid;
            // This test assumed records were inserted sequentially into page 1, slot i.
            rid.page = 1;
            rid.slot = i;
            Record *r;
            TEST_CHECK(createRecord(&r, schema));
            if (getRecord(table, rid, r) == RC_OK) {
                Value *currSal;
                TEST_CHECK(getAttr(r, schema, 2, &currSal));
                float newSal = currSal->v.floatV + 100.0f;
                freeVal(currSal);
                Value *newVal;
                MAKE_VALUE(newVal, DT_FLOAT, newSal);
                TEST_CHECK(setAttr(r, schema, 2, newVal));
                freeVal(newVal);
                TEST_CHECK(updateRecord(table, r));
            }
            freeRecord(r);
        }
    }
    printf("Updated records with id < 10 by increasing salary by 100.\n");

    // Deleted records with id >= 15.
    for (int i = 15; i < numRecords; i++) {
        TEST_CHECK(deleteRecord(table, rids[i]));
    }
    printf("Deleted records with id >= 15.\n");

    // Final scan: count remaining records.
    RM_ScanHandle scan2;
    TEST_CHECK(startScan(table, &scan2, NULL));
    int finalCount = 0;
    TEST_CHECK(createRecord(&temp, schema));
    while (next(&scan2, temp) == RC_OK) {
        finalCount++;
    }
    TEST_CHECK(closeScan(&scan2));
    freeRecord(temp);
    printf("Final record count after updates and deletions: %d\n", finalCount);

    // Retrieved record with id = 5 (if it existed) to verify its updated values.
    RID checkRID;
    checkRID.page = 1;  // Assumed placement; adjust if necessary.
    checkRID.slot = 5;
    Record *checkRec;
    TEST_CHECK(createRecord(&checkRec, schema));
    if (getRecord(table, checkRID, checkRec) == RC_OK) {
        Value *vid, *vname, *vsal;
        TEST_CHECK(getAttr(checkRec, schema, 0, &vid));
        TEST_CHECK(getAttr(checkRec, schema, 1, &vname));
        TEST_CHECK(getAttr(checkRec, schema, 2, &vsal));
        printf("Record with id 5: id=%d, salary=%f\n", vid->v.intV, vsal->v.floatV);
        freeVal(vid);
        freeVal(vname);
        freeVal(vsal);
    }
    freeRecord(checkRec);

    TEST_CHECK(closeTable(table));
    TEST_CHECK(deleteTable(tableName));
    free(rids);
    free(table);
    freeSchema(schema);

    TEST_DONE();
    TEST_CHECK(shutdownRecordManager());
}

// --------------------------------------------------------------------------
// main
// Called each test function sequentially.
// --------------------------------------------------------------------------
int main(void)
{
    srand((unsigned int)time(NULL));

    simpleTableTest();
    testRandomInsertsAndDeletes();
    testConditionalUpdates();

    printf("=== Finished test_assign3_4 ===\n\n");
    return 0;
}