/* Unity - Lightweight C Unit Testing Framework
 * Copyright (c) 2007-2023 Mike Karlesky, Mark VanderVoord, Greg Williams
 * SPDX-License-Identifier: MIT
 *
 * Minimal self-contained version for iotspool test suite.
 */
#ifndef UNITY_H
#define UNITY_H

#include <stdint.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>

/* ---- types ------------------------------------------------------------ */
typedef struct {
    const char *file;
    uint32_t    line;
    uint32_t    tests;
    uint32_t    failures;
    uint32_t    ignored;
    uint8_t     current_result; /* 0=pass, 1=fail, 2=ignore */
    char        msg[256];
} UnityFixture;

extern UnityFixture Unity;

/* ---- internal --------------------------------------------------------- */
void UnityBegin(const char *file);
int  UnityEnd(void);
void UnityTestResultsBegin(const char *file, uint32_t line);
void UnityTestResultsFailBegin(uint32_t line);
void UnityConcludeTest(void);
void UnityAddMsgIfSpecified(const char *msg);
void UnityPrintExpectedAndActualStrings(const char *expected, const char *actual);

void unity_fail_int   (int64_t  expected, int64_t  actual, uint32_t line, const char *msg);
void unity_fail_uint  (uint64_t expected, uint64_t actual, uint32_t line, const char *msg);
void unity_fail_str   (const char *exp,   const char *act, uint32_t line, const char *msg);
void unity_fail_ptr   (const void *exp,   const void *act, uint32_t line, const char *msg);
void unity_fail_msg   (uint32_t line, const char *msg);

/* ---- macros ----------------------------------------------------------- */
#define TEST_ASSERT_TRUE(cond) \
    do { if (!(cond)) { unity_fail_msg(__LINE__, #cond " is not TRUE"); } } while(0)

#define TEST_ASSERT_FALSE(cond) \
    do { if ((cond)) { unity_fail_msg(__LINE__, #cond " is not FALSE"); } } while(0)

#define TEST_ASSERT_EQUAL_INT(e, a) \
    do { if ((int64_t)(e) != (int64_t)(a)) unity_fail_int((int64_t)(e),(int64_t)(a),__LINE__,NULL); } while(0)

#define TEST_ASSERT_EQUAL_UINT32(e, a) \
    do { if ((uint64_t)(e) != (uint64_t)(a)) unity_fail_uint((uint64_t)(e),(uint64_t)(a),__LINE__,NULL); } while(0)

#define TEST_ASSERT_EQUAL_STRING(e, a) \
    do { unity_fail_str((e),(a),__LINE__,NULL); } while(0)

#define TEST_ASSERT_NOT_NULL(p) \
    do { if ((p)==NULL) unity_fail_msg(__LINE__, #p " is NULL"); } while(0)

#define TEST_ASSERT_NULL(p) \
    do { if ((p)!=NULL) unity_fail_msg(__LINE__, #p " is not NULL"); } while(0)

#define TEST_ASSERT_EQUAL(e, a) \
    do { if ((intptr_t)(e) != (intptr_t)(a)) unity_fail_int((int64_t)(e),(int64_t)(a),__LINE__,NULL); } while(0)

#define TEST_ASSERT_NOT_EQUAL(e, a) \
    do { if ((intptr_t)(e) == (intptr_t)(a)) unity_fail_msg(__LINE__, "Values are equal"); } while(0)

#define TEST_ASSERT_MEMORY_EQUAL(e, a, n) \
    do { if (memcmp((e),(a),(n))!=0) unity_fail_msg(__LINE__, "Memory mismatch: " #e " vs " #a); } while(0)

#define RUN_TEST(fn) \
    do { \
        Unity.current_result = 0; \
        Unity.tests++; \
        printf("  %-55s ", #fn); \
        fn(); \
        if (Unity.current_result == 0) puts("PASS"); \
        else Unity.failures++; \
    } while(0)

#define UNITY_BEGIN()  UnityBegin(__FILE__)
#define UNITY_END()    UnityEnd()

#endif /* UNITY_H */
