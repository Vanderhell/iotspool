/* unity.c - Unity test framework implementation
 * SPDX-License-Identifier: MIT
 */
#include "unity.h"
#include <stdio.h>
#include <stdarg.h>
#include <setjmp.h>

UnityFixture Unity;

static jmp_buf unity_jmp;

void UnityBegin(const char *file)
{
    memset(&Unity, 0, sizeof(Unity));
    Unity.file = file;
    printf("\n=== %s ===\n", file);
}

int UnityEnd(void)
{
    printf("\n%u Tests  %u Failures  %u Ignored\n",
           Unity.tests, Unity.failures, Unity.ignored);
    return (int)Unity.failures;
}

void unity_fail_msg(uint32_t line, const char *msg)
{
    Unity.current_result = 1;
    printf("FAIL\n    %s:%u: %s\n", Unity.file ? Unity.file : "?", line, msg ? msg : "");
}

void unity_fail_int(int64_t expected, int64_t actual, uint32_t line, const char *msg)
{
    Unity.current_result = 1;
    printf("FAIL\n    %s:%u: Expected %lld Was %lld%s%s\n",
           Unity.file ? Unity.file : "?", line,
           (long long)expected, (long long)actual,
           msg ? " : " : "", msg ? msg : "");
}

void unity_fail_uint(uint64_t expected, uint64_t actual, uint32_t line, const char *msg)
{
    Unity.current_result = 1;
    printf("FAIL\n    %s:%u: Expected 0x%llx Was 0x%llx%s%s\n",
           Unity.file ? Unity.file : "?", line,
           (unsigned long long)expected, (unsigned long long)actual,
           msg ? " : " : "", msg ? msg : "");
}

void unity_fail_str(const char *expected, const char *actual, uint32_t line, const char *msg)
{
    if (expected == actual) return;
    if (expected && actual && strcmp(expected, actual) == 0) return;
    Unity.current_result = 1;
    printf("FAIL\n    %s:%u: Expected \"%s\" Was \"%s\"%s%s\n",
           Unity.file ? Unity.file : "?", line,
           expected ? expected : "(null)", actual ? actual : "(null)",
           msg ? " : " : "", msg ? msg : "");
}

void unity_fail_ptr(const void *expected, const void *actual, uint32_t line, const char *msg)
{
    if (expected == actual) return;
    Unity.current_result = 1;
    printf("FAIL\n    %s:%u: Pointer mismatch%s%s\n",
           Unity.file ? Unity.file : "?", line,
           msg ? " : " : "", msg ? msg : "");
}
