/*

THIS VERSION HAS VARIABLE CHUNK SIZES IN MEMORY (CALLED PORTIONS) AND UTILIZES ORIGAMI SORT AND ORIGAMI MERGE SORT WITH 1 BUCKET REFILL

Notes:
*** OLD
- Max file size that merge_sort can currently handle is (chunk_size)^2
    - This is since mergesort_buffer_size = chunk_size, and num_vals_per_chunk is defined as mergesort_buffer_size / num chunks.
      If num_chunks > mergesort_buffer_size, num_vals_per_chunk < 1, which doesn't make sense. Need at least 1 val per chunk since can't store partial ints.
        - mergesort_bs must be >= num_chunks
***
What is the new max file size that this can currently handle?


- Chunk size must be a multiple of 512
- The largest chunk size doesn't increase as chunks start using all their values
    - chunks themselves are still the same size, though. On average will this matter since they'll all empty at around the same time? Only think this is the case by intuition, not supported by math


Known issues:
- Origami is an out-of-sort algorithm, so only half the available ram used can be used for chunk size



*/

#include <stdio.h>      
#include <assert.h>
#include <stdlib.h>  
#include <algorithm>
#include <time.h>
#include <queue>
#include <algorithm>
#include <limits>
#include <unordered_set>
#include <iostream>
#include <sstream>
#include <string>
#include <fstream>
#include <istream>
#include <windows.h>
#include <cmath>
#include "external_sort.h"
#include "external_sorter.h"
#include "MinHeap.h"

#include "commons.h"
#include "writer.h"
#include "utils.h"
#include "merge_utils.h"
#include "sorter.h"

using namespace std;


#define CREATE_VAR1(X,Y) X##Y 
#define CREATE_VAR(X,Y) CREATE_VAR1(X,Y)

#define MAKE_KEY(k)                             \
    next##k = a * next##k + c;                  \
    wbuffer[i + k] = next##k;                   \

//#define DEBUG_PRINT

struct my_lesser {
    bool operator()(const MinHeapNode& x, const MinHeapNode& y) const {
        return x.val > y.val;
    }
};


external_sort::external_sort(unsigned long long int _FILE_SIZE, unsigned long long int _MEM_SIZE, char _fname[], char _chunk_sorted_fname[], char _full_sorted_fname[], char _metrics_fname[], int _num_runs, bool _TEST_SORT, bool _GIVE_VALS, bool _DEBUG)
    : file_size{ _FILE_SIZE }, fname{ _fname }, chunk_sorted_fname{ _chunk_sorted_fname }, full_sorted_fname{ _full_sorted_fname }, test_sort{ _TEST_SORT }, give_vals{ _GIVE_VALS }, debug{ _DEBUG }, num_runs{ _num_runs }, metrics_fname{ _metrics_fname }
{
    this->windows_fs = { 0 };
    this->windows_fs.QuadPart = sizeof(Itemtype) * _FILE_SIZE;
    this->write_buffer_size = (1LLU << 20) / sizeof(Itemtype);
    this->block_size = 1LLU << 20;

    this->total_time = 0;
    this->total_generate_time = 0;
    this->total_write_time = 0;
    this->total_sort_time = 0;
    this->total_read_time = 0;
    this->total_merge_time = 0;
    this->total_load_time = 0;
    this->total_merge_read_time = 0;
    this->total_heap_time = 0;
    this->total_merge_write_time = 0;
    this->num_seeks = 0;
    this->merge_write_duration = 0;
    this->merge_read_duration = 0;
    this->merge_populate_duration = 0;
    this->merge_duration = 0;

    MEMORYSTATUSEX statex = { 0 };
    statex.dwLength = sizeof(statex);
    GlobalMemoryStatusEx(&statex);

    printf("    There is currently %llu KB of free memory available\n", statex.ullAvailPhys / 1024);
    unsigned long long mem_avail = 0.7 * statex.ullAvailPhys;
    // divided by 2 since Origami is an out-of-place sorter
    mem_avail = ((mem_avail + 511) & (~511));
    //mem_avail = 1LLU << (unsigned)log2(mem_avail);
    // the following used for origami sort benchmark
    mem_avail = (std::min)(1LLU << (unsigned)log2(mem_avail), sizeof(Itemtype) * _FILE_SIZE / 2);
    //mem_avail = 1LLU << 30

    this->merge_mem_avail = mem_avail;
    this->mem_avail = mem_avail / 2;
    printf("        External sort will use %llu B (%llu vals) of memory\n", merge_mem_avail, merge_mem_avail / sizeof(Itemtype));
    assert(merge_mem_avail % sizeof(Itemtype) == 0);
    this->chunk_size = mem_avail / (2 * sizeof(Itemtype));

    BOOL succeeded = GetDiskFreeSpaceA(NULL, NULL, &this->bytes_per_sector, NULL, NULL);
    if (!succeeded) {
        printf("%s: Failed getting disk information with %d\n", __FUNCTION__, GetLastError());
    }
    if (this->fname == this->chunk_sorted_fname) {
        this->seq_run = true;
    }
}

int external_sort::write_file()
{
    printf("\n%s\n", __FUNCTION__);
    printf("    Written: 0 B (0 MB)");
    srand((unsigned int)time(0));
    //srand(0);
    LARGE_INTEGER start = { 0 }, end = { 0 }, freq = { 0 };

    QueryPerformanceFrequency(&freq);
    QueryPerformanceCounter(&start);

    unsigned long long int number_written = 0;

    Itemtype* wbuffer = (Itemtype*)_aligned_malloc(static_cast<size_t>(this->write_buffer_size) * sizeof(Itemtype), this->bytes_per_sector);
    double generation_duration = 0, write_duration = 0;

    HANDLE pfile = CreateFile(this->fname, GENERIC_READ | GENERIC_WRITE,
        FILE_SHARE_READ | FILE_SHARE_DELETE | FILE_SHARE_WRITE,
        NULL,
        OPEN_EXISTING,
        FILE_ATTRIBUTE_NORMAL | FILE_FLAG_NO_BUFFERING,
        NULL);


    if (pfile == INVALID_HANDLE_VALUE) {
        printf("__FUNCTION__write_file(): Failed opening file with %d\n", GetLastError());
        exit(1);
    }
    Itemtype CREATE_VAR(next, 0) = rand();
    Itemtype CREATE_VAR(next, 1) = rand();
    Itemtype CREATE_VAR(next, 2) = rand();
    Itemtype CREATE_VAR(next, 3) = rand();
    Itemtype CREATE_VAR(next, 4) = rand();
    Itemtype CREATE_VAR(next, 5) = rand();
    Itemtype CREATE_VAR(next, 6) = rand();
    Itemtype CREATE_VAR(next, 7) = rand();

    const unsigned int a = 214013;
    // const unsigned int m = 4096*4;
    const unsigned int c = 2531011;
    while (number_written < this->file_size) {
        // populate buffer - utilizes the Linear Congruential Generator method
        // https://en.wikipedia.org/wiki/Linear_congruential_generator

        unsigned long num_vals_to_gen = 0;
        if (number_written + this->write_buffer_size > this->file_size) {
            if (number_written) {
                num_vals_to_gen = this->file_size % number_written;
            }
            else {
                num_vals_to_gen = this->file_size;
            }
        }
        else {
            num_vals_to_gen = this->write_buffer_size;
        }
        if (this->debug) {
            printf("  num_vals_to_gen = %lu\n", num_vals_to_gen);
        }
        QueryPerformanceCounter(&start);
        for (unsigned int i = 0; i < num_vals_to_gen; i += 8) {
            MAKE_KEY(0);
            MAKE_KEY(1);
            MAKE_KEY(2);
            MAKE_KEY(3);
            MAKE_KEY(4);
            MAKE_KEY(5);
            MAKE_KEY(6);
            MAKE_KEY(7);
        }
        QueryPerformanceCounter(&end);
        generation_duration += end.QuadPart - start.QuadPart;

        if (this->give_vals) {
            for (unsigned int i = 0; i < 2 && i < num_vals_to_gen; i++) {
                printf("buffer[%d] = %llu\n", i, wbuffer[i]);
            }
            /*for (unsigned int i = num_vals_to_gen - 2; i < num_vals_to_gen; i++) {
                printf("buffer[%d] = %llu\n", i, wbuffer[i]);
            }*/
        }

        unsigned long num_vals_to_write = 0;
        unsigned long new_num_vals_to_write = 0;
        if (number_written + this->write_buffer_size > this->file_size) {
            if (number_written) {
                num_vals_to_write = this->file_size % number_written;
            }
            else {
                num_vals_to_write = this->file_size;
            }
            new_num_vals_to_write = (num_vals_to_write + 127) & (~127);
        }
        else {
            num_vals_to_write = this->write_buffer_size;
            new_num_vals_to_write = this->write_buffer_size;
        }
        if (this->debug) {
            printf("    num_vals_to_write = %lu\n", num_vals_to_write);
            printf("    new_num_vals_to_write = %lu\n", new_num_vals_to_write);
        }
        DWORD num_bytes_written;
        hrc::time_point s, e;
        s = hrc::now();
        BOOL was_success = WriteFile(pfile, wbuffer, sizeof(Itemtype) * new_num_vals_to_write, &num_bytes_written, NULL);
        e = hrc::now();
        double el = ELAPSED_MS(s, e);
        //QueryPerformanceCounter(&start);
        //QueryPerformanceCounter(&end);
        if (!(was_success)) {
            printf("%s: Failed writing to file with %d\n", __FUNCTION__, GetLastError());
            exit(1);
        }

        //write_duration += end.QuadPart - start.QuadPart;
        write_duration += (el / 1000);
        number_written += num_vals_to_write;

        if ((number_written * sizeof(Itemtype)) % GB(1LLU) == 0) {
            printf("                                                                          \r");
            printf("    Written: %llu B (%llu MB)", number_written * sizeof(Itemtype), number_written * sizeof(Itemtype) / (1 << 20));
            //printf("      tot_bytes_read  = %llu B = %llu MB\n", tot_bytes_read, tot_bytes_read / (1LLU << 20));
        }
        if (this->debug) {
            printf("    number_written = %llu\n", number_written);
            printf("    bufsize = %d\n", this->write_buffer_size);
            printf("    windows_fs.QuadPart = %llu\n", this->windows_fs.QuadPart);
            printf("    windows_fs.HighPart = %llu\n", this->windows_fs.HighPart);
            printf("    windows_fs.LowPart = %llu\n", this->windows_fs.LowPart);
        }

    }
    CloseHandle(pfile);
    if (number_written % 512 != 0) {
        LARGE_INTEGER before_sfp = { 0 };
        before_sfp.QuadPart = this->windows_fs.QuadPart;
        pfile = CreateFile(this->fname, GENERIC_WRITE, 0, 0, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);
        if ((SetFilePointer(pfile, this->windows_fs.LowPart, &this->windows_fs.HighPart, FILE_BEGIN)) == INVALID_SET_FILE_POINTER) {
            printf("%s: Error with SetFilePointer with %d\n", __FUNCTION__, GetLastError());
            exit(1);
        }
        /*if (!SetEndOfFile(pfile)) {
            printf("%s: Error with SetEndOfFile with %d\n", __FUNCTION__, GetLastError());
            exit(1);
        }*/
        CloseHandle(pfile);
        this->windows_fs.QuadPart = before_sfp.QuadPart;
    }

    _aligned_free(wbuffer);
    wbuffer = nullptr;

    this->number_elements_touched = number_written;
    this->generation_duration = generation_duration / freq.QuadPart;
    printf("\n");
    printf("    Generation done in %.2f ms, speed %.2f M keys/s\n", this->generation_duration * 1e3, this->file_size * 1.0 / 1e6 / this->generation_duration);
    //printf("\nDone in %.2f ms, Speed: %.2f M keys/s\n", el, this->file_size * 1.0 / el / 1e3);

    this->write_duration = write_duration;// / freq.QuadPart;
    printf("    Write done in %.2f ms, speed %.2f M keys/s\n", this->write_duration * 1e3, this->file_size * 1.0 / 1e6 / this->write_duration);

    return 0;
}


double glb_populate_duration, glb_read_duration, glb_write_duration, glb_merge_duration;


unsigned long read_into_buffer(HANDLE* fp, Itemtype* buf, unsigned long long tot_bytes)
{
    HANDLE f = *fp;
    DWORD num_bytes_touched;
    //unsigned long long tot_bytes = sizeof(Itemtype) * new_num_vals_to_read;
    unsigned long long num_loops = 0;
    // - 511 since it needs to be divisible by 512
    //unsigned num_to_read = UINT_MAX - 511;
    // want to read only 1 MB at a time
    unsigned num_to_read = 1LLU << 20;
    while (tot_bytes > num_to_read)
    {
        bool was_success = ReadFile(f, buf + (num_loops * num_to_read / sizeof(Itemtype)), num_to_read, &num_bytes_touched, NULL);
        if (!(was_success)) {
            printf("%s: Failed reading from populated file with %d\n", __FUNCTION__, GetLastError());
            exit(1);
        }
        tot_bytes -= num_to_read;
        num_loops++;
    }
    bool was_success = ReadFile(f, buf + (num_loops * num_to_read / sizeof(Itemtype)), tot_bytes, &num_bytes_touched, NULL);
    if (!(was_success)) {
        printf("%s: Failed reading from populated file with %d\n", __FUNCTION__, GetLastError());
        exit(1);
    }
    return num_bytes_touched;
}


int num_chunks;
int orig_num_chunks;
HANDLE fp[MTREE_MAX_WAY + 1];
ui64 bytes_left[MTREE_MAX_WAY + 1];
bool str_inits[MTREE_MAX_WAY + 1];
char* X[MTREE_MAX_WAY + 1], * endX[MTREE_MAX_WAY + 1];
//ui64 in_buf_size, out_buf_size;
ui64 nseeks;
ui64 tot_bytes_written;
ui64 largest_chunk;
ui64 delta;
unsigned long long num_refills;
ui64 tot_bytes_read;
std::vector<state_vars> state;
std::queue<Itemtype*> free_blocks;
bool two_real_chunks = false;
ui64 mem_block_size;
unsigned int glb_bytes_per_sector;
ui64 chunk_size;
ui64 file_size;
Itemtype glb_last_val;
bool seek_to_write;


int external_sort::sort_file()
{
#define SORT_CHECK
    printf("\n%s\n", __FUNCTION__);
    printf("    Written: 0 B (0 MB)");

    LARGE_INTEGER start = { 0 }, end = { 0 }, freq = { 0 }, num_bytes_written = { 0 };

    unsigned long long oos_size = this->chunk_size;

    unsigned long long int written = 0, number_read = 0;
    double sort_duration = 0, read_duration = 0, sort_write_duration = 0;

    QueryPerformanceFrequency(&freq);
    HANDLE old_file = nullptr, chunk_sorted_file = nullptr;

    if (this->seq_run)
    {
        old_file = CreateFile(this->fname, GENERIC_READ | GENERIC_WRITE,
            FILE_SHARE_READ | FILE_SHARE_DELETE | FILE_SHARE_WRITE,
            NULL,
            OPEN_EXISTING,
            FILE_ATTRIBUTE_NORMAL | FILE_FLAG_NO_BUFFERING,
            NULL);
        chunk_sorted_file = CreateFile(this->chunk_sorted_fname, GENERIC_READ | GENERIC_WRITE,
            FILE_SHARE_READ | FILE_SHARE_DELETE | FILE_SHARE_WRITE,
            NULL,
            OPEN_EXISTING,
            FILE_ATTRIBUTE_NORMAL | FILE_FLAG_NO_BUFFERING,
            NULL);
        if (old_file == INVALID_HANDLE_VALUE) {
            printf("%s: Failed opening populated file with %d\n", __FUNCTION__, GetLastError());
            exit(1);
        }
        else if (chunk_sorted_file == INVALID_HANDLE_VALUE) {
            printf("%s: Failed opening new file for sort output with %d\n", __FUNCTION__, GetLastError());
            exit(1);
        }
        DWORD num_moved = 0;
        num_moved = SetFilePointer(chunk_sorted_file, this->windows_fs.LowPart, &this->windows_fs.HighPart, FILE_BEGIN);
        if (num_moved == INVALID_SET_FILE_POINTER) {
            printf("%s: Line ~321 error in SetFilePointer with %d\n", __FUNCTION__, GetLastError());
            exit(1);
        }
    }
    else {
        printf("Need to fix this\n");
        exit(1);
    }

    num_chunks = (this->file_size % this->chunk_size == 0) ? (this->file_size / this->chunk_size) : ((this->file_size / this->chunk_size) + 1);
#ifdef DEBUG_PRINT
    printf("    num_chunks = %llu\n", num_chunks);
#endif
    Itemtype* sort_buffer = (Itemtype*)_aligned_malloc(static_cast<size_t>(oos_size) * sizeof(Itemtype), this->bytes_per_sector);
    while (number_read < this->file_size)
    {
        //#ifdef DEBUG_PRINT
        //            printf("number_read = %lu\n", number_read);
        //#endif
        unsigned long long num_vals_to_read = 0;
        unsigned long long new_num_vals_to_read = 0;
        if (number_read + oos_size > this->file_size) {
            if (number_read) {
                num_vals_to_read = this->file_size % number_read;
            }
            else {
                num_vals_to_read = this->file_size;
            }
            new_num_vals_to_read = (num_vals_to_read + 127) & (~127);
        }
        else {
            num_vals_to_read = oos_size;
            new_num_vals_to_read = oos_size;
        }
        //#ifdef DEBUG_PRINT
        //            printf("    num_vals_to_read = %lu\n", num_vals_to_read);
        //            printf("    new_num_vals_to_read = %lu\n", new_num_vals_to_read);
        //            printf("    this->chunk_size = %lu\n", this->chunk_size);
        //            printf("    oos_size = %lu\n", oos_size);
        //#endif
        unsigned long long tot_bytes = sizeof(Itemtype) * new_num_vals_to_read;
        QueryPerformanceCounter(&start);
        read_into_buffer(&old_file, sort_buffer, tot_bytes);
        QueryPerformanceCounter(&end);

        read_duration += end.QuadPart - start.QuadPart;
        number_read += num_vals_to_read;
        //#ifdef DEBUG_PRINT
        //            printf("    number_read after read = %llu\n", number_read);
        //#endif
                /*for (unsigned int i = 0; i < 2; i++) {
                    printf("buffer[%d] = %u\n", i, sort_buffer[i]);
                }
                for (unsigned int i = num_vals_to_read - 2; i < num_vals_to_read; i++) {
                    printf("buffer[%d] = %u\n", i, sort_buffer[i]);
                }*/
                // sort the buffer and get time info
        Itemtype* sort_buffer_end = sort_buffer + num_vals_to_read;
        Itemtype* output = (Itemtype*)_aligned_malloc(static_cast<size_t>(oos_size) * sizeof(Itemtype), this->bytes_per_sector);
        Itemtype* o = sort_buffer;

        if (num_vals_to_read < (1LLU << 20) / sizeof(Itemtype)) {
            //printf("    quicksort\n");
            QueryPerformanceCounter(&start);
            std::sort(sort_buffer, sort_buffer + num_vals_to_read);
            QueryPerformanceCounter(&end);
        }
        else {
            //printf("    origami\n");
            ui n_threads = 8;
            ui n_cores = 8;
            ui min_k = 4;
            Itemtype* kway_buf = nullptr;
            ui64 kway_buf_size = MB(256);
            if (min_k > 2) {
                kway_buf = (Itemtype*)VALLOC(kway_buf_size);
                memset(kway_buf, 0, kway_buf_size);
            }
            QueryPerformanceCounter(&start);
            //o = origami_sorter::sort_single_thread<Itemtype, Regtype>(sort_buffer, output, sort_buffer_end, num_vals_to_read, 2, nullptr);
            o = origami_sorter::sort_multi_thread<Itemtype, Regtype>(sort_buffer, output, num_vals_to_read, n_threads, n_cores, min_k, kway_buf);
            QueryPerformanceCounter(&end);
            VFREE(kway_buf);
        }
        Itemtype last_val = 0;

#ifdef SORT_CHECK
        for (int i = 0; i < num_vals_to_read; i++) {
            if (o[i] < last_val) {
                printf("%s: Output buffer out of order: %u > %u at i = %d (i_max = %llu) \n", __FUNCTION__, last_val, o[i], i, num_vals_to_read);
                exit(1);
            }
            last_val = o[i];
        }
#endif
#ifdef DEBUG_PRINT
        for (unsigned int i = 0; i < 2 && i < num_vals_to_read; i++) {
            printf("buffer[%d] = %u\n", i, o[i]);
            printf("o[%d] = %u\n", i, o[i]);
        }
        if (num_vals_to_read > 6) {
            printf("buffer[%d] = %llu\n", 262144 - 3, o[262144 - 3]);
            printf("buffer[%d] = %llu\n", 262144 - 2, o[262144 - 2]);
            printf("buffer[%d] = %llu\n", 262144 - 1, o[262144 - 1]);
            printf("buffer[%d] = %llu\n", 262144, o[262144]);
            printf("buffer[%d] = %llu\n", 262144 + 1, o[262144 + 1]);
            printf("buffer[%d] = %llu\n", 262144 + 2, o[262144 + 2]);
            printf("buffer[%d] = %llu\n", 262144 + 3, o[262144 + 3]);
        }
        for (unsigned int i = num_vals_to_read - 2; i < num_vals_to_read && i >= 0; i++) {
            printf("buffer[%d] = %u\n", i, sort_buffer[i]);
        }
#endif
        /*for (unsigned int i = 0; i < 2 && i < num_vals_to_read; i++) {
            printf("buffer[%d] = %u\n", i, o[i]);
            printf("o[%d] = %u\n", i, o[i]);
        }*/
        /*if (num_vals_to_read > 6) {
            printf("buffer[%d] = %llu\n", num_vals_to_read / 2 - 3, o[num_vals_to_read / 2 - 3]);
            printf("buffer[%d] = %llu\n", num_vals_to_read / 2 - 2, o[num_vals_to_read / 2 - 2]);
            printf("buffer[%d] = %llu\n", num_vals_to_read / 2 - 1, o[num_vals_to_read / 2 - 1]);
            printf("buffer[%d] = %llu\n", num_vals_to_read / 2, o[num_vals_to_read / 2]);
            printf("buffer[%d] = %llu\n", num_vals_to_read / 2 + 1, o[num_vals_to_read / 2 + 1]);
            printf("buffer[%d] = %llu\n", num_vals_to_read - 2, o[num_vals_to_read - 2]);
            printf("buffer[%d] = %llu\n", num_vals_to_read - 1, o[num_vals_to_read - 1]);
        }*/
        sort_duration += end.QuadPart - start.QuadPart;
        unsigned long long loop_written = 0;
        while (written < number_read)
        {
            //#ifdef DEBUG_PRINT
            //                printf("    written = %lu\n", written);
            //                printf("    number_read = %lu\n", number_read);
            //#endif
                        //unsigned long num_bytes_to_write = (number_read % this->write_buffer_size == 0) ? (this->write_buffer_size) : ((written + this->write_buffer_size > number_read) ? (number_read % written) : (this->write_buffer_size));
            unsigned long num_vals_to_write = 0;
            unsigned long new_num_vals_to_write = 0;
            if (written + this->write_buffer_size > number_read) {
                if (written && number_read % written != 0) {
                    num_vals_to_write = number_read % written;
                }
                else if (written && number_read % written == 0) {
                    num_vals_to_write = oos_size;
                }
                else {
                    num_vals_to_write = number_read;
                }
                new_num_vals_to_write = (num_vals_to_write + 127) & (~127);
            }
            else {
                num_vals_to_write = this->write_buffer_size;
                new_num_vals_to_write = this->write_buffer_size;
            }

            DWORD num_bytes_touched;
            QueryPerformanceCounter(&start);
            bool was_success = WriteFile(chunk_sorted_file, o + loop_written, sizeof(Itemtype) * new_num_vals_to_write, &num_bytes_touched, NULL);
            QueryPerformanceCounter(&end);
            sort_write_duration += end.QuadPart - start.QuadPart;

            if (!(was_success)) {
                printf("%s: Failed writing to new file for sort output with %d\n", __FUNCTION__, GetLastError());
                exit(1);
            }

            if (num_vals_to_write != new_num_vals_to_write) {
                // this is likely incorrect and also in the wrong spot in this function
                // the new chunk_sorted_file handle would need the NO_BUFFERING flag as well after this. The chunk sizes must be still multiples of 512
                printf("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\n");
                CloseHandle(chunk_sorted_file);
                chunk_sorted_file = CreateFile(this->chunk_sorted_fname, GENERIC_READ | GENERIC_WRITE,
                    FILE_SHARE_READ | FILE_SHARE_DELETE | FILE_SHARE_WRITE,
                    NULL,
                    OPEN_EXISTING,
                    FILE_ATTRIBUTE_NORMAL,
                    NULL);
                DWORD num_moved = 0;
                num_moved = SetFilePointer(chunk_sorted_file, this->windows_fs.LowPart, NULL/* & this->windows_fs.HighPart*/, FILE_BEGIN);
                if (num_moved == INVALID_SET_FILE_POINTER) {
                    printf("%s: error in SetFilePointer with %d\n", __FUNCTION__, GetLastError());
                    exit(1);
                }
                /*if (!SetEndOfFile(chunk_sorted_file)) {
                    printf("%s: error in SetEndOfFile with %d\n", __FUNCTION__, GetLastError());
                    exit(1);
                }*/
                //windows_fs.QuadPart = before_sfp.QuadPart;
            }


            num_bytes_written.QuadPart += num_bytes_touched;
            written += num_vals_to_write;
            loop_written += num_vals_to_write;
            if ((written * sizeof(Itemtype)) % GB(1LLU) == 0) {
                printf("                                                                          \r");
                printf("    Written: %llu B (%llu MB)", written * sizeof(Itemtype), written * sizeof(Itemtype) / (1 << 20));
                //printf("      tot_bytes_read  = %llu B = %llu MB\n", tot_bytes_read, tot_bytes_read / (1LLU << 20));
            }
            if (this->debug) {
                printf("    num_vals_to_write = %lu\n", num_vals_to_write);
                printf("    new_num_bytes_to_write = %lu\n", new_num_vals_to_write);
                printf("    written after write = %llu\n", written);
                printf("    num_bytes_touched = %d\n", num_bytes_touched);
            }
        }
        loop_written = 0;

        if (this->debug) {
            printf("    file_size = %llu\n", this->file_size);
            printf("    windows_fs.QuadPart = %llu\n", this->windows_fs.QuadPart);
            printf("    windows_fs.HighPart = %lu\n", this->windows_fs.HighPart);
            printf("    windows_fs.LowPart = %llu\n", this->windows_fs.LowPart);
            printf("    write_buffer_size = %d\n", this->write_buffer_size);
        }
        _aligned_free(output);
    }
    if (this->debug)
    {
        printf("\n");
    }

    _aligned_free(sort_buffer);
    sort_buffer = nullptr;
    CloseHandle(old_file);
    CloseHandle(chunk_sorted_file);

    this->number_elements_touched = number_read;
    this->sort_duration = sort_duration / freq.QuadPart;
    this->read_duration = read_duration / freq.QuadPart;
    this->sort_write_duration = sort_write_duration / freq.QuadPart;
    printf("\n    Sort done in %.2f ms, speed %.2f M keys/s\n", this->sort_duration * 1e3, this->file_size * 1.0 / 1e6 / this->sort_duration);
    printf("    Read done in %.2f ms, speed %.2f M keys/s\n", this->read_duration * 1e3, this->file_size * 1.0 / 1e6 / this->read_duration);
    printf("    Write done in %.2f ms, speed %.2f M keys/s\n", this->sort_write_duration * 1e3, this->file_size * 1.0 / 1e6 / this->sort_write_duration);
#undef SORT_CHECK
    return 0;
}


uint64_t populate_blocks(unsigned idx, unsigned long long remaining_vals, unsigned long long total_vals_to_read, HANDLE* fp)
{
    HANDLE f = *fp;
    DWORD num_bytes_touched;
    //unsigned long long tot_bytes = sizeof(Itemtype) * new_num_vals_to_read;
    unsigned long long num_loops = 0;
    // - 511 since it needs to be divisible by 512
    //unsigned num_to_read = UINT_MAX - 511;
    // want to read only 1 MB at a time
    unsigned num_to_read = mem_block_size;
    uint64_t tot_bytes = remaining_vals * sizeof(Itemtype);
    uint64_t total_overread_bytes = total_vals_to_read * sizeof(Itemtype);

    unsigned long long buf_offset = 0;
    uint64_t num_vals_last_block = 0;
#ifdef DEBUG_PRINT
    printf("        %s: state[idx].num_blocks = %llu\n", __FUNCTION__, state[idx].num_blocks);
#endif
    for (int j = 0; j < state[idx].num_blocks; j++) {
        unsigned long long vals_to_copy = (std::min)(total_vals_to_read, mem_block_size / sizeof(Itemtype));
        //printf("j = %u, vals_to_copy = %llu\n", j, vals_to_copy);
        LARGE_INTEGER aligned_vals_to_copy = { 0 };
        aligned_vals_to_copy.QuadPart = (vals_to_copy + 127) & (~127);
        //unsigned long long padded_vals_to_copy = vals_to_copy;

        if (j == state[idx].num_blocks - 1)
        {
            //padded_vals_to_copy += (sizeof(Regtype) / sizeof(Itemtype) - (vals_to_copy % (sizeof(Regtype) / sizeof(Itemtype))));
            num_vals_last_block = (std::min)(remaining_vals, mem_block_size / sizeof(Itemtype));
#ifdef DEBUG_PRINT

            printf("                state[%d].num_vals_last_block  = %llu\n", idx, num_vals_last_block);
            printf("                    vals_to_copy  = %llu\n", vals_to_copy);
            printf("                    aligned_vals_to_copy  = %llu\n", aligned_vals_to_copy.QuadPart);
#endif
        }

        if (free_blocks.size() == 0)
        {
            //printf("    free block queue is 0 with num_blocks = %u and j = %u\n", this->state[idx].num_blocks, j);
            Itemtype* temp = (Itemtype*)_aligned_malloc(mem_block_size, glb_bytes_per_sector);
            free_blocks.push(temp);
        }

        Itemtype* temp = free_blocks.front();
        free_blocks.pop();
        hrc::time_point s, e;
        s = hrc::now();
        bool was_success = ReadFile(f, temp, (DWORD)(aligned_vals_to_copy.QuadPart * sizeof(Itemtype)), &num_bytes_touched, NULL);
        e = hrc::now();
        double el = ELAPSED_MS(s, e);
        glb_read_duration += (el / 1000);
        if (!(was_success)) {
            printf("%s: Failed reading from populated file with %d\n", __FUNCTION__, GetLastError());
            exit(1);
        }
        tot_bytes -= (vals_to_copy * sizeof(Itemtype));
        num_loops++;
        //memcpy(temp, rbuff + buf_offset, vals_to_copy * sizeof(Itemtype));
        state[idx].bq.push(temp);
        //buf_offset += vals_to_copy;
        remaining_vals -= vals_to_copy;
        total_vals_to_read -= vals_to_copy;
    }
#ifdef DEBUG_PRINT
    printf("            tot_bytes  = %llu\n", tot_bytes);
    printf("            state[idx].bq.front()[0]  = %u\n", state[idx].bq.front()[0]);
    printf("            state[idx].bq.front()[1]  = %u\n", state[idx].bq.front()[1]);
    printf("            state[idx].bq.front()[2047]  = %u\n", state[idx].bq.front()[2047]);
    printf("            state[idx].bq.front()[2048]  = %u\n", state[idx].bq.front()[2048]);
    printf("            state[idx].bq.front()[262142]  = %u\n", state[idx].bq.front()[262142]);
    printf("            state[idx].bq.front()[262143]  = %u\n", state[idx].bq.front()[262143]);
#endif
    return num_vals_last_block;
}


void process_buffer(int stream_idx, char** _p, char** _endp) {
    // define X and endX to be pointers on the current block for each stream
    // experiment with size of memory blocks to help minimize process buffer calls (1MB -> 16 MB -> 32 -> 64)
#define CORRECTNESS_CHECK
    if (stream_idx == -1) {		// flush output buffer
        char* output = X[num_chunks];
        char* endpos = *_p;
        //printf("endpos = %llx\n", endpos);
        if (endpos != nullptr) {
            ui64 bytes = endpos - output;
            HANDLE h_write = fp[num_chunks];
            DWORD bytesWritten;
            hrc::time_point s, e;
            s = hrc::now();
            int bWrt = WriteFile(h_write, output, bytes, &bytesWritten, NULL);
            e = hrc::now();
            double el = ELAPSED_MS(s, e);
            glb_write_duration += (el / 1000);
            if (bWrt == 0) {
                printf("WriteFile failed with %d\n", GetLastError());
                getchar();
                exit(-1);
            }
            tot_bytes_written += bytesWritten;

#ifdef CORRECTNESS_CHECK
            for (int i = 0; i < bytes / sizeof(Itemtype); i++) {
                if (*((Itemtype*)output + i) < glb_last_val) {
                    printf("%s: Output buffer out of order: %u > %u at i = %d (i_max = %llu) \n", __FUNCTION__, glb_last_val, *((Itemtype*)output + i), i, bytes / sizeof(Itemtype));
                    printf("      tot_bytes_read  = %llu B = %llu MB\n", tot_bytes_read, tot_bytes_read / (1LLU << 20));
                    printf("        tot_bytes_written = %llu B (%llu MB)\n", tot_bytes_written, tot_bytes_written / (1 << 20));
                    printf("            output[1] = %llu\n", *((Itemtype*)output + 1));
                    printf("        endX[0] - X[0] = %llu\n", endX[0] - X[0]);
                    printf("        &state[0].num_blocks = %llu\n", (&state[0])->num_blocks);
                    printf("        &state[0].curr_block = %llu\n", (&state[0])->curr_block);
                    printf("        &state[1].num_blocks = %llu\n", (&state[1])->num_blocks);
                    printf("        &state[1].curr_block = %llu\n", (&state[1])->curr_block);
                    printf("        &state[2].num_blocks = %llu\n", (&state[2])->num_blocks);
                    printf("        &state[2].curr_block = %llu\n", (&state[2])->curr_block);
                    printf("        &state[3].num_blocks = %llu\n", (&state[3])->num_blocks);
                    printf("        &state[3].curr_block = %llu\n", (&state[3])->curr_block);
                    printf("          bytes_left[0] = %llu B\n", bytes_left[0]);
                    printf("          bytes_left[1] = %llu B\n", bytes_left[1]);
                    printf("          bytes_left[2] = %llu B\n", bytes_left[2]);
                    printf("          bytes_left[3] = %llu B\n", bytes_left[3]);
                    exit(1);
                }
                glb_last_val = *((Itemtype*)output + i);
            }
#endif

            if (tot_bytes_written % GB(1LLU) == 0) {
                printf("                                                                          \r");
                printf("    Written: %llu B (%llu MB)", tot_bytes_written, tot_bytes_written / (1 << 20));
                //printf("      tot_bytes_read  = %llu B = %llu MB\n", tot_bytes_read, tot_bytes_read / (1LLU << 20));
            }
        }
        *_p = X[num_chunks];
        *_endp = endX[num_chunks];
    }
    else {						// fill input buffer
        //seek_to_write = true;
        // it's the end of the block that X and endX point to. Need to check things
        HANDLE f = fp[stream_idx];
        //ui64 bytes = min(in_buf_size, bytes_left[stream_idx]);
        DWORD bytes_read;
        char* p = X[stream_idx];
        DWORD max_read = mem_block_size;

        state_vars* sv = &state[stream_idx];

        // it should be impossible for the current block to not be empty
        // it's in the last block
        if (sv->curr_block == sv->num_blocks && bytes_left[stream_idx] > 0 && str_inits[stream_idx] == true)
        {
#ifdef DEBUG_PRINT
            printf("    Refilling... stream = %u\n", stream_idx);
            printf("      tot_bytes_read  = %llu B = %llu MB\n", tot_bytes_read, tot_bytes_read / (1LLU << 20));
            printf("        tot_bytes_written = %llu B (%llu MB)\n", tot_bytes_written, tot_bytes_written / (1 << 20));

            printf("        &state[0].num_blocks = %llu\n", (&state[0])->num_blocks);
            printf("        &state[0].curr_block = %llu\n", (&state[0])->curr_block);
            printf("        &state[1].num_blocks = %llu\n", (&state[1])->num_blocks);
            printf("        &state[1].curr_block = %llu\n", (&state[1])->curr_block);
            printf("        &state[2].num_blocks = %llu\n", (&state[2])->num_blocks);
            printf("        &state[2].curr_block = %llu\n", (&state[2])->curr_block);
            printf("        &state[3].num_blocks = %llu\n", (&state[3])->num_blocks);
            printf("        &state[3].curr_block = %llu\n", (&state[3])->curr_block);
#endif
            free_blocks.push(sv->bq.front());
            sv->bq.pop();
            bytes_left[stream_idx] -= (sv->num_vals_last_block * sizeof(Itemtype));
            tot_bytes_read += (sv->num_vals_last_block * sizeof(Itemtype));
#ifdef DEBUG_PRINT
            printf("          bytes_left[0] = %llu B\n", bytes_left[0]);
            printf("          bytes_left[1] = %llu B\n", bytes_left[1]);
            printf("          bytes_left[2] = %llu B\n", bytes_left[2]);
            printf("          bytes_left[3] = %llu B\n", bytes_left[3]);
#endif
            if (/*sv->start_offset + sv->seek_offset < sv->end_offset*/bytes_left[stream_idx] > 0 && str_inits[stream_idx] == true)
            {
                num_refills++;
                LARGE_INTEGER num_bytes_to_move = { 0 };
                num_bytes_to_move.QuadPart = sv->start_offset + sv->seek_offset;

                LARGE_INTEGER aligned_bytes_to_move = { 0 };
                aligned_bytes_to_move.QuadPart = (num_bytes_to_move.QuadPart + 511) & (~511);

                if (num_bytes_to_move.QuadPart % 512 != 0) {
                    aligned_bytes_to_move.QuadPart -= 512;
                }

                tot_bytes_read += (sv->num_vals_last_block * sizeof(Itemtype));
#ifdef DEBUG_PRINT
                printf("      tot_bytes_read  = %llu B = %llu MB\n", tot_bytes_read, tot_bytes_read / (1LLU << 20));
#endif
                sv->bufsize = (std::min)(largest_chunk, (uint64_t)((sv->end_offset - (sv->start_offset + sv->seek_offset)) / sizeof(Itemtype)));
                sv->nobuff_bufsize = (static_cast<INT64>(sv->bufsize) + 127) & (~127);

                unsigned long long bytes_to_read = sv->bufsize * sizeof(Itemtype);
                unsigned long long new_bytes_to_read = ((bytes_to_read + (num_bytes_to_move.QuadPart - aligned_bytes_to_move.QuadPart)) + 511) & (~511);

                // since num_bytes_to_move must be aligned on 512, there's a possibility that the new aligned_bytes_to_move and bytes_to_read wouldn't actually cover the data area
                // where the desired ints are held. So, may need to read more bytes to new_bytes_to_read to get to the area that holds the ints we care about
                while (new_bytes_to_read + aligned_bytes_to_move.QuadPart < sv->seek_offset + bytes_to_read) {
                    new_bytes_to_read += 512;
                }

                unsigned long long remaining_vals = sv->bufsize;
                sv->num_blocks = (bytes_to_read % (mem_block_size) == 0) ? (bytes_to_read / (mem_block_size)) : (bytes_to_read / (mem_block_size)+1);
#ifdef DEBUG_PRINT
                printf("    bytes_to_read            = %llu\n", bytes_to_read);
                printf("    new_bytes_to_read        = %llu\n", new_bytes_to_read);
                printf("    num_bytes_to_move        = %llu\n", num_bytes_to_move.QuadPart);
                printf("    aligned_bytes_to_move    = %llu\n", aligned_bytes_to_move.QuadPart);
                printf("    sv->bufsize              = %llu\n", sv->bufsize);
#endif
                DWORD num_moved = SetFilePointer(f, aligned_bytes_to_move.LowPart, &aligned_bytes_to_move.HighPart, FILE_BEGIN);
                if (num_moved == INVALID_SET_FILE_POINTER) {
                    printf("%s: Failed setting file pointer in populated file with %d\n", __FUNCTION__, GetLastError());
                    exit(1);
                }
                nseeks++;
                //unsigned long long old_last_bytes = sv->num_vals_last_block * sizeof(Itemtype);

                hrc::time_point s, e;
                s = hrc::now();
                sv->num_vals_last_block = populate_blocks((unsigned)stream_idx, remaining_vals, remaining_vals/*new_bytes_to_read / sizeof(Itemtype)*/, &f);
                e = hrc::now();
                double el = ELAPSED_MS(s, e);
                glb_populate_duration += (el / 1000);

                sv->seek_offset += bytes_to_read;
                sv->curr_block = 1;

                ui64 in_buf_size = (std::min)(mem_block_size, remaining_vals * sizeof(Itemtype));
#ifdef DEBUG_PRINT
                printf("    in_buf_size    = %llu\n", in_buf_size);
                printf("    remaining_vals = %llu\n", remaining_vals);
                printf("    mem_block_size = %llu\n", mem_block_size);
                printf("        &state[0].num_blocks = %llu\n", (&state[0])->num_blocks);
                printf("        &state[1].num_blocks = %llu\n", (&state[1])->num_blocks);
                printf("        &state[2].num_blocks = %llu\n", (&state[2])->num_blocks);
                printf("        &state[3].num_blocks = %llu\n", (&state[3])->num_blocks);
                printf("        oldX = %llu\n", p);
#endif
                // now must set the appropriate info
                X[stream_idx] = (char*)state[stream_idx].bq.front();
                endX[stream_idx] = (char*)state[stream_idx].bq.front() + in_buf_size;
#ifdef DEBUG_PRINT
                printf("        newX = %llu\n", X[stream_idx]);
                printf("        endX = %llu\n", endX[stream_idx]);
                printf("        endX - newX = %llu\n", endX[stream_idx] - X[stream_idx]);
#endif
                * _p = X[stream_idx];
                *_endp = endX[stream_idx];
#ifdef DEBUG_PRINT
                printf("          bytes_left[0] = %llu MB\n", bytes_left[0] / (1LLU << 20));
                printf("          bytes_left[1] = %llu MB\n", bytes_left[1] / (1LLU << 20));
                printf("          bytes_left[2] = %llu MB\n", bytes_left[2] / (1LLU << 20));
                printf("          bytes_left[3] = %llu MB\n", bytes_left[3] / (1LLU << 20));
                printf("          bytes_left[0] = %llu B\n", bytes_left[0]);
                printf("          bytes_left[1] = %llu B\n", bytes_left[1]);
                printf("          bytes_left[2] = %llu B\n", bytes_left[2]);
                printf("          bytes_left[3] = %llu B\n", bytes_left[3]);

                printf("             stream_idx = %llu\n", stream_idx);
                //printf("             old_last_bytes = %llu\n", old_last_bytes);
                printf("             sv->num_vals_last_block = %llu\n", sv->num_vals_last_block);
                //bytes_left[stream_idx] -= (std::min)(mem_block_size, old_last_vals * sizeof(Itemtype));
                //bytes_left[stream_idx] -= old_last_bytes;
                printf("          bytes_left[stream_idx] = %llu\n", bytes_left[stream_idx]);
                printf("            X[stream_idx][0] = %u\n", *((Itemtype*)X[stream_idx]));
                printf("            X[stream_idx][1] = %u\n", *((Itemtype*)X[stream_idx] + 1));
                printf("            X[stream_idx][2] = %u\n", *((Itemtype*)X[stream_idx] + 2));
                printf("            X[1][0] = %u\n", *((Itemtype*)X[1]));
                printf("            X[1][1] = %u\n", *((Itemtype*)X[1] + 1));
                printf("            X[1][2] = %u\n", *((Itemtype*)X[1] + 2));
                printf("            X[2][0] = %u\n", *((Itemtype*)X[2]));
                printf("            X[2][1] = %u\n", *((Itemtype*)X[2] + 1));
                printf("            X[2][2] = %u\n", *((Itemtype*)X[2] + 2));
                printf("            X[3][0] = %u\n", *((Itemtype*)X[3]));
                printf("            X[3][1] = %u\n", *((Itemtype*)X[3] + 1));
                printf("            X[3][2] = %u\n", *((Itemtype*)X[3] + 2));
#endif
            }
            else if (bytes_left[stream_idx] == 0 && str_inits[stream_idx] == true) {
#ifdef DEBUG_PRINT
                printf("    No more bytes left in stream %u\n", stream_idx);
                printf("          bytes_left[stream_idx] = %llu\n", bytes_left[stream_idx]);
#endif
                * _p = X[stream_idx];
                *_endp = X[stream_idx];
            }
            else {
                printf("    %s: probable error", __FUNCTION__);
                exit(1);
            }
        }
        // not in last block
        else if (sv->curr_block != sv->num_blocks && bytes_left[stream_idx] > 0 && str_inits[stream_idx] == true)
        {
            free_blocks.push(sv->bq.front());
            sv->bq.pop();
            sv->curr_block++;
            //sv->bufsize -= (mem_block_size / sizeof(Itemtype));
            //ui64 in_buf_size = (std::min)((unsigned long long)mem_block_size, sv->bufsize * sizeof(Itemtype));
            ui64 in_buf_size = mem_block_size;
            if (sv->curr_block == sv->num_blocks)
                in_buf_size = sv->num_vals_last_block * sizeof(Itemtype);
            /*if (sv->curr_block == sv->num_blocks)
            {
                printf("here");
                in_buf_size = (std::min)(in_buf_size, sv->num_vals_last_block * sizeof(Itemtype));
                printf("    in_buf_size = %llu\n", in_buf_size);
            }*/

            // now must set the appropriate info
            //printf("  stream = %u, in_buf_size = %llu  ", stream_idx, in_buf_size);
            //printf("  sv->bufsize = %llu  ", sv->bufsize);
            X[stream_idx] = (char*)state[stream_idx].bq.front();
            endX[stream_idx] = (char*)state[stream_idx].bq.front() + in_buf_size;
            *_p = X[stream_idx];
            *_endp = endX[stream_idx];
            bytes_left[stream_idx] -= mem_block_size;
            tot_bytes_read += mem_block_size;
        }
        else {
#ifdef DEBUG_PRINT
            printf("                    Stream %d initialized\n", stream_idx);
#endif
            str_inits[stream_idx] = true;
            ui64 in_buf_size = mem_block_size;
            if (sv->curr_block == sv->num_blocks)
                in_buf_size = sv->num_vals_last_block * sizeof(Itemtype);
            X[stream_idx] = (char*)state[stream_idx].bq.front();
            endX[stream_idx] = (char*)state[stream_idx].bq.front() + in_buf_size;
            *_p = X[stream_idx];
            *_endp = endX[stream_idx];
        }
    }
#undef CORRECTNESS_CHECK
}


void external_sort::init_buffers(char* buf) {
    // mem_size = RAM, buf = char array 
    // out_buf_size = MB(1);
    num_seeks = 0;
    glb_bytes_per_sector = this->bytes_per_sector;
    ui64 largest_chunk = 0;
    // delta now in bytes
    // need to have a pointer in X to the start of every stream's first block
    FOR(i, num_chunks, 1) {
        char* p = (char*)state[i].bq.front();
        ui64 in_buf_size = mem_block_size;
        if (state[i].curr_block == state[i].num_blocks)
            in_buf_size = state[i].num_vals_last_block * sizeof(Itemtype);
        X[i] = p;
        endX[i] = p + in_buf_size;
    }
    if (two_real_chunks == 2) {
        //need to make it so X[2] = endX[2] and X[3] = endX[3]
        X[2] = (char*)state[2].bq.front();
        endX[2] = X[2];
        X[3] = (char*)state[3].bq.front();
        endX[3] = X[3];

        // output buffer
        X[4] = buf;
        endX[4] = buf + (this->write_buffer_size * sizeof(Itemtype));
    }
    else {
        //output buffer
        X[num_chunks] = buf;
        endX[num_chunks] = buf + (this->write_buffer_size * sizeof(Itemtype));
    }
}


int external_sort::merge_sort()
{
    /*
        Merge sort Version 1 Steps (i.e., all sizes line up for an easy implementation)
        1)  Create an array of size num_chunks of MinHeapNodes and an array of size 1GB
                i)  Populate the 1GB array with the first n vals from each chunk
        2)  Create MinHeapNodes with the first val from every chunk (now taken from the 1GB array) and insert into array
        3)  Create MinHeap from the array
        4)  While the MinHeap is not empty:
                While the buffersize array is not full:
                    i)  Pop heap and take the val from that node and append it to the buffersize array
                Write buffersize array to the end of the file and "empty" it
    */
    //#define MERGE_DEBUG
    printf("\n%s\n", __FUNCTION__);
    num_chunks = (this->file_size % this->chunk_size == 0) ? (this->file_size / this->chunk_size) : ((this->file_size / this->chunk_size) + 1);
    printf("    num_chunks = %llu\n", num_chunks);
    orig_num_chunks = num_chunks;
    file_size = this->file_size;
    chunk_size = this->chunk_size;
    tot_bytes_read = 0;
    tot_bytes_written = 0;
    mem_block_size = this->block_size;
    num_refills = 0;
    nseeks = 0;
    glb_read_duration = 0;
    glb_write_duration = 0;
    glb_merge_duration = 0;
    glb_populate_duration = 0;
    glb_last_val = 0;

    if (num_chunks == 1) {
        // need to copy the portion from chunk_sorted to full_sorted in the large file
        if (this->seq_run) {
            HANDLE chunk_sorted_file = nullptr, full_sorted_file = nullptr;

            LARGE_INTEGER new_fp = { 0 };
            new_fp.QuadPart = 2 * this->windows_fs.QuadPart;

            chunk_sorted_file = CreateFile(this->chunk_sorted_fname, GENERIC_READ | GENERIC_WRITE,
                FILE_SHARE_READ | FILE_SHARE_DELETE | FILE_SHARE_WRITE,
                NULL,
                OPEN_EXISTING,
                FILE_ATTRIBUTE_NORMAL | FILE_FLAG_NO_BUFFERING,
                NULL);
            full_sorted_file = CreateFile(this->full_sorted_fname, GENERIC_READ | GENERIC_WRITE,
                FILE_SHARE_READ | FILE_SHARE_DELETE | FILE_SHARE_WRITE,
                NULL,
                OPEN_EXISTING,
                FILE_ATTRIBUTE_NORMAL | FILE_FLAG_NO_BUFFERING,
                NULL);
            if (chunk_sorted_file == INVALID_HANDLE_VALUE) {
                printf("%s: Failed opening populated file with %d\n", __FUNCTION__, GetLastError());
                exit(1);
            }
            else if (full_sorted_file == INVALID_HANDLE_VALUE) {
                printf("%s: Failed opening new file for mergesort output with %d\n", __FUNCTION__, GetLastError());
                exit(1);
            }
            DWORD num_moved = 0;
            num_moved = SetFilePointer(chunk_sorted_file, this->windows_fs.LowPart, &this->windows_fs.HighPart, FILE_BEGIN);
            if (num_moved == INVALID_SET_FILE_POINTER) {
                printf("%s: error in SetFilePointer for chunk sorted with %d\n", __FUNCTION__, GetLastError());
                exit(1);
            }

            num_moved = SetFilePointer(full_sorted_file, new_fp.LowPart, &new_fp.HighPart, FILE_BEGIN);
            if (num_moved == INVALID_SET_FILE_POINTER) {
                printf("%s: error in SetFilePointer for full sorted with %d\n", __FUNCTION__, GetLastError());
                exit(1);
            }

            LARGE_INTEGER tot_copied = { 0 };
            ui64 num_mem_bytes = (std::min)((unsigned long long)4294967295, this->merge_mem_avail);
            while (tot_copied.QuadPart < this->file_size) {

                Itemtype* copy_buffer = (Itemtype*)_aligned_malloc(num_mem_bytes * sizeof(Itemtype), this->bytes_per_sector);
                unsigned long long num_vals_to_read = 0;
                unsigned long long new_num_vals_to_read = 0;
                if (tot_copied.QuadPart + num_mem_bytes > this->file_size) {
                    if (tot_copied.QuadPart) {
                        num_vals_to_read = this->file_size % tot_copied.QuadPart;
                    }
                    else {
                        num_vals_to_read = this->file_size;
                    }
                    new_num_vals_to_read = (num_vals_to_read + 127) & (~127);
                }
                else {
                    num_vals_to_read = num_mem_bytes;
                    new_num_vals_to_read = num_mem_bytes;
                }
                if (this->debug) {
                    printf("    num_vals_to_read = %lu\n", num_vals_to_read);
                    printf("    new_num_vals_to_read = %lu\n", new_num_vals_to_read);
                    printf("    this->chunk_size = %lu\n", this->chunk_size);
                    printf("    num_mem_bytes = %lu\n", num_mem_bytes);
                }
                unsigned long long tot_bytes = sizeof(Itemtype) * new_num_vals_to_read;
                read_into_buffer(&chunk_sorted_file, copy_buffer, tot_bytes);
                printf("copy_buffer[0] = %u\n", copy_buffer[0]);
                printf("copy_buffer[1] = %u\n", copy_buffer[1]);
                // once the values are in sort_buffer, write them into the file
                DWORD num_bytes_touched;
                bool was_success = WriteFile(full_sorted_file, copy_buffer, tot_bytes, &num_bytes_touched, NULL);
                if (!(was_success)) {
                    printf("%s: Failed writing to new file for sort output with %d\n", __FUNCTION__, GetLastError());
                    exit(1);
                }
                printf("    num_bytes_touched = %llu\n", num_bytes_touched);
                printf("    tot_bytes = %llu\n", tot_bytes);

                tot_copied.QuadPart += tot_bytes;
                _aligned_free(copy_buffer);
                copy_buffer = nullptr;
            }
            CloseHandle(chunk_sorted_file);
            CloseHandle(full_sorted_file);
        }
        else {
            if (!CopyFile(this->chunk_sorted_fname, this->full_sorted_fname, false))
            {
                printf("%s: Failed copying chunk sorted file with %d\n", __FUNCTION__, GetLastError());
                exit(1);
            }
        }

    }
    else {
        two_real_chunks = false;
        if (num_chunks == 2) {
            two_real_chunks = true;
        }
        LARGE_INTEGER start = { 0 }, end = { 0 }, freq = { 0 }, num_bytes_written = { 0 };
        unsigned long long int written = 0, number_read = 0;

        if (this->debug) {
            printf("    file_size = %llu\n", this->file_size);
            printf("    file_size * sizeof(Itemtype) = %llu\n", this->file_size * sizeof(Itemtype));
            printf("    num_chunks = %llu\n", num_chunks);
            printf("    chunk_size / num_chunks = %llu\n", this->chunk_size / num_chunks);
            printf("    chunk_size = %llu\n", this->chunk_size);
        }
        HANDLE chunk_sorted_file = nullptr, full_sorted_file = nullptr;

        LARGE_INTEGER new_fp = { 0 };
        new_fp.QuadPart = 2 * this->windows_fs.QuadPart;
        if (this->seq_run)
        {
            chunk_sorted_file = CreateFile(this->chunk_sorted_fname, GENERIC_READ | GENERIC_WRITE,
                FILE_SHARE_READ | FILE_SHARE_DELETE | FILE_SHARE_WRITE,
                NULL,
                OPEN_EXISTING,
                FILE_ATTRIBUTE_NORMAL | FILE_FLAG_NO_BUFFERING,
                NULL);
            full_sorted_file = CreateFile(this->full_sorted_fname, GENERIC_READ | GENERIC_WRITE,
                FILE_SHARE_READ | FILE_SHARE_DELETE | FILE_SHARE_WRITE,
                NULL,
                OPEN_EXISTING,
                FILE_ATTRIBUTE_NORMAL | FILE_FLAG_NO_BUFFERING,
                NULL);
            if (chunk_sorted_file == INVALID_HANDLE_VALUE) {
                printf("%s: Failed opening populated file with %d\n", __FUNCTION__, GetLastError());
                exit(1);
            }
            else if (full_sorted_file == INVALID_HANDLE_VALUE) {
                printf("%s: Failed opening new file for mergesort output with %d\n", __FUNCTION__, GetLastError());
                exit(1);
            }
            DWORD num_moved = 0;
            num_moved = SetFilePointer(chunk_sorted_file, this->windows_fs.LowPart, &this->windows_fs.HighPart, FILE_BEGIN);
            if (num_moved == INVALID_SET_FILE_POINTER) {
                printf("%s: error in SetFilePointer for chunk sorted with %d\n", __FUNCTION__, GetLastError());
                exit(1);
            }
            nseeks++;
            num_moved = SetFilePointer(full_sorted_file, new_fp.LowPart, &new_fp.HighPart, FILE_BEGIN);
            if (num_moved == INVALID_SET_FILE_POINTER) {
                printf("%s: error in SetFilePointer for full sorted with %d\n", __FUNCTION__, GetLastError());
                exit(1);
            }
            nseeks++;
        }
        else {
            printf("Need to fix this\n");
            exit(1);
        }

        unsigned long long total_bytes_touched = 0;
        DWORD num_bytes_touched;

        //unsigned long long delta = 0;
        //unsigned long long largest_chunk = 0;
        delta = (2 * (this->merge_mem_avail / sizeof(Itemtype)) / (num_chunks * (num_chunks + 1)));
#ifdef MERGE_DEBUG
        printf("    Number of chunks  = %llu\n", num_chunks);
        printf("       this->merge_mem_avail  = %llu MB\n", this->merge_mem_avail / (1LLU << 20));
#endif
        //printf("       delta          = %llu vals (%llu MB)\n", delta, delta * sizeof(Itemtype) / (1LLU << 20));
        //delta -= (sizeof(Regtype) / sizeof(Itemtype) - (delta % (sizeof(Regtype) / sizeof(Itemtype))));
        //delta -= delta % (sizeof(Regtype) / sizeof(Itemtype));
        delta -= delta % this->bytes_per_sector;
        //delta += (4LLU * 512);
        delta += (1LLU * 512);
#ifdef MERGE_DEBUG
        printf("       delta          = %llu vals (%llu MB)\n", delta, delta * sizeof(Itemtype) / (1LLU << 20));
#endif
        largest_chunk = num_chunks * delta;

#ifdef MERGE_DEBUG
        printf("       largest_chunk  = %llu vals (%llu MB)\n", largest_chunk, largest_chunk * sizeof(Itemtype) / (1LLU << 20));
#endif

        unsigned long long tot_bytes_from_delta = 0;
        for (unsigned int i = 0; i < num_chunks; i++)
        {
            tot_bytes_from_delta += (static_cast<unsigned long long>(i) + 1) * delta;
        }
        printf("       tot_bytes_from_delta  = %llu MB\n", tot_bytes_from_delta / (1LLU << 20));

        //Itemtype* sorted_num_buffer = (Itemtype*)_aligned_malloc(this->write_buffer_size * sizeof(Itemtype), this->bytes_per_sector);
        INT64 running_file_offset = 0;

        for (int i = 0; i < num_chunks; i++) {
            struct state_vars new_chunk = { 0 };
            if (i != num_chunks - 1 && this->file_size % this->chunk_size != 0) {
                new_chunk.chunk_size = this->chunk_size;
            }
            else if (i == num_chunks - 1 && this->file_size % this->chunk_size != 0) {
                new_chunk.chunk_size = this->file_size % this->chunk_size;
            }
            else {
                new_chunk.chunk_size = this->chunk_size;
            }

            // how many vals are currently in the write buffer
            new_chunk.curr_buflen = 0;

            // what is the current block number
            new_chunk.curr_block = 1;

            // if it's in a large sequential file, need to set the offsets properly within the space
            if (!this->seq_run)
            {
                // start of the whole chunk in the file
                new_chunk.start_offset = running_file_offset * sizeof(Itemtype);
                // end of the whole chunk in the file (equivalent to the start of the next chunk, if it exists)
                new_chunk.end_offset = (running_file_offset + new_chunk.chunk_size) * sizeof(Itemtype);
                // how big a portion this chunk gets in memory (not necessarilly contiguous)
                new_chunk.bufsize = (std::min)((INT64)(delta * (static_cast<unsigned long long>(i) + 1)), (INT64)((new_chunk.end_offset - new_chunk.start_offset) / sizeof(Itemtype)));
                // next place in the file to start the next seek from the start_offset
                new_chunk.seek_offset = (std::min)(new_chunk.bufsize * sizeof(Itemtype), new_chunk.end_offset - new_chunk.start_offset);
            }
            else {
                // start of the whole chunk in the file
                new_chunk.start_offset = this->windows_fs.QuadPart + running_file_offset * sizeof(Itemtype);
                // end of the whole chunk in the file (equivalent to the start of the next chunk, if it exists)
                new_chunk.end_offset = this->windows_fs.QuadPart + (running_file_offset + new_chunk.chunk_size) * sizeof(Itemtype);
                // how big a portion this chunk gets in memory (not necessarilly contiguous)
                new_chunk.bufsize = (std::min)((INT64)(delta * (static_cast<unsigned long long>(i) + 1)), (INT64)((new_chunk.end_offset - new_chunk.start_offset) / sizeof(Itemtype)));
                // next place in the file to start the next seek from the start_offset
                new_chunk.seek_offset = /*this->windows_fs.QuadPart + */(std::min)(new_chunk.bufsize * sizeof(Itemtype), new_chunk.end_offset - new_chunk.start_offset);
            }
            new_chunk.nobuff_bufsize = (static_cast<INT64>(new_chunk.bufsize) + 127) & (~127);

            // each chunk's portion of the memory is made out of blocks, which are 1 MB sizes of memory linked together in a queue
            new_chunk.num_blocks = (new_chunk.bufsize * sizeof(Itemtype) % (this->block_size) == 0) ? (new_chunk.bufsize * sizeof(Itemtype) / (this->block_size)) : (new_chunk.bufsize * sizeof(Itemtype) / (this->block_size) + 1);
#ifdef MERGE_DEBUG
            printf("  i = %u\n", i);
            printf("    new_chunk.num_blocks = %llu\n", new_chunk.num_blocks);
            printf("    new_chunk.bufsize    = %llu\n", new_chunk.bufsize);
            printf("    new_chunk.chunk_size = %llu\n", new_chunk.chunk_size);
            printf("    this->chunk_size     = %llu\n", this->chunk_size);
            printf("    this->file_size     = %llu\n", this->file_size);
#endif
            for (unsigned i = 0; i < new_chunk.num_blocks; i++)
            {
                Itemtype* temp = (Itemtype*)_aligned_malloc(this->block_size, this->bytes_per_sector);
                free_blocks.push(temp);
            }

            running_file_offset += new_chunk.chunk_size;

            //#ifdef DEBUG_PRINT
            //                printf("\ni = %d\n", i);
            //                new_chunk.print();
            //#endif
            state.push_back(new_chunk);
        }

        if (two_real_chunks) {
            for (int i = 0; i < 2; i++) {
                struct state_vars new_chunk = { 0 };
                new_chunk.chunk_size = 0;

                // how many vals are currently in the write buffer
                new_chunk.curr_buflen = 0;

                // what is the current block number
                new_chunk.curr_block = 1;


                //// if it's in a large sequential file, need to set the offsets properly within the space

                //if (!this->seq_run)
                //{
                //    // start of the whole chunk in the file
                //    new_chunk.start_offset = running_file_offset * sizeof(Itemtype);
                //    // end of the whole chunk in the file (equivalent to the start of the next chunk, if it exists)
                //    new_chunk.end_offset = (running_file_offset + new_chunk.chunk_size) * sizeof(Itemtype);
                //    // how big a portion this chunk gets in memory (not necessarilly contiguous)
                //    new_chunk.bufsize = (std::min)((INT64)(delta * (static_cast<unsigned long long>(i) + 1)), (INT64)((new_chunk.end_offset - new_chunk.start_offset) / sizeof(Itemtype)));
                //    // next place in the file to start the next seek from the start_offset
                //    new_chunk.seek_offset = (std::min)(new_chunk.bufsize * sizeof(Itemtype), new_chunk.end_offset - new_chunk.start_offset);
                //}
                //else {
                //    // start of the whole chunk in the file
                //    new_chunk.start_offset = this->windows_fs.QuadPart + running_file_offset * sizeof(Itemtype);
                //    // end of the whole chunk in the file (equivalent to the start of the next chunk, if it exists)
                //    new_chunk.end_offset = this->windows_fs.QuadPart + (running_file_offset + new_chunk.chunk_size) * sizeof(Itemtype);
                //    // how big a portion this chunk gets in memory (not necessarilly contiguous)
                //    new_chunk.bufsize = (std::min)((INT64)(delta * (static_cast<unsigned long long>(i) + 1)), (INT64)((new_chunk.end_offset - new_chunk.start_offset) / sizeof(Itemtype)));
                //    // next place in the file to start the next seek from the start_offset
                //    new_chunk.seek_offset = /*this->windows_fs.QuadPart + */(std::min)(new_chunk.bufsize * sizeof(Itemtype), new_chunk.end_offset - new_chunk.start_offset);
                //}
                //new_chunk.nobuff_bufsize = (static_cast<INT64>(new_chunk.bufsize) + 127) & (~127);

                // each chunk's portion of the memory is made out of blocks, which are 1 MB sizes of memory linked together in a queue
                new_chunk.num_blocks = 1;// (new_chunk.bufsize * sizeof(Itemtype) % (this->block_size) == 0) ? (new_chunk.bufsize * sizeof(Itemtype) / (this->block_size)) : (new_chunk.bufsize * sizeof(Itemtype) / (this->block_size) + 1);

                for (unsigned i = 0; i < new_chunk.num_blocks; i++)
                {
                    Itemtype* temp = (Itemtype*)_aligned_malloc(this->block_size, this->bytes_per_sector);
                    free_blocks.push(temp);
                }

                //running_file_offset += new_chunk.chunk_size;

#ifdef DEBUG_PRINT
                printf("\ni = %d\n", i);
                new_chunk.print();
#endif
                state.push_back(new_chunk);
            }
        }

        LARGE_INTEGER num_bytes_to_move = { 0 };
        QueryPerformanceFrequency(&freq);
        for (unsigned i = 0; i < num_chunks; i++)
        {
            //Itemtype* rbuff = (Itemtype*)_aligned_malloc(state[i].nobuff_bufsize * sizeof(Itemtype), this->bytes_per_sector);
            num_bytes_to_move.QuadPart = state[i].start_offset;
            LARGE_INTEGER aligned_bytes_to_move = { 0 };
            aligned_bytes_to_move.QuadPart = (num_bytes_to_move.QuadPart + 511) & (~511);
            if (num_bytes_to_move.QuadPart % 512 != 0) {
                aligned_bytes_to_move.QuadPart -= 512;
            }
#ifdef DEBUG_PRINT
            if (this->debug) {
                printf("  i = %d\n", i);
                printf("    num_bytes_to_move.QuadPart = %llu\n", num_bytes_to_move.QuadPart);
                printf("    aligned_bytes_to_move.QuadPart = %llu\n", aligned_bytes_to_move.QuadPart);
                printf("    this->state[i].nobuff_bufsize = %llu\n", state[i].nobuff_bufsize);
            }
            printf("  i = %d\n", i);
            printf("    num_bytes_to_move.QuadPart = %llu\n", num_bytes_to_move.QuadPart);
            printf("    aligned_bytes_to_move.QuadPart = %llu\n", aligned_bytes_to_move.QuadPart);
            printf("    this->state[i].nobuff_bufsize = %llu\n", state[i].nobuff_bufsize);
            printf("    this->state[i].bufsize = %llu\n", state[i].bufsize);
#endif

            DWORD num_moved = SetFilePointer(chunk_sorted_file, aligned_bytes_to_move.LowPart, &aligned_bytes_to_move.HighPart, FILE_BEGIN);
            if (num_moved == INVALID_SET_FILE_POINTER) {
                printf("%s: Failed setting file pointer in populated file with %d\n", __FUNCTION__, GetLastError());
                exit(1);
            }
            nseeks++;
            unsigned long long tot_bytes = sizeof(Itemtype) * state[i].nobuff_bufsize;

            // don't read values into an intermediate buffer, can just read them directly into the allocated memory blocks
            //read_into_buffer(&chunk_sorted_file, rbuff, tot_bytes);

            unsigned long long bytes_to_read = state[i].bufsize * sizeof(Itemtype);
            unsigned long long new_bytes_to_read = ((bytes_to_read + (num_bytes_to_move.QuadPart - aligned_bytes_to_move.QuadPart)) + 511) & (~511);

            // since num_bytes_to_move must be aligned on 512, there's a possibility that the new aligned_bytes_to_move and bytes_to_read wouldn't actually cover the data area
            // where the desired ints are held. So, may need to read more bytes to new_bytes_to_read to get to the area that holds the ints we care about
            while (new_bytes_to_read + aligned_bytes_to_move.QuadPart < state[i].seek_offset + bytes_to_read) {
                new_bytes_to_read += 512;
            }
            unsigned long long remaining_vals = (std::min)(state[i].bufsize, state[i].nobuff_bufsize);
#ifdef DEBUG_PRINT
            printf("        remaining_vals        = %llu\n", remaining_vals);
            printf("        new_bytes_to_read     = %llu\n", new_bytes_to_read / sizeof(Itemtype));
            printf("        num_bytes_to_move     = %llu\n", num_bytes_to_move.QuadPart);
            printf("        aligned_bytes_to_move = %llu\n", aligned_bytes_to_move.QuadPart);
#endif
            hrc::time_point s, e;
            s = hrc::now();
            state[i].num_vals_last_block = populate_blocks(i, remaining_vals, new_bytes_to_read / sizeof(Itemtype), &chunk_sorted_file);
            e = hrc::now();
            double el = ELAPSED_MS(s, e);
            glb_populate_duration += (el / 1000);
#ifdef DEBUG_PRINT
            printf("          state[i].num_vals_last_block = %llu\n", state[i].num_vals_last_block);
#endif
        }

        if (two_real_chunks) {
            unsigned long long remaining = 1;
            hrc::time_point s, e;
            s = hrc::now();
            state[2].num_vals_last_block = populate_blocks(2, remaining, remaining, nullptr);
            state[3].num_vals_last_block = populate_blocks(3, remaining, remaining, nullptr);
            e = hrc::now();
            double el = ELAPSED_MS(s, e);
            glb_populate_duration += (el / 1000);
        }

#ifdef MERGE_DEBUG
        for (unsigned int i = 0; i < num_chunks; i += 1) {
            printf("    i = %u\n", i);
            printf("      this->state[i].num_blocks = %llu\n", state[i].num_blocks);
        }
#endif
        if (this->give_vals) {
            //printf("    \n\nraw_num_buffer = %llu\n", raw_num_buffer);
            for (unsigned int i = 0; i < num_chunks; i += 1) {
                printf("    i = %u\n", i);
                printf("      this->state[i].num_blocks = %llu\n", state[i].num_blocks);
                //printf("        this->state[i].bq.front()[0] = %u\n", state[i].bq.front()[0]);
                //printf("        this->state[i].bq.front()[1] = %u\n", state[i].bq.front()[1]);
                //printf("        this->state[i].bq.front()[blocksize - 1] = %u\n", state[i].bq.front()[(this->block_size / sizeof(Itemtype)) - 1]);
                //printf("        this->state[i].bq.front()[bufsize] = %u\n", this->state[i].bq.front()[this->state[i].bufsize]);
                //printf("        this->state[i].bufpos[this->state[i].bufsize - 6] = %u\n", this->state[i].bufpos[this->state[i].bufsize - 6]);
                //printf("        this->state[i].bufpos[this->state[i].bufsize - 5] = %u\n", this->state[i].bufpos[this->state[i].bufsize - 5]);
                //printf("        this->state[i].bufpos[this->state[i].bufsize - 4] = %u\n", this->state[i].bufpos[this->state[i].bufsize - 4]);
                //printf("        this->state[i].bufpos[this->state[i].bufsize - 3] = %u\n", this->state[i].bufpos[this->state[i].bufsize - 3]);
                //printf("        this->state[i].bufpos[this->state[i].bufsize - 2] = %u\n", this->state[i].bufpos[this->state[i].bufsize - 2]);
                //printf("        this->state[i].bufpos[this->state[i].bufsize - 1] = %u\n", this->state[i].bufpos[this->state[i].bufsize - 1]);
                //printf("        this->state[i].bufpos[this->state[i].bufsize] = %u\n", this->state[i].bufpos[this->state[i].bufsize]);
            }
        }

        // 2)  Create MinHeapNodes with the first val from every chunk (now taken from each state's queue) and insert into heap array
        // 3)  Create MinHeap from the array    


        SetThreadAffinityMask(GetCurrentThread(), 1 << 4);

        if (this->seq_run)
        {
            unsigned long long tot_bytes = this->file_size * sizeof(Itemtype);
            FOR(i, num_chunks, 1) {
                bytes_left[i] = (std::min)(tot_bytes, this->chunk_size * sizeof(Itemtype));
                tot_bytes -= (this->chunk_size * sizeof(Itemtype));
                str_inits[i] = false;
            }

            if (two_real_chunks) {
                bytes_left[2] = 0;
                bytes_left[3] = 0;
            }

            LARGE_INTEGER li_dist = { 0 };
            ui64 cur_byte = 0;
            FOR(i, num_chunks, 1) {
                /*HANDLE h_tmp = CreateFile(
                    this->fname,
                    GENERIC_READ | GENERIC_WRITE,
                    FILE_SHARE_READ | FILE_SHARE_DELETE | FILE_SHARE_WRITE,
                    NULL,
                    OPEN_EXISTING,
                    FILE_ATTRIBUTE_NORMAL,
                    NULL
                );
                li_dist.QuadPart = cur_byte;
                BOOL bRet = SetFilePointerEx(
                    h_tmp,
                    li_dist,
                    NULL,
                    FILE_BEGIN
                );
                if (bRet < 0) {
                    printf("SetFilePointerEx error %d\n", GetLastError());
                    exit(-1);
                }*/

                fp[i] = chunk_sorted_file;
                // might be wrong, but each chunk should just be this->mem_avail size
                //cur_byte += this->chunk_size;
            }

            if (two_real_chunks) {
                fp[2] = chunk_sorted_file;
                fp[3] = chunk_sorted_file;
                fp[4] = full_sorted_file;
            }
            else {
                fp[num_chunks] = full_sorted_file;
            }

            // setup in-memory buffers
            char* buf = (char*)_aligned_malloc(this->write_buffer_size * sizeof(Itemtype), this->bytes_per_sector);

            init_buffers(buf);

            void (*f)(int, char**, char**);
            f = &process_buffer;
            //printf("    Initial populate done in %.2f ms, speed %.2f M keys/s\n", glb_populate_duration * 1e3, this->file_size * 1.0 / 1e6 / glb_populate_duration);
            printf("Merging %s ... \n", this->fname);
            hrc::time_point s, e;
            //using Reg = REG_TYPE;
            //using Item = Itemtype;
#ifdef MERGE_DEBUG
            printf("          bytes_left[0] = %llu MB\n", bytes_left[0] / (1LLU << 20));
            printf("          bytes_left[1] = %llu MB\n", bytes_left[1] / (1LLU << 20));
            printf("          bytes_left[2] = %llu MB\n", bytes_left[2] / (1LLU << 20));
            printf("          bytes_left[3] = %llu MB\n", bytes_left[3] / (1LLU << 20));
            printf("          bytes_left[0] = %llu B\n", bytes_left[0]);
            printf("          bytes_left[1] = %llu B\n", bytes_left[1]);
            printf("          bytes_left[2] = %llu B\n", bytes_left[2]);
            printf("          bytes_left[3] = %llu B\n", bytes_left[3]);
#endif

#ifdef DEBUG_PRINT
            printf("            state[0].num_vals_last_block = %llu B\n", state[0].num_vals_last_block * sizeof(Itemtype));
            printf("            state[1].num_vals_last_block = %llu B\n", state[1].num_vals_last_block * sizeof(Itemtype));
            printf("            state[2].num_vals_last_block = %llu B\n", state[2].num_vals_last_block * sizeof(Itemtype));
            printf("            state[3].num_vals_last_block = %llu B\n", state[3].num_vals_last_block * sizeof(Itemtype));
#endif
            printf("    Written: 0 B (0 MB)");
            s = hrc::now();
            origami_external_sorter::merge<Regtype, Itemtype>(f, num_chunks);
            e = hrc::now();
            double el = ELAPSED_MS(s, e);
            printf("\nDone in %.2f ms, Speed: %.2f M keys/s\n", el, this->file_size * 1.0 / el / 1e3);
            this->merge_duration = el / 1000;

            // cleanup
            if (!two_real_chunks) {
                //FOR(i, num_chunks + 1, 1) CloseHandle(fp[i]);
                FOR(i, num_chunks + 1, 1) fp[i] = nullptr;
                //CloseHandle(fp[num_chunks + 1]);
            }
            else {
                //FOR(i, num_chunks + 3, 1) CloseHandle(fp[i]);
                FOR(i, num_chunks + 3, 1) fp[i] = nullptr;
                //CloseHandle(fp[num_chunks + 3]);
            }
            _aligned_free(buf);
            buf = nullptr;
            if (two_real_chunks) {
                _aligned_free(X[2]);
                _aligned_free(X[3]);
            }
        }
        else {
            printf("Need to fix this %s\n", __FUNCTION__);
            exit(1);
        }
        CloseHandle(chunk_sorted_file);
        CloseHandle(full_sorted_file);
    }
    printf("    num_seeks = %d, num_refills = %d\n", nseeks, num_refills);
    //printf("num_refills = %d\n", num_refills);
    //printf("    tot_bytes_read  = %llu B = %llu MB\n", tot_bytes_read, tot_bytes_read / (1LLU << 20));
    //printf("    tot_bytes_written  = %llu B = %llu MB\n", tot_bytes_written, tot_bytes_written / (1LLU << 20));
    //printf("    this->file_size = %llu B = %llu MB\n", this->file_size * sizeof(Itemtype), this->file_size * sizeof(Itemtype) / (1LLU << 20));
    //printf("num_refills = %llu\n", num_refills);
    //_aligned_free(sorted_num_buffer);

    this->num_seeks = nseeks;
    this->merge_write_duration = glb_write_duration;
    this->merge_populate_duration = glb_populate_duration;
    this->merge_read_duration = glb_read_duration;
    printf("    Merge done in %.2f ms, speed %.2f M keys/s\n", this->merge_duration * 1e3, this->file_size * 1.0 / 1e6 / this->merge_duration);
    printf("    Populate done in %.2f ms, speed %.2f M keys/s\n", this->merge_populate_duration * 1e3, this->file_size * 1.0 / 1e6 / this->merge_populate_duration);
    printf("    Read done in %.2f ms, speed %.2f M keys/s\n", this->merge_read_duration * 1e3, this->file_size * 1.0 / 1e6 / this->merge_read_duration);
    printf("    Write done in %.2f ms, speed %.2f M keys/s\n", this->merge_write_duration * 1e3, this->file_size * 1.0 / 1e6 / this->merge_write_duration);
    return 0;
}


void external_sort::print_metrics()
{
    if (this->debug) {
        printf("\n");
    }
    printf("file size = %f MB\n", this->file_size * sizeof(Itemtype) / 1e6);
    printf("Averages over %d runs\n", this->num_runs);
    printf("\n    Random File Generation Statistics\n");
    printf("    Generation time: %f s\n", this->total_generate_time / this->num_runs);
    printf("    Generation rate: %f million keys/s\n", this->file_size * this->num_runs / (this->total_generate_time * 1e6));
    printf("    Write time:      %f s\n", this->total_write_time / this->num_runs);
    printf("    Write rate:      %f MB/s\n", this->file_size * this->num_runs * sizeof(Itemtype) / (this->total_write_time * 1e6));
    printf("\n    Chunk Sort Statistics\n");
    printf("    Sort time:       %f s\n", this->total_sort_time / this->num_runs);
    printf("    Sort rate:       %f MB/s\n", this->file_size * this->num_runs * sizeof(Itemtype) / (this->total_sort_time * 1e6));
    printf("    Sort rate:       %f million keys/s\n", this->file_size * this->num_runs / (this->total_sort_time * 1e6));
    printf("    Read time:       %f s\n", this->total_read_time / this->num_runs);
    printf("    Read rate:       %f MB/s\n", this->file_size * this->num_runs * sizeof(Itemtype) / (this->total_read_time * 1e6));
    printf("\n    Merge Statistics\n");
    printf("    Merge time:      %f s\n", this->total_merge_time / this->num_runs);
    printf("    Merge rate:      %f MB/s\n", this->file_size * this->num_runs * sizeof(Itemtype) / (this->total_merge_time * 1e6));
    printf("    Load time:       %f s\n", this->total_load_time / this->num_runs);
    printf("    Load rate:       %f MB/s\n", static_cast<unsigned long long>((1 << 30)) * this->num_runs * sizeof(Itemtype) / (this->total_load_time * 1e6));
    printf("    Read time:       %f s\n", this->total_merge_read_time / this->num_runs);
    printf("    Read rate:       %f MB/s\n", this->file_size * this->num_runs * sizeof(Itemtype) / (this->total_merge_read_time * 1e6));
    printf("    Heap time:       %f s\n", this->total_heap_time / this->num_runs);
    printf("    Heap rate:       %f MB/s\n", this->file_size * this->num_runs * sizeof(Itemtype) / (this->total_heap_time * 1e6));
    printf("    Write time:      %f s\n", this->total_merge_write_time / this->num_runs);
    printf("    Write rate:      %f MB/s\n", this->file_size * this->num_runs * sizeof(Itemtype) / (this->total_merge_write_time * 1e6));
}


int external_sort::save_metrics(bool header, bool extra_space)
{
    ifstream check;
    string line = "";
    stringstream s;
    check.open(this->metrics_fname);
    if (header/*check.peek() == ifstream::traits_type::eof() || extra_space*/)
    {
        if (extra_space) { s << "\n"; }
        s << "Way,,";
        s << "File Size (MB),Memory Size (MB),Number of Seeks,Total Time (s),,Generation Time (s),Generation Rate (million keys/s),Write Time (s),Write Rate (MB/s),,";
        //s << "File Size (MB),Memory Size (MB),Number of Chunks,Total Time (s),,Generation Time (s),Generation Rate (million keys/s),Write Time (s),Write Rate (MB/s),,";
        s << "Sort Time (s),Sort Rate (MB/s),Sort Rate (million keys/s),Read Time (s),Read Rate (MB/s),Write Rate (MB/s),,";
        //s << "Merge Time (s),Merge Rate (MB/s),Load Time (s),Load Rate (MB/s),Read Time (s),Read Rate (MB/s),";
        s << "Merge Time (s),Merge Rate (MB/s),Read Rate (MB/s),Populate Rate (MB/s),";
        //s << "Heap Time (s),Heap Rate (MB/s),Write Time (s),Write Rate (MB/s),\n";
        s << "Write Rate (MB/s),\n";
    }
    check.close();
    ofstream ofile;
    ofile.open(this->metrics_fname, ios_base::app);
    if (ofile.is_open()) {
        s << orig_num_chunks << ",,";
        // file size and memory size
        s << this->file_size * sizeof(Itemtype) / (1LLU << 20) << "," << this->chunk_size * sizeof(Itemtype) / (1LLU << 20) << "," << this->num_seeks << "," << this->total_time / this->num_runs << ",,";
        //s << this->file_size * sizeof(Itemtype) / 1e6 << "," << this->chunk_size * sizeof(Itemtype) / 1e6 << "," << num_chunks << "," << this->total_time / this->num_runs << ",,";
        // generation time and rate
        s << this->total_generate_time / this->num_runs << "," << this->file_size * this->num_runs / (this->total_generate_time * (1LLU << 20)) << ",";
        // write time and rate
        s << this->total_write_time / this->num_runs << "," << this->file_size * this->num_runs * sizeof(Itemtype) / (this->total_write_time * (1LLU << 20)) << ",,";
        // sort time and rates
        s << this->total_sort_time / this->num_runs << "," << this->file_size * this->num_runs * sizeof(Itemtype) / (this->total_sort_time * (1LLU << 20)) << "," << this->file_size * this->num_runs / (this->total_sort_time * (1LLU << 20)) << ",";
        // sort read time and rate
        s << this->total_read_time / this->num_runs << "," << this->file_size * this->num_runs * sizeof(Itemtype) / (this->total_read_time * (1LLU << 20)) << ",";
        // sort write rate
        s << this->file_size * sizeof(Itemtype) / (this->sort_write_duration * (1LLU << 20)) << ",,";
        // merge time and rate
        s << this->total_merge_time / this->num_runs << "," << this->file_size * this->num_runs * sizeof(Itemtype) / (this->total_merge_time * (1LLU << 20)) << ",";
        // load time and rate
        //s << this->total_load_time / this->num_runs << "," << static_cast<unsigned long long>((1 << 30)) * this->num_runs * sizeof(Itemtype) / (this->total_load_time * 1e6) << ",";
        // read rate
        s << this->file_size * this->num_runs * sizeof(Itemtype) / (this->total_merge_read_time * (1LLU << 20)) << ",";
        // populate rate
        s << this->file_size * this->num_runs * sizeof(Itemtype) / (this->merge_populate_duration * (1LLU << 20)) << ",";
        // heap time and rate
        //s << this->total_heap_time / this->num_runs << "," << this->file_size * this->num_runs * sizeof(Itemtype) / (this->total_heap_time * 1e6) << ",";
        // write time and rate
        //s << this->total_merge_write_time / this->num_runs << "," << this->file_size * this->num_runs * sizeof(Itemtype) / (this->total_merge_write_time * 1e6) << ",\n";
        // rate
        s << this->file_size * this->num_runs * sizeof(Itemtype) / (this->total_merge_write_time * 1e6) << ",\n";
        if (extra_space) {
            s << "\n";
        }
        line = s.str();
        ofile << line;
    }
    ofile.close();
    return 0;
}


int external_sort::generate_averages()
{
    LARGE_INTEGER start = { 0 }, end = { 0 }, freq = { 0 };
    QueryPerformanceFrequency(&freq);
    for (int i = 0; i < this->num_runs; i++)
    {
        QueryPerformanceCounter(&start);
        write_file();
        if (this->debug)
        {
            printf("Number of bytes written = %llu\n", this->number_elements_touched);
        }
        this->total_generate_time += this->generation_duration;
        this->total_write_time += this->write_duration;
        if (this->test_sort)
        {
            sort_file();
            this->total_sort_time += this->sort_duration;
            this->total_read_time += this->read_duration;
        }
        merge_sort();
        QueryPerformanceCounter(&end);
        this->total_time += (static_cast<double>(end.QuadPart) - start.QuadPart) / freq.QuadPart;
        this->total_merge_time += this->merge_duration;
        this->total_load_time += this->load_duration;
        this->total_merge_read_time += this->merge_read_duration;
        this->total_heap_time += this->heap_duration;
        this->total_merge_write_time += this->merge_write_duration;

        unsigned s = free_blocks.size();
        for (unsigned i = 0; i < s; i++)
        {
            _aligned_free(free_blocks.front());
            free_blocks.pop();
        }
        state.clear();
    }
    return 0;
}


int external_sort::shallow_validate()
{
    printf("\n%s\n", __FUNCTION__);
    //#define DEBUG_SHALLOW_PRINT
    LARGE_INTEGER num_bytes_written = { 0 }, random_sorted_size = { 0 }, chunk_sorted_size = { 0 }, merge_sorted_size = { 0 };

    //unsigned int* write_buffer = (unsigned int*)_aligned_malloc(static_cast<size_t>(this->write_buffer_size) * sizeof(Itemtype), this->bytes_per_sector);
    Itemtype* val_buffer = (Itemtype*)_aligned_malloc(static_cast<size_t>(this->write_buffer_size) * sizeof(Itemtype), this->bytes_per_sector);
    unsigned long long int written = 0, number_read = 0;
    double sort_duration = 0, read_duration = 0;
    HANDLE file = nullptr, cfile = nullptr, rfile = nullptr;

    if (this->seq_run) {
        file = CreateFile(this->full_sorted_fname, GENERIC_READ | GENERIC_WRITE,
            FILE_SHARE_READ | FILE_SHARE_DELETE | FILE_SHARE_WRITE,
            NULL,
            OPEN_EXISTING,
            FILE_ATTRIBUTE_NORMAL,
            NULL);


        DWORD num_moved = 0;
        LARGE_INTEGER new_fp = { 0 };
        new_fp.QuadPart = 2LLU * this->windows_fs.QuadPart;
        num_moved = SetFilePointer(file, new_fp.LowPart, &new_fp.HighPart, FILE_BEGIN);
        if (num_moved == INVALID_SET_FILE_POINTER) {
            printf("%s: error in SetFilePointer for full sorted with %d\n", __FUNCTION__, GetLastError());
            exit(1);
        }

        unsigned int last_val = 0;
        while (number_read < this->file_size) {
#ifdef DEBUG_SHALLOW_PRINT
            printf("number_read = %lu\n", number_read);
#endif         
            unsigned long num_vals_to_read = 0;
            unsigned long new_num_vals_to_read = 0;
            if (number_read + this->write_buffer_size > this->file_size) {
                if (number_read) {
                    num_vals_to_read = this->file_size % number_read;
                }
                else {
                    num_vals_to_read = this->file_size;
                }
                new_num_vals_to_read = (num_vals_to_read + 127) & (~127);
            }
            else {
                num_vals_to_read = this->write_buffer_size;
                new_num_vals_to_read = this->write_buffer_size;
            }
#ifdef DEBUG_SHALLOW_PRINT
            printf("    num_vals_to_read = %lu\n", num_vals_to_read);
            printf("    new_num_vals_to_read = %lu\n", new_num_vals_to_read);
            printf("    this->chunk_size = %lu\n", this->chunk_size);
            printf("    this->file_size = %lu\n", this->file_size);
            printf("    this->write_buffer_size = %lu\n", this->write_buffer_size);
#endif  
            DWORD num_bytes_touched;
            bool was_success = ReadFile(file, val_buffer, sizeof(Itemtype) * new_num_vals_to_read, &num_bytes_touched, NULL);
            if (!(was_success)) {
                printf("%s: Failed reading from populated file with %d\n", __FUNCTION__, GetLastError());
                exit(1);
            }
            number_read += num_vals_to_read;
#ifdef DEBUG_SHALLOW_PRINT
            printf("    number_read after read = %llu\n", number_read);
#endif

            for (int i = 0; i < num_vals_to_read; i++) {
                if (val_buffer[i] < last_val) {
                    printf("%s: Merged file out of order: %u > %u at i = %d (num_read = %llu vals, %llu MB)\n", __FUNCTION__, val_buffer[i], last_val, i, number_read, number_read * sizeof(Itemtype) / (1LLU << 20));
                    printf("    this->file_size = %llu\n", this->file_size);
                    printf("    this->windows_fs = %llu\n", this->windows_fs.QuadPart);
                    printf("    num_vals_to_read = %llu\n", num_vals_to_read);
                    printf("    new_num_vals_to_read = %llu\n", new_num_vals_to_read);
                    printf("    num_bytes_touched = %llu\n", num_bytes_touched);
                    printf("    val_buffer[0] = %llu\n", val_buffer[0]);
                    printf("    val_buffer[1] = %llu\n", val_buffer[1]);
                    printf("    val_buffer[2] = %llu\n", val_buffer[2]);
                    printf("    val_buffer[3] = %llu\n", val_buffer[3]);
                    printf("    val_buffer[4] = %llu\n", val_buffer[4]);
                    printf("    val_buffer[%d] = %llu\n", (std::max)(0, i - 1), val_buffer[(std::max)(0, i - 1)]);
                    printf("    val_buffer[%d] = %llu\n", i, val_buffer[i]);
                    printf("    val_buffer[%d] = %llu\n", i + 1, val_buffer[i + 1]);
                    exit(1);
                }
                last_val = val_buffer[i];
            }
        }
        _aligned_free(val_buffer);
        val_buffer = nullptr;
        CloseHandle(file);
    }
    else {
        file = CreateFile(this->full_sorted_fname, GENERIC_READ | GENERIC_WRITE,
            FILE_SHARE_READ | FILE_SHARE_DELETE | FILE_SHARE_WRITE,
            NULL,
            OPEN_EXISTING,
            FILE_ATTRIBUTE_NORMAL,
            NULL);
        cfile = CreateFile(this->chunk_sorted_fname, GENERIC_READ | GENERIC_WRITE,
            FILE_SHARE_READ | FILE_SHARE_DELETE | FILE_SHARE_WRITE,
            NULL,
            OPEN_EXISTING,
            FILE_ATTRIBUTE_NORMAL,
            NULL);
        rfile = CreateFile(this->fname, GENERIC_READ | GENERIC_WRITE,
            FILE_SHARE_READ | FILE_SHARE_DELETE | FILE_SHARE_WRITE,
            NULL,
            OPEN_EXISTING,
            FILE_ATTRIBUTE_NORMAL,
            NULL);

        if (file == INVALID_HANDLE_VALUE) {
            printf("%s: Failed opening merged file with %d\n", __FUNCTION__, GetLastError());
            exit(1);
        }
        if (cfile == INVALID_HANDLE_VALUE) {
            printf("%s: Failed opening chunk file with %d\n", __FUNCTION__, GetLastError());
            exit(1);
        }
        if (rfile == INVALID_HANDLE_VALUE) {
            printf("%s: Failed opening random file with %d\n", __FUNCTION__, GetLastError());
            exit(1);
        }

        int retval = 0;
        retval = GetFileSizeEx(file, &random_sorted_size);
        if (retval == 0)
        {
            printf("%s: Failed getting file size for random file with %d\n", __FUNCTION__, GetLastError());
            exit(1);
        }
        retval = GetFileSizeEx(cfile, &chunk_sorted_size);
        if (retval == 0)
        {
            printf("%s: Failed getting file size for chunk file with %d\n", __FUNCTION__, GetLastError());
            exit(1);
        }
        retval = GetFileSizeEx(file, &merge_sorted_size);
        if (retval == 0)
        {
            printf("%s: Failed getting file size for merged file with %d\n", __FUNCTION__, GetLastError());
            exit(1);
        }

        if (random_sorted_size.QuadPart != chunk_sorted_size.QuadPart)
        {
            printf("%s: Random and chunk file sizes aren't equal\n", __FUNCTION__);
            exit(1);
        }
        if (random_sorted_size.QuadPart != merge_sorted_size.QuadPart)
        {
            printf("%s: Random and merged file sizes aren't equal\n", __FUNCTION__);
            exit(1);
        }
        if (merge_sorted_size.QuadPart != chunk_sorted_size.QuadPart)
        {
            printf("%s: Merged and chunk file sizes aren't equal\n", __FUNCTION__);
            exit(1);
        }
        CloseHandle(cfile);
        CloseHandle(rfile);

        unsigned int last_val = 0;
        while (number_read < this->file_size) {
            if (this->debug) {
                printf("number_read = %lu\n", number_read);
            }
            unsigned long num_vals_to_read = 0;
            unsigned long new_num_vals_to_read = 0;
            if (number_read + this->write_buffer_size > this->file_size) {
                if (number_read) {
                    num_vals_to_read = this->file_size % number_read;
                }
                else {
                    num_vals_to_read = this->file_size;
                }
                new_num_vals_to_read = (num_vals_to_read + 127) & (~127);
            }
            else {
                num_vals_to_read = this->write_buffer_size;
                new_num_vals_to_read = this->write_buffer_size;
            }
            if (this->debug) {
                printf("    num_vals_to_read = %lu\n", num_vals_to_read);
                printf("    new_num_vals_to_read = %lu\n", new_num_vals_to_read);
                printf("    this->chunk_size = %lu\n", this->chunk_size);
                printf("    this->file_size = %lu\n", this->file_size);
                printf("    this->write_buffer_size = %lu\n", this->write_buffer_size);

            }
            DWORD num_bytes_touched;
            bool was_success = ReadFile(file, val_buffer, sizeof(Itemtype) * new_num_vals_to_read, &num_bytes_touched, NULL);
            if (!(was_success)) {
                printf("%s: Failed reading from populated file with %d\n", __FUNCTION__, GetLastError());
                exit(1);
            }
            number_read += num_vals_to_read;
            if (this->debug) {
                printf("    number_read after read = %llu\n", number_read);
            }

            for (int i = 0; i < num_vals_to_read; i++) {
                if (val_buffer[i] < last_val) {
                    printf("%s: Merged file out of order: %u > %u at i = %d\n", __FUNCTION__, val_buffer[i], last_val, i);
                    exit(1);
                }
                last_val = val_buffer[i];
            }
        }


        _aligned_free(val_buffer);
        val_buffer = nullptr;
        CloseHandle(file);
    }

    printf("\n%s: Merged file in order\n", __FUNCTION__);
    return 0;
}


int external_sort::deep_validate()
{
    printf("\n%s\n", __FUNCTION__);

    unordered_set<unsigned int>* original = new unordered_set<unsigned int>;
    unordered_set<unsigned int>* chunk_sorted = new unordered_set<unsigned int>;
    unordered_set<unsigned int>* merge_sorted = new unordered_set<unsigned int>;


    LARGE_INTEGER num_bytes_written = { 0 };

    Itemtype* val_buffer = (Itemtype*)_aligned_malloc(static_cast<size_t>(this->write_buffer_size) * sizeof(Itemtype), this->bytes_per_sector);
    unsigned long long int written = 0, number_read = 0;
    double sort_duration = 0, read_duration = 0;


    HANDLE foriginal = CreateFile(this->fname, GENERIC_READ | GENERIC_WRITE,
        FILE_SHARE_READ | FILE_SHARE_DELETE | FILE_SHARE_WRITE,
        NULL,
        OPEN_EXISTING,
        FILE_ATTRIBUTE_NORMAL | FILE_FLAG_NO_BUFFERING,
        NULL);
    HANDLE fchunk_sorted = CreateFile(this->chunk_sorted_fname, GENERIC_READ | GENERIC_WRITE,
        FILE_SHARE_READ | FILE_SHARE_DELETE | FILE_SHARE_WRITE,
        NULL,
        OPEN_EXISTING,
        FILE_ATTRIBUTE_NORMAL | FILE_FLAG_NO_BUFFERING,
        NULL);
    HANDLE fmerge_sorted = CreateFile(this->full_sorted_fname, GENERIC_READ | GENERIC_WRITE,
        FILE_SHARE_READ | FILE_SHARE_DELETE | FILE_SHARE_WRITE,
        NULL,
        OPEN_EXISTING,
        FILE_ATTRIBUTE_NORMAL | FILE_FLAG_NO_BUFFERING,
        NULL);


    if (foriginal == INVALID_HANDLE_VALUE) {
        printf("%s: Failed opening new file foriginal for sort output with %d\n", __FUNCTION__, GetLastError());
        exit(1);
    }
    if (fchunk_sorted == INVALID_HANDLE_VALUE) {
        printf("%s: Failed opening new file fchunk_sorted for sort output with %d\n", __FUNCTION__, GetLastError());
        exit(1);
    }
    if (fmerge_sorted == INVALID_HANDLE_VALUE) {
        printf("%s: Failed opening new file fmerge_sorted for sort output with %d\n", __FUNCTION__, GetLastError());
        exit(1);
    }
    LARGE_INTEGER new_fp = { 0 };
    new_fp.QuadPart = 2 * this->windows_fs.QuadPart;
    if (this->seq_run) {
        DWORD num_moved = 0;
        num_moved = SetFilePointer(fchunk_sorted, this->windows_fs.LowPart, &this->windows_fs.HighPart, FILE_BEGIN);
        if (num_moved == INVALID_SET_FILE_POINTER) {
            printf("%s: error in SetFilePointer for chunk sorted with %d\n", __FUNCTION__, GetLastError());
            exit(1);
        }

        num_moved = SetFilePointer(fmerge_sorted, new_fp.LowPart, &new_fp.HighPart, FILE_BEGIN);
        if (num_moved == INVALID_SET_FILE_POINTER) {
            printf("%s: error in SetFilePointer for full sorted with %d\n", __FUNCTION__, GetLastError());
            exit(1);
        }
    }


    while (number_read < this->file_size) {
        if (this->debug) {
            printf("number_read = %lu\n", number_read);
        }

        unsigned long long num_vals_to_read = 0;
        unsigned long long new_num_vals_to_read = 0;
        if (number_read + this->write_buffer_size > this->file_size) {
            if (number_read) {
                num_vals_to_read = this->file_size % number_read;
            }
            else {
                num_vals_to_read = this->file_size;
            }
            new_num_vals_to_read = (num_vals_to_read + 127) & (~127);
        }
        else {
            num_vals_to_read = this->write_buffer_size;
            new_num_vals_to_read = this->write_buffer_size;
        }
        if (this->debug) {
            printf("    num_vals_to_read = %lu\n", num_vals_to_read);
            printf("    new_num_vals_to_read = %lu\n", new_num_vals_to_read);
            printf("    this->chunk_size = %lu\n", this->chunk_size);
            printf("    this->file_size = %lu\n", this->file_size);
        }

        // original file
        DWORD num_bytes_touched;
        bool was_success = ReadFile(foriginal, val_buffer, sizeof(Itemtype) * new_num_vals_to_read, &num_bytes_touched, NULL);
        if (!(was_success)) {
            printf("%s: Failed reading from foriginal with %d\n", __FUNCTION__, GetLastError());
            exit(1);
        }

        for (int i = 0; i < num_vals_to_read; i++) {
            original->insert(val_buffer[i]);
        }

        // chunk sorted file
        was_success = ReadFile(fchunk_sorted, val_buffer, sizeof(Itemtype) * new_num_vals_to_read, &num_bytes_touched, NULL);
        if (!(was_success)) {
            printf("%s: Failed reading from fchunk_sorted with %d\n", __FUNCTION__, GetLastError());
            exit(1);
        }

        for (int i = 0; i < num_vals_to_read; i++) {
            chunk_sorted->insert(val_buffer[i]);
        }

        // merge sorted file
        was_success = ReadFile(fmerge_sorted, val_buffer, sizeof(Itemtype) * new_num_vals_to_read, &num_bytes_touched, NULL);
        if (!(was_success)) {
            printf("%s: Failed reading from fmerge_sorted with %d\n", __FUNCTION__, GetLastError());
            exit(1);
        }

        number_read += num_vals_to_read;
        if (this->debug) {
            printf("    number_read after read = %llu\n", number_read);
        }

        for (int i = 0; i < num_vals_to_read; i++) {
            merge_sorted->insert(val_buffer[i]);
        }
    }


    _aligned_free(val_buffer);
    val_buffer = nullptr;

    CloseHandle(foriginal);
    CloseHandle(fchunk_sorted);
    CloseHandle(fmerge_sorted);

    if (*original != *chunk_sorted) {
        printf("%s: original and chunk_sorted contain different values\n", __FUNCTION__);
        printf("    original.size() = %d\n", original->size());
        printf("    chunk_sorted.size() = %d\n", chunk_sorted->size());
        exit(1);
    }
    else if (*original != *merge_sorted) {
        printf("%s: original and merge_sorted contain different values\n", __FUNCTION__);
        printf("    original.size() = %d\n", original->size());
        printf("    merge_sorted.size() = %d\n", merge_sorted->size());
        exit(1);
    }
    else if (*chunk_sorted != *merge_sorted) {
        printf("%s: merge_sorted and chunk_sorted contain different values\n", __FUNCTION__);
        printf("    chunk_sorted.size() = %d\n", chunk_sorted->size());
        printf("    merge_sorted.size() = %d\n", merge_sorted->size());
        exit(1);
    }

    delete original;
    delete chunk_sorted;
    delete merge_sorted;

    printf("\n%s: All files contain the same values\n", __FUNCTION__);

    return 0;
}
