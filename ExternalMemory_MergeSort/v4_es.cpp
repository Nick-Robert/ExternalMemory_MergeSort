/*

THIS VERSION HAS VARIABLE CHUNK SIZES IN MEMORY (CALLED PORTIONS) AND UTILIZES ORIGAMI SORT AND MINHEAP MERGE SORT
    (has queues of blocks of 1MB memory of Itemtype)

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
    wbuffer[i + k] = next##k;            \


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
    this->write_buffer_size = (static_cast<unsigned long long>(1) << 20) / sizeof(Itemtype);
    this->block_size = static_cast<unsigned long long>(1) << 20;

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

    MEMORYSTATUSEX statex = { 0 };
    statex.dwLength = sizeof(statex);
    GlobalMemoryStatusEx(&statex);

    printf("    There is currently %llu KB of free memory available\n", statex.ullAvailPhys / 1024);
    unsigned long long mem_avail = 0.7 * statex.ullAvailPhys;
    // divided by 2 since Origami is an out-of-place sorter
    mem_avail = ((mem_avail + 511) & (~511)) / 2;
    mem_avail = static_cast<unsigned long long>(1) << (unsigned)log2(mem_avail);
    this->mem_avail = mem_avail;
    printf("        External sort will use %llu B (%llu vals) of memory\n", mem_avail, mem_avail / sizeof(Itemtype));
    assert(mem_avail % sizeof(Itemtype) == 0);
    this->chunk_size = mem_avail / sizeof(Itemtype);

    BOOL succeeded = GetDiskFreeSpaceA(NULL, NULL, &this->bytes_per_sector, NULL, NULL);
    if (!succeeded) {
        printf("%s: Failed getting disk information with %d\n", __FUNCTION__, GetLastError());
    }
}

//external_sort::~external_sort() 
//{
//    //delete[] this->state;
//}


int external_sort::write_file()
{
    printf("\n%s\n", __FUNCTION__);
    srand((unsigned int)time(0));
    //srand(0);
    LARGE_INTEGER start = { 0 }, end = { 0 }, freq = { 0 };

    QueryPerformanceFrequency(&freq);
    QueryPerformanceCounter(&start);

    unsigned long long int number_written = 0;

    Itemtype* wbuffer = (Itemtype*)_aligned_malloc(static_cast<size_t>(this->write_buffer_size) * sizeof(Itemtype), this->bytes_per_sector);
    double generation_duration = 0, write_duration = 0;

    HANDLE pfile = CreateFile(this->fname, GENERIC_WRITE, 0, 0, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL | FILE_FLAG_NO_BUFFERING, NULL);
    if (pfile == INVALID_HANDLE_VALUE) {
        printf("__FUNCTION__write_file(): Failed opening file with %d\n", GetLastError());
        exit(1);
    }
    else {
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
            /*for (unsigned int i = 0; i < 2 && i < num_vals_to_gen; i++) {
                printf("buffer[%d] = %llu\n", i, wbuffer[i]);
            }*/
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
            QueryPerformanceCounter(&start);
            BOOL was_success = WriteFile(pfile, wbuffer, sizeof(Itemtype) * new_num_vals_to_write, &num_bytes_written, NULL);
            QueryPerformanceCounter(&end);
            if (!(was_success)) {
                printf("%s: Failed writing to file with %d\n", __FUNCTION__, GetLastError());
                exit(1);
            }

            write_duration += end.QuadPart - start.QuadPart;
            number_written += num_vals_to_write;

            if (this->debug) {
                printf("    number_written = %llu\n", number_written);
                printf("    bufsize = %d\n", this->write_buffer_size);
                printf("    windows_fs.QuadPart = %llu\n", this->windows_fs.QuadPart);
                printf("    windows_fs.HighPart = %llu\n", this->windows_fs.HighPart);
                printf("    windows_fs.LowPart = %llu\n", this->windows_fs.LowPart);
            }
        }
    }
    CloseHandle(pfile);
    LARGE_INTEGER before_sfp = { 0 };
    before_sfp.QuadPart = this->windows_fs.QuadPart;
    pfile = CreateFile(this->fname, GENERIC_WRITE, 0, 0, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);
    if ((SetFilePointer(pfile, this->windows_fs.LowPart, &this->windows_fs.HighPart, FILE_BEGIN)) == INVALID_SET_FILE_POINTER) {
        printf("%s: Error with SetFilePointer with %d\n", __FUNCTION__, GetLastError());
        exit(1);
    }
    if (!SetEndOfFile(pfile)) {
        printf("%s: Error with SetEndOfFile with %d\n", __FUNCTION__, GetLastError());
        exit(1);
    }
    CloseHandle(pfile);
    this->windows_fs.QuadPart = before_sfp.QuadPart;

    _aligned_free(wbuffer);
    wbuffer = nullptr;

    this->number_elements_touched = number_written;
    this->generation_duration = generation_duration / freq.QuadPart;
    this->write_duration = write_duration / freq.QuadPart;
    return 0;
}

unsigned long read_into_buffer(HANDLE* fp, Itemtype* buf, unsigned long long tot_bytes)
{
    HANDLE f = *fp;
    DWORD num_bytes_touched;
    //unsigned long long tot_bytes = sizeof(Itemtype) * new_num_vals_to_read;
    unsigned long long num_loops = 0;
    // - 511 since it needs to be divisible by 512
    unsigned num_to_read = UINT_MAX - 511;
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

int external_sort::sort_file()
{
    printf("\n%s\n", __FUNCTION__);
    LARGE_INTEGER start = { 0 }, end = { 0 }, freq = { 0 }, num_bytes_written = { 0 };

    unsigned long long oos_size = this->chunk_size;

    unsigned long long int written = 0, number_read = 0;
    double sort_duration = 0, read_duration = 0;

    QueryPerformanceFrequency(&freq);

    HANDLE old_file = CreateFile(this->fname, GENERIC_READ, 0, 0, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL | FILE_FLAG_NO_BUFFERING, NULL);
    HANDLE chunk_sorted_file = CreateFile(this->chunk_sorted_fname, GENERIC_WRITE, 0, 0, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL | FILE_FLAG_NO_BUFFERING, NULL);
    if (old_file == INVALID_HANDLE_VALUE) {
        printf("%s: Failed opening populated file with %d\n", __FUNCTION__, GetLastError());
        exit(1);
    }
    else if (chunk_sorted_file == INVALID_HANDLE_VALUE) {
        printf("%s: Failed opening new file for sort output with %d\n", __FUNCTION__, GetLastError());
        exit(1);
    }
    Itemtype* sort_buffer = (Itemtype*)_aligned_malloc(static_cast<size_t>(oos_size) * sizeof(Itemtype), this->bytes_per_sector);
    while (number_read < this->file_size) {
        if (this->debug) {
            printf("number_read = %lu\n", number_read);
        }
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
        if (this->debug) {
            printf("    num_vals_to_read = %lu\n", num_vals_to_read);
            printf("    new_num_vals_to_read = %lu\n", new_num_vals_to_read);
            printf("    this->chunk_size = %lu\n", this->chunk_size);
            printf("    oos_size = %lu\n", oos_size);
        }
        unsigned long long tot_bytes = sizeof(Itemtype) * new_num_vals_to_read;
        QueryPerformanceCounter(&start);
        read_into_buffer(&old_file, sort_buffer, tot_bytes);
        QueryPerformanceCounter(&end);

        read_duration += end.QuadPart - start.QuadPart;
        number_read += num_vals_to_read;
        if (this->debug) {
            printf("    number_read after read = %llu\n", number_read);
        }
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
        //Itemtype* o = sort_buffer;
        /*printf("    num_vals_to_read = %lu\n", num_vals_to_read);
        printf("    sort_buffer = %llu\n", sort_buffer);
        printf("    o = %llu\n", o);
        printf("    sort_buffer_end = %llu\n", sort_buffer_end);*/
        //printf("        num_vals_to_read = %lu\n", num_vals_to_read);
        /*printf("        this->chunk_size = %lu\n", this->chunk_size);
        printf("        sizeof(Itemtype) = %lu\n", sizeof(Itemtype));*/

        if (num_vals_to_read < (static_cast<unsigned long long>(1) << 20) / sizeof(Itemtype)) {
            //printf("    quicksort\n");
            QueryPerformanceCounter(&start);
            std::sort(sort_buffer, sort_buffer + num_vals_to_read);
            QueryPerformanceCounter(&end);
        }
        else {
            //printf("    origami\n");
            QueryPerformanceCounter(&start);
            o = origami_sorter::sort_single_thread<Itemtype, Regtype>(sort_buffer, output, sort_buffer_end, num_vals_to_read, 2, nullptr);
            QueryPerformanceCounter(&end);
        }
        //printf("buffer[%d] = %u\n", 0, o[0]);

        if (this->give_vals) {
            for (unsigned int i = 0; i < 2 && i < num_vals_to_read; i++) {
                printf("buffer[%d] = %u\n", i, o[i]);
                printf("o[%d] = %u\n", i, o[i]);
            }
            if (num_vals_to_read > 6) {
                printf("buffer[%d] = %llu\n", num_vals_to_read / 2 - 3, o[num_vals_to_read / 2 - 3]);
                printf("buffer[%d] = %llu\n", num_vals_to_read / 2 - 2, o[num_vals_to_read / 2 - 2]);
                printf("buffer[%d] = %llu\n", num_vals_to_read / 2 - 1, o[num_vals_to_read / 2 - 1]);
                printf("buffer[%d] = %llu\n", num_vals_to_read / 2, o[num_vals_to_read / 2]);
                printf("buffer[%d] = %llu\n", num_vals_to_read / 2 + 1, o[num_vals_to_read / 2 + 1]);
                printf("buffer[%d] = %llu\n", num_vals_to_read - 2, o[num_vals_to_read - 2]);
                printf("buffer[%d] = %llu\n", num_vals_to_read - 1, o[num_vals_to_read - 1]);
            }
            /*for (unsigned int i = num_vals_to_read - 2; i < num_vals_to_read && i >= 0; i++) {
                printf("buffer[%d] = %u\n", i, sort_buffer[i]);
            }*/
        }
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
        while (written < number_read) {
            if (this->debug) {
                printf("    written = %lu\n", written);
                printf("    number_read = %lu\n", number_read);
            }
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
            /*printf("    num_vals_to_write = %lu\n", num_vals_to_write);
            printf("    new_num_vals_to_write = %lu\n", new_num_vals_to_write);
            printf("        *(sort_buffer + written) = %u\n", *(sort_buffer + (unsigned int)written));
            printf("        *(sort_buffer) = %u\n", *(sort_buffer));
            printf("        *(sort_buffer + 1) = %u\n", *(sort_buffer + 1));
            printf("        buffer[0] = %u\n", sort_buffer[0]);
            printf("            sort_buffer = %llu\n", sort_buffer);
            printf("            sort_buffer + written = %llu\n", sort_buffer + written);
            printf("            &buffer[0] = %llu\n", &sort_buffer[0]);
            printf("            &buffer[1] = %llu\n", &sort_buffer[1]);
            printf("            &buffer[1] - &buffer[0] = %llu\n", &sort_buffer[1] - &sort_buffer[0]);*/

            DWORD num_bytes_touched;
            bool was_success = WriteFile(chunk_sorted_file, o + loop_written, sizeof(Itemtype) * new_num_vals_to_write, &num_bytes_touched, NULL);
            if (!(was_success)) {
                printf("%s: Failed writing to new file for sort output with %d\n", __FUNCTION__, GetLastError());
                exit(1);
            }
            /*printf("buffer[%d] = %llu\n", loop_written, o[loop_written]);
            printf("    num_bytes_touched in write = %d\n", num_bytes_touched);*/

            if (num_vals_to_write != new_num_vals_to_write) {
                CloseHandle(chunk_sorted_file);
                chunk_sorted_file = CreateFile(this->chunk_sorted_fname, GENERIC_WRITE, 0, 0, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);
                //LARGE_INTEGER before_sfp = { 0 };
                //before_sfp.QuadPart = this->windows_fs.QuadPart;
                DWORD num_moved = 0;
                num_moved = SetFilePointer(chunk_sorted_file, this->windows_fs.LowPart, NULL/*&this->windows_fs.HighPart*/, FILE_BEGIN);
                if (num_moved == INVALID_SET_FILE_POINTER) {
                    printf("%s: error in SetFilePointer with %d\n", __FUNCTION__, GetLastError());
                    exit(1);
                }
                if (!SetEndOfFile(chunk_sorted_file)) {
                    printf("%s: error in SetEndOfFile with %d\n", __FUNCTION__, GetLastError());
                    exit(1);
                }
                //windows_fs.QuadPart = before_sfp.QuadPart;
            }


            num_bytes_written.QuadPart += num_bytes_touched;
            written += num_vals_to_write;
            loop_written += num_vals_to_write;
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
    return 0;
}

uint64_t external_sort::populate_blocks(unsigned idx, unsigned long long* remaining_vals, Itemtype* rbuff)
{
    unsigned long long buf_offset = 0;
    uint64_t num_vals_last_block = 0;
    for (int j = 0; j < this->state[idx].num_blocks; j++) {
        unsigned long long vals_to_copy = (std::min)(*remaining_vals, this->block_size / sizeof(Itemtype));

        if (j == this->state[idx].num_blocks - 1)
        {
            num_vals_last_block = vals_to_copy;
        }

        if (this->free_blocks.size() == 0)
        {
            //printf("    free block queue is 0 with num_blocks = %u and j = %u\n", this->state[idx].num_blocks, j);
            Itemtype* temp = (Itemtype*)_aligned_malloc(this->block_size, this->bytes_per_sector);
            this->free_blocks.push(temp);
        }

        //Itemtype* temp = (Itemtype*)_aligned_malloc(this->block_size, this->bytes_per_sector);
        Itemtype* temp = this->free_blocks.front();
        this->free_blocks.pop();
        memcpy(temp, rbuff + buf_offset, vals_to_copy * sizeof(Itemtype));
        this->state[idx].bq.push(temp);
        buf_offset += vals_to_copy;
        *remaining_vals -= vals_to_copy;
    }
    return num_vals_last_block;
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
    printf("\n%s\n", __FUNCTION__);
    unsigned long long num_chunks = (this->file_size % this->chunk_size == 0) ? (this->file_size / this->chunk_size) : ((this->file_size / this->chunk_size) + 1);

    LARGE_INTEGER start = { 0 }, end = { 0 }, merge_start = { 0 }, merge_end = { 0 }, freq = { 0 }, num_bytes_written = { 0 };
    unsigned long long int written = 0, number_read = 0;
    double load_duration = 0, read_duration = 0, heap_duration = 0, write_duration = 0, merge_duration = 0;


    if (this->debug) {
        printf("    file_size = %llu\n", this->file_size);
        printf("    file_size * sizeof(Itemtype) = %llu\n", this->file_size * sizeof(Itemtype));
        printf("    num_chunks = %llu\n", num_chunks);
        printf("    chunk_size / num_chunks = %llu\n", this->chunk_size / num_chunks);
        printf("    chunk_size = %llu\n", this->chunk_size);
    }

    HANDLE chunk_sorted_file = CreateFile(this->chunk_sorted_fname, GENERIC_READ, 0, 0, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL | FILE_FLAG_NO_BUFFERING, NULL);
    HANDLE full_sorted_file = CreateFile(this->full_sorted_fname, GENERIC_WRITE, 0, 0, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL | FILE_FLAG_NO_BUFFERING, NULL);
    if (chunk_sorted_file == INVALID_HANDLE_VALUE) {
        printf("%s: Failed opening populated file with %d\n", __FUNCTION__, GetLastError());
        exit(1);
    }
    else if (full_sorted_file == INVALID_HANDLE_VALUE) {
        printf("%s: Failed opening new file for mergesort output with %d\n", __FUNCTION__, GetLastError());
        exit(1);
    }

    unsigned long long total_bytes_touched = 0;
    DWORD num_bytes_touched;

    unsigned long long delta = 0;
    unsigned long long largest_chunk = 0;
    delta = (2 * (this->mem_avail / sizeof(Itemtype)) / (num_chunks * (num_chunks + 1)));
    largest_chunk = num_chunks * delta;

    printf("    Number of chunks  = %llu\n", num_chunks);
    printf("       delta          = %llu vals\n", delta);
    printf("       largest_chunk  = %llu vals\n", largest_chunk);


    unsigned long long tot_bytes_from_delta = 0;
    for (unsigned int i = 0; i < num_chunks; i++)
    {
        tot_bytes_from_delta += (static_cast<unsigned long long>(i) + 1) * delta;
    }

    Itemtype* sorted_num_buffer = (Itemtype*)_aligned_malloc(this->write_buffer_size * sizeof(Itemtype), this->bytes_per_sector);
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

        // start of the whole chunk in the file
        new_chunk.start_offset = running_file_offset * sizeof(Itemtype);

        // end of the whole chunk in the file (equivalent to the start of the next chunk, if it exists)
        new_chunk.end_offset = (running_file_offset + new_chunk.chunk_size) * sizeof(Itemtype);

        // how big a portion this chunk gets in memory (not necessarilly contiguous)
        new_chunk.bufsize = (std::min)((INT64)(delta * (static_cast<unsigned long long>(i) + 1)), (INT64)((new_chunk.end_offset - new_chunk.start_offset) / sizeof(Itemtype)));
        // next place in the file to start the next seek from the start_offset
        new_chunk.seek_offset = (std::min)(new_chunk.bufsize * sizeof(Itemtype), new_chunk.end_offset - new_chunk.start_offset);

        new_chunk.nobuff_bufsize = (static_cast<INT64>(new_chunk.bufsize) + 127) & (~127);

        // each chunk's portion of the memory is made out of blocks, which are 1 MB sizes of memory linked together in a queue
        new_chunk.num_blocks = (new_chunk.bufsize * sizeof(Itemtype) % (this->block_size) == 0) ? (new_chunk.bufsize * sizeof(Itemtype) / (this->block_size)) : (new_chunk.bufsize * sizeof(Itemtype) / (this->block_size) + 1);

        for (unsigned i = 0; i < new_chunk.num_blocks; i++)
        {
            Itemtype* temp = (Itemtype*)_aligned_malloc(this->block_size, this->bytes_per_sector);
            this->free_blocks.push(temp);
        }

        running_file_offset += new_chunk.chunk_size;

        if (this->debug) {
            printf("\ni = %d\n", i);
            new_chunk.print();
        }
        this->state.push_back(new_chunk);

    }

    LARGE_INTEGER num_bytes_to_move = { 0 };
    QueryPerformanceFrequency(&freq);
    QueryPerformanceCounter(&start);
    for (unsigned i = 0; i < num_chunks; i++)
    {
        Itemtype* rbuff = (Itemtype*)_aligned_malloc(this->state[i].nobuff_bufsize * sizeof(Itemtype), this->bytes_per_sector);
        num_bytes_to_move.QuadPart = this->state[i].start_offset;
        LARGE_INTEGER aligned_bytes_to_move = { 0 };
        aligned_bytes_to_move.QuadPart = (num_bytes_to_move.QuadPart + 511) & (~511);
        if (num_bytes_to_move.QuadPart % 512 != 0) {
            aligned_bytes_to_move.QuadPart -= 512;
        }
        if (this->debug) {
            printf("  i = %d\n", i);
            printf("    num_bytes_to_move.QuadPart = %llu\n", num_bytes_to_move.QuadPart);
            printf("    aligned_bytes_to_move.QuadPart = %llu\n", aligned_bytes_to_move.QuadPart);
            printf("    this->state[i].nobuff_bufsize = %llu\n", this->state[i].nobuff_bufsize);
        }
        QueryPerformanceCounter(&end);
        load_duration += end.QuadPart - start.QuadPart;

        DWORD num_moved = SetFilePointer(chunk_sorted_file, aligned_bytes_to_move.LowPart, &aligned_bytes_to_move.HighPart, FILE_BEGIN);
        if (num_moved == INVALID_SET_FILE_POINTER) {
            printf("%s: Failed setting file pointer in populated file with %d\n", __FUNCTION__, GetLastError());
            exit(1);
        }
        QueryPerformanceCounter(&start);
        unsigned long long tot_bytes = sizeof(Itemtype) * this->state[i].nobuff_bufsize;
        DWORD num_bytes_touched = read_into_buffer(&chunk_sorted_file, rbuff, tot_bytes);
        QueryPerformanceCounter(&end);
        load_duration += end.QuadPart - start.QuadPart;

        QueryPerformanceCounter(&start);

        if (this->debug) {
            printf("    num_bytes_touched = %llu\n", num_bytes_touched);
            printf("    num_moved = %lu\n\n", num_moved);
        }

        unsigned long long remaining_vals = (std::min)(this->state[i].bufsize, num_bytes_touched / sizeof(Itemtype));
        this->state[i].num_vals_last_block = populate_blocks(i, &remaining_vals, rbuff);
        //for (int j = 0; j < this->state[i].num_blocks; j++) {
        //    unsigned long long vals_to_copy = (std::min)(remaining_vals, this->block_size / sizeof(Itemtype));
        //    
        //    if (j == this->state[i].num_blocks - 1)
        //    {
        //        this->state[i].num_vals_last_block = vals_to_copy;
        //    }

        //    //Itemtype* temp = (Itemtype*)_aligned_malloc(this->block_size, this->bytes_per_sector);
        //    Itemtype* temp = this->free_blocks.front();
        //    this->free_blocks.pop();
        //    memcpy(temp, rbuff + buf_offset, vals_to_copy * sizeof(Itemtype));
        //    this->state[i].bq.push(temp);
        //    buf_offset += vals_to_copy;
        //    remaining_vals -= vals_to_copy;
        //}

        if (this->debug) {
            printf("    rbuff[0] = %llu\n", rbuff[0]);
            printf("    this->state[i].bufsize = %llu\n", this->state[i].bufsize);
        }

        _aligned_free(rbuff);
        rbuff = NULL;
    }

    QueryPerformanceCounter(&end);
    load_duration += end.QuadPart - start.QuadPart;

    if (this->give_vals) {
        //printf("    \n\nraw_num_buffer = %llu\n", raw_num_buffer);
        for (unsigned int i = 0; i < num_chunks; i += 1) {
            printf("    i = %u\n", i);
            printf("      this->state[i].bufsize = %llu\n", this->state[i].bufsize);
            printf("        this->state[i].bq.front()[0] = %u\n", this->state[i].bq.front()[0]);
            printf("        this->state[i].bq.front()[1] = %u\n", this->state[i].bq.front()[1]);
            printf("        this->state[i].bq.front()[blocksize - 1] = %u\n", this->state[i].bq.front()[(this->block_size / sizeof(Itemtype)) - 1]);
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

    priority_queue <MinHeapNode, vector<MinHeapNode>, my_lesser > mh;
    QueryPerformanceCounter(&start);
    for (int i = 0; i < num_chunks; i++) {
        MinHeapNode* new_node = new MinHeapNode{ 0 };
        //new_node->val = this->state[i].bufpos[0];
        new_node->val = this->state[i].bq.front()[this->state[i].curr_buflen];
        if (this->debug) {
            printf("i = %d, new_node->val = %llu\n", i, new_node->val);
        }
        new_node->chunk_index = i;
        mh.push(*new_node);
    }
    if (this->debug) {
        printf("\n");
    }

    QueryPerformanceCounter(&end);
    heap_duration += end.QuadPart - start.QuadPart;

    unsigned long long sorted_buf_size = 0;
    unsigned long long tot_num_vals = 0;
    unsigned long long num_refills = 0;
    Itemtype last_val = 0;
    QueryPerformanceCounter(&merge_start);
    while (mh.size()) {
        MinHeapNode root = mh.top();
        mh.pop();
        sorted_num_buffer[sorted_buf_size++] = root.val;

        if (sorted_buf_size == this->write_buffer_size) {
            QueryPerformanceCounter(&start);
            bool was_success = WriteFile(full_sorted_file, sorted_num_buffer, sizeof(Itemtype) * this->write_buffer_size, &num_bytes_touched, NULL);
            QueryPerformanceCounter(&end);
            if (!(was_success)) {
                printf("%s: Failed writing to merge sorted file with %d\n", __FUNCTION__, GetLastError());
                exit(1);
            }
            write_duration += end.QuadPart - start.QuadPart;
            sorted_buf_size = 0;
        }

        state_vars* sv = &this->state[root.chunk_index];
        tot_num_vals++;

        if (root.val < last_val) {
            printf("%s: next_val (%llu) is less than last_val (%llu) [IDX = %u]\n", __FUNCTION__, root.val, last_val, root.chunk_index);
            printf("   tot_num_vals (vals taken out of minheap) = %llu\n", tot_num_vals);
            printf("   fs - tot_num_vals = %lld\n", this->file_size - tot_num_vals);
            printf("    mh.size() = %u\n", mh.size());
            printf("    this->write_buffer_size = %u\n", this->write_buffer_size);
            printf("    sv->curr_buflen = %u\n", sv->curr_buflen);
            printf("    sv->bufsize = %u\n", sv->bufsize);
            printf("    sv->num_vals_last_block = %u\n", sv->num_vals_last_block);
            printf("    sv->(this->block_size / sizeof(Itemtype)) - 1 = %u\n", (this->block_size / sizeof(Itemtype)) - 1);
            printf("    sv->curr_block = %u\n", sv->curr_block);
            printf("        root.chunk_index = %u\n", root.chunk_index);
            printf("        sv->bq.front()[127] = %u\n", sv->bq.front()[127]);
            printf("        sv->bq.front()[128] = %u\n", sv->bq.front()[128]);

            exit(1);
        }

        last_val = root.val;
        sv->curr_buflen++;

        if (this->debug) {
            printf("root.chunk_index = %u\n", root.chunk_index);
            printf("  root.val = %u\n", root.val);
            printf("  sorted_buf_size = %u\n", sorted_buf_size);
            printf("    sv->curr_buflen = %llu\n", sv->curr_buflen);
            printf("    sv->bufsize = %llu\n", sv->bufsize);
        }

        // it's in the last block
        if (sv->curr_block == sv->num_blocks)
        {
            // no more values to give
            if (sv->curr_buflen >= sv->num_vals_last_block)
            {
                num_refills++;
                this->free_blocks.push(sv->bq.front());
                sv->bq.pop();
                assert(sv->bq.empty() == 1);
                if (sv->start_offset + sv->seek_offset < sv->end_offset)
                {
                    LARGE_INTEGER num_bytes_to_move = { 0 };
                    num_bytes_to_move.QuadPart = sv->start_offset + (unsigned long long)sv->seek_offset;

                    LARGE_INTEGER aligned_bytes_to_move = { 0 };
                    aligned_bytes_to_move.QuadPart = (num_bytes_to_move.QuadPart + 511) & (~511);

                    if (num_bytes_to_move.QuadPart % 512 != 0) {
                        aligned_bytes_to_move.QuadPart -= 512;
                    }

                    if (this->debug) {
                        printf(" root.chunk_index = %u\n", root.chunk_index);
                        printf(" root.val = %u\n", root.val);
                        printf("    num_bytes_to_move.QuadPart = %llu\n", num_bytes_to_move.QuadPart);
                        printf("    aligned_bytes_to_move.QuadPart = %llu\n", aligned_bytes_to_move.QuadPart);
                        printf("    this->state[root.chunk_index->bufpos = %llu\n", *(this->state[root.chunk_index].bufpos));
                        printf("    this->state[root.chunk_index->bufpos] + sv->seek_offset / (sizeof(Itemtype) = %llu\n", *(this->state[root.chunk_index].bufpos + sv->seek_offset / (sizeof(Itemtype))));
                        printf("    this->state[root.chunk_index->bufpos] + sv->seek_offset / (sizeof(Itemtype) - 1= %llu\n", *(this->state[root.chunk_index].bufpos + (sv->seek_offset / (sizeof(Itemtype))) - 1));
                    }
                    sv->bufsize = (std::min)(largest_chunk, (uint64_t)((sv->end_offset - (sv->start_offset + sv->seek_offset)) / sizeof(Itemtype)));

                    unsigned long long bytes_to_read = sv->bufsize * sizeof(Itemtype);
                    unsigned long long new_bytes_to_read = ((bytes_to_read + (num_bytes_to_move.QuadPart - aligned_bytes_to_move.QuadPart)) + 511) & (~511);

                    // since num_bytes_to_move must be aligned on 512, there's a possibility that the new aligned_bytes_to_move and bytes_to_read wouldn't actually cover the data area
                    // where the desired ints are held. So, may need to read more bytes to new_bytes_to_read to get to the area that holds the ints we care about
                    // this won't run while it uses Origami since Origami sort requires everything to be a power of 2
                    while (new_bytes_to_read + aligned_bytes_to_move.QuadPart < sv->seek_offset + bytes_to_read) {
                        new_bytes_to_read += 512;
                    }

                    Itemtype* rbuff = (Itemtype*)_aligned_malloc(new_bytes_to_read, this->bytes_per_sector);

                    if (this->debug) {
                        printf("    sv->end_offset = %llu\n", sv->end_offset);
                        printf("    sv->seek_offset = %llu\n", sv->seek_offset);
                        printf("    sv->start_offset = %llu\n", sv->start_offset);
                        printf("    sv->nobuff_bufsize = %llu\n", sv->nobuff_bufsize);
                        printf("    sv->curr_buflen = %llu\n", sv->curr_buflen);
                        printf("    sv->bufsize = %llu\n", sv->bufsize);
                        printf("    sv->bufpos = %llu\n", sv->bufpos);
                        printf("    bytes_to_read = %llu\n", bytes_to_read);
                        printf("    new_bytes_to_read = %llu\n", new_bytes_to_read);
                    }

                    DWORD num_moved = 0;
                    QueryPerformanceCounter(&start);
                    if ((num_moved = SetFilePointer(chunk_sorted_file, aligned_bytes_to_move.LowPart, &aligned_bytes_to_move.HighPart, FILE_BEGIN)) == INVALID_SET_FILE_POINTER) {
                        printf("%s: Failed setting file pointer in chunk sorted file with %d\n", __FUNCTION__, GetLastError());
                        exit(1);
                    }
                    this->num_seeks++;

                    if (this->debug) {
                        printf("    num_moved = %lu\n", num_moved);
                    }

                    bool was_success = ReadFile(chunk_sorted_file, rbuff, new_bytes_to_read, &num_bytes_touched, NULL);
                    QueryPerformanceCounter(&end);
                    read_duration += end.QuadPart - start.QuadPart;

                    if (!(was_success)) {
                        printf("%s: Failed reading from populated file with %d\n", __FUNCTION__, GetLastError());
                        exit(1);
                    }
                    if (this->debug) {
                        printf("    rbuff[%d] = %llu\n", (unsigned long long)(num_bytes_to_move.QuadPart - aligned_bytes_to_move.QuadPart) / sizeof(Itemtype), rbuff[(unsigned long long)(num_bytes_to_move.QuadPart - aligned_bytes_to_move.QuadPart) / sizeof(Itemtype)]);
                        printf("    (unsigned long long)(num_bytes_to_move.QuadPart - aligned_bytes_to_move.QuadPart) / sizeof(Itemtype) = %llu\n", (unsigned long long)(num_bytes_to_move.QuadPart - aligned_bytes_to_move.QuadPart) / sizeof(Itemtype));
                    }

                    unsigned long long remaining_vals = sv->bufsize;
                    //unsigned long long buf_offset = 0;
                    sv->num_blocks = (bytes_to_read % (this->block_size) == 0) ? (bytes_to_read / (this->block_size)) : (bytes_to_read / (this->block_size) + 1);

                    sv->num_vals_last_block = populate_blocks(root.chunk_index, &remaining_vals, rbuff + (unsigned long long)(num_bytes_to_move.QuadPart - aligned_bytes_to_move.QuadPart) / sizeof(Itemtype));

                    //for (int j = 0; j < sv->num_blocks; j++) {
                    //    unsigned long long vals_to_copy = (std::min)(remaining_vals, this->block_size / sizeof(Itemtype));
                    //    
                    //    if (j == sv->num_blocks - 1)
                    //    {
                    //        sv->num_vals_last_block = vals_to_copy;
                    //    }
                    //    
                    //    if (this->free_blocks.size() == 0)
                    //    {
                    //        //printf("    free block queue is 0 with num_blocks = %u and j = %u\n", sv->num_blocks, j);
                    //        Itemtype* temp = (Itemtype*)_aligned_malloc(this->block_size, this->bytes_per_sector);
                    //        this->free_blocks.push(temp);
                    //    }

                    //    Itemtype* temp = this->free_blocks.front();
                    //    this->free_blocks.pop();
                    //    memcpy(temp, rbuff + (unsigned long long)(num_bytes_to_move.QuadPart - aligned_bytes_to_move.QuadPart) / sizeof(Itemtype) + buf_offset, vals_to_copy * sizeof(Itemtype));
                    //    sv->bq.push(temp);

                    //    buf_offset += vals_to_copy;
                    //    remaining_vals -= vals_to_copy;
                    //}

                    _aligned_free(rbuff);
                    sv->seek_offset += bytes_to_read;
                    sv->curr_buflen = 0;
                    sv->curr_block = 1;

                    if (this->debug) {
                        printf("    sv->bufsize = %llu\n", sv->bufsize);
                        printf("    sv->seek_offset = %llu\n", sv->seek_offset);

                    }
                    if (this->give_vals) {
                        printf("      sv->bufsize = %llu\n", sv->bufsize);
                        printf("        sv->bq.front()[0] = %u\n", sv->bq.front()[0]);
                        printf("        sv->bq.front()[1] = %u\n", sv->bq.front()[1]);
                        printf("        root.chunk_index = %u\n", root.chunk_index);
                        printf("        sv->bq.front()[blocksize - 1] = %u\n", sv->bq.front()[(this->block_size / sizeof(Itemtype)) - 1]);
                    }

                    root.val = sv->bq.front()[sv->curr_buflen];

                    if (this->debug) {
                        printf("     root.val = %u\n", root.val);
                    }
                    QueryPerformanceCounter(&start);
                    mh.push(root);
                    QueryPerformanceCounter(&end);
                    heap_duration += end.QuadPart - start.QuadPart;
                }
            }
            else
            {
                root.val = sv->bq.front()[sv->curr_buflen];
                QueryPerformanceCounter(&start);
                mh.push(root);
                QueryPerformanceCounter(&end);
                heap_duration += end.QuadPart - start.QuadPart;
            }
        }
        // not in last block
        else
        {
            // has more values to give
            if (sv->curr_buflen < (this->block_size / sizeof(Itemtype)))
            {
                assert(sv->curr_buflen != this->block_size - 1);
                root.val = sv->bq.front()[sv->curr_buflen];
                QueryPerformanceCounter(&start);
                mh.push(root);
                QueryPerformanceCounter(&end);
                heap_duration += end.QuadPart - start.QuadPart;
            }
            else
            {
                this->free_blocks.push(sv->bq.front());
                sv->bq.pop();
                sv->curr_block++;
                sv->curr_buflen = 0;
                assert(sv->bq.empty() == 0);
                root.val = sv->bq.front()[sv->curr_buflen];
                QueryPerformanceCounter(&start);
                mh.push(root);
                QueryPerformanceCounter(&end);
                heap_duration += end.QuadPart - start.QuadPart;
            }
        }
    }

    QueryPerformanceCounter(&merge_end);
    merge_duration += merge_end.QuadPart - merge_start.QuadPart;
    merge_duration = merge_duration - read_duration - heap_duration - write_duration;

    if (sorted_buf_size) {
        unsigned ns = (sorted_buf_size + 127) & (~127);
        if (this->debug) {
            printf("    sorted_buf_size = %u\n", sorted_buf_size);
            printf("    ns = %u\n", ns);
        }

        QueryPerformanceCounter(&start);
        bool was_success = WriteFile(full_sorted_file, sorted_num_buffer, sizeof(Itemtype) * ns, &num_bytes_touched, NULL);
        if (!(was_success)) {
            printf("%s: Failed writing to merge sorted file with %d\n", __FUNCTION__, GetLastError());
            exit(1);
        }

        if (!CloseHandle(full_sorted_file)) {
            printf("%s: failed to close handle with no buffering with %d\n", __FUNCTION__, GetLastError());
        }
        full_sorted_file = CreateFile(this->full_sorted_fname, GENERIC_WRITE, 0, 0, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);
        //printf(" this->file_size * sizeof(Itemtype) = %llu\n", this->file_size * sizeof(Itemtype));
        LARGE_INTEGER dist = { 0 };
        dist.QuadPart = this->file_size * sizeof(Itemtype);
        if ((SetFilePointer(full_sorted_file, dist.LowPart, &dist.HighPart, FILE_BEGIN)) == INVALID_SET_FILE_POINTER) {
            printf("%s: Failed setting file pointer to truncate merged file with %d\n", __FUNCTION__, GetLastError());
            exit(1);
        }
        this->num_seeks++;

        if (!SetEndOfFile(full_sorted_file)) {
            printf("%s: Failed setting end of file to truncate merged file with %d\n", __FUNCTION__, GetLastError());
            exit(1);
        }
        QueryPerformanceCounter(&end);

        write_duration += end.QuadPart - start.QuadPart;
        sorted_buf_size = 0;
        if (!CloseHandle(full_sorted_file)) {
            printf("%s: failed to close handle full_sorted_file with %d\n", __FUNCTION__, GetLastError());
            exit(1);
        }

        CloseHandle(chunk_sorted_file);
    }
    else {
        CloseHandle(chunk_sorted_file);
        if (!CloseHandle(full_sorted_file)) {
            printf("%s: failed to close handle full_sorted_file when sorted_buf_size == 0 with %d\n", __FUNCTION__, GetLastError());
            exit(1);
        }
    }

    this->merge_duration = merge_duration / freq.QuadPart;
    this->load_duration = load_duration / freq.QuadPart;
    this->merge_read_duration = read_duration / freq.QuadPart;
    this->heap_duration = heap_duration / freq.QuadPart;
    this->merge_write_duration = write_duration / freq.QuadPart;
    _aligned_free(sorted_num_buffer);
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
        s << "File Size (MB),Memory Size (MB),Number of Seeks,Total Time (s),,Generation Time (s),Generation Rate (million keys/s),Write Time (s),Write Rate (MB/s),,";
        s << "Sort Time (s),Sort Rate (MB/s),Sort Rate (million keys/s),Read Time (s),Read Rate (MB/s),,";
        s << "Merge Time (s),Merge Rate (MB/s),Load Time (s),Load Rate (MB/s),Read Time (s),Read Rate (MB/s),";
        s << "Heap Time (s),Heap Rate (MB/s),Write Time (s),Write Rate (MB/s),\n";
    }
    check.close();
    ofstream ofile;
    ofile.open(this->metrics_fname, ios_base::app);
    if (ofile.is_open()) {
        // file size and memory size
        s << this->file_size * sizeof(Itemtype) / 1e6 << "," << this->chunk_size * sizeof(Itemtype) / 1e6 << "," << this->num_seeks << "," << this->total_time / this->num_runs << ",,";
        // generation time and rate
        s << this->total_generate_time / this->num_runs << "," << this->file_size * this->num_runs / (this->total_generate_time * 1e6) << ",";
        // write time and rate
        s << this->total_write_time / this->num_runs << "," << this->file_size * this->num_runs * sizeof(Itemtype) / (this->total_write_time * 1e6) << ",,";
        // sort time and rates
        s << this->total_sort_time / this->num_runs << "," << this->file_size * this->num_runs * sizeof(Itemtype) / (this->total_sort_time * 1e6) << "," << this->file_size * this->num_runs / (this->total_sort_time * 1e6) << ",";
        // read time and rate
        s << this->total_read_time / this->num_runs << "," << this->file_size * this->num_runs * sizeof(Itemtype) / (this->total_read_time * 1e6) << ",,";
        // merge time and rate
        s << this->total_merge_time / this->num_runs << "," << this->file_size * this->num_runs * sizeof(Itemtype) / (this->total_merge_time * 1e6) << ",";
        // load time and rate
        s << this->total_load_time / this->num_runs << "," << static_cast<unsigned long long>((1 << 30)) * this->num_runs * sizeof(Itemtype) / (this->total_load_time * 1e6) << ",";
        // read time and rate
        s << this->total_merge_read_time / this->num_runs << "," << this->file_size * this->num_runs * sizeof(Itemtype) / (this->total_merge_read_time * 1e6) << ",";
        // heap time and rate
        s << this->total_heap_time / this->num_runs << "," << this->file_size * this->num_runs * sizeof(Itemtype) / (this->total_heap_time * 1e6) << ",";
        // write time and rate
        s << this->total_merge_write_time / this->num_runs << "," << this->file_size * this->num_runs * sizeof(Itemtype) / (this->total_merge_write_time * 1e6) << ",\n";
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

        unsigned s = this->free_blocks.size();
        for (unsigned i = 0; i < s; i++)
        {
            _aligned_free(this->free_blocks.front());
            this->free_blocks.pop();
        }
    }
    return 0;
}


int external_sort::shallow_validate()
{
    printf("\n%s\n", __FUNCTION__);

    LARGE_INTEGER num_bytes_written = { 0 }, random_sorted_size = { 0 }, chunk_sorted_size = { 0 }, merge_sorted_size = { 0 };

    //unsigned int* write_buffer = (unsigned int*)_aligned_malloc(static_cast<size_t>(this->write_buffer_size) * sizeof(Itemtype), this->bytes_per_sector);
    Itemtype* val_buffer = (Itemtype*)_aligned_malloc(static_cast<size_t>(this->write_buffer_size) * sizeof(Itemtype), this->bytes_per_sector);
    unsigned long long int written = 0, number_read = 0;
    double sort_duration = 0, read_duration = 0;


    HANDLE file = CreateFile(this->full_sorted_fname, GENERIC_READ, 0, 0, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL | FILE_FLAG_NO_BUFFERING, NULL);
    HANDLE cfile = CreateFile(this->chunk_sorted_fname, GENERIC_READ, 0, 0, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);
    HANDLE rfile = CreateFile(this->fname, GENERIC_READ, 0, 0, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);

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


    HANDLE foriginal = CreateFile(this->fname, GENERIC_READ, 0, 0, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL | FILE_FLAG_NO_BUFFERING, NULL);
    HANDLE fchunk_sorted = CreateFile(this->chunk_sorted_fname, GENERIC_READ, 0, 0, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL | FILE_FLAG_NO_BUFFERING, NULL);
    HANDLE fmerge_sorted = CreateFile(this->full_sorted_fname, GENERIC_READ, 0, 0, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL | FILE_FLAG_NO_BUFFERING, NULL);


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
