/*
Notes:
- Max file size that merge_sort can currently handle is (chunk_size)^2
    - This is since mergesort_buffer_size = chunk_size, and num_vals_per_chunk is defined as mergesort_buffer_size / num chunks. 
      If num_chunks > mergesort_buffer_size, num_vals_per_chunk < 1, which doesn't make sense. Need at least 1 val per chunk since can't store partial ints.
        - mergesort_bs must be >= num_chunks
- Chunk size must be a multiple of 512
- There are no balances for chunks that have different population sizes (i.e., given a file size of 257, chunk 1 gets 128 vals in the raw_num_buffer and so does chunk 2 even though it only has 1 value in the whole chunk)
    - Once a chunk runs out of values, that chunk's space in raw_num_buffer cannot currently be used by other chunks


*/

#define NOMINMAX
#include <stdio.h>      
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
#include "external_sort.h"
#include "MinHeap.h"

using namespace std;


#define CREATE_VAR1(X,Y) X##Y 
#define CREATE_VAR(X,Y) CREATE_VAR1(X,Y)

#define MAKE_KEY(k)                             \
    next##k = a * next##k + c;                  \
    buffer[i + k] = next##k;            \


struct my_lesser {
    bool operator()(const MinHeapNode& x, const MinHeapNode& y) const {
        return x.val > y.val;
    }
};


external_sort::external_sort(unsigned long long int _FILE_SIZE, unsigned long long int _MEM_SIZE, char _fname[], char _chunk_sorted_fname[], char _full_sorted_fname[], char _metrics_fname[], int _num_runs, bool _TEST_SORT, bool _GIVE_VALS, bool _DEBUG)
    : file_size{ _FILE_SIZE }, fname{ _fname }, chunk_sorted_fname{ _chunk_sorted_fname }, full_sorted_fname{ _full_sorted_fname }, test_sort{ _TEST_SORT }, give_vals{ _GIVE_VALS }, debug{ _DEBUG }, num_runs{ _num_runs }, metrics_fname{ _metrics_fname }, chunk_size { _MEM_SIZE }, mergesort_buffer_size { _MEM_SIZE }
{
    this->windows_fs = { 0 };
    this->windows_fs.QuadPart = sizeof(KEY_TYPE) * _FILE_SIZE;
    this->write_buffer_size = (static_cast<unsigned long long>(1) << 20) / sizeof(KEY_TYPE);
    
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


    BOOL succeeded = GetDiskFreeSpaceA(NULL, NULL, &this->bytes_per_sector, NULL, NULL);
    if (!succeeded) {
        printf("__FUNCTION__main(): Failed getting disk information with %d\n", GetLastError());
    }
}

external_sort::~external_sort() 
{
    //delete[] this->state;
}


int external_sort::write_file()
{
    //if (this->debug) {
        printf("\n%s\n", __FUNCTION__);
    //}
    srand((unsigned int)time(0));
    //srand(0);
    LARGE_INTEGER start = { 0 }, end = { 0 }, freq = { 0 };

    QueryPerformanceFrequency(&freq);
    QueryPerformanceCounter(&start);

    unsigned long long int number_written = 0;

    KEY_TYPE* buffer = (KEY_TYPE*)_aligned_malloc(static_cast<size_t>(this->write_buffer_size) * sizeof(KEY_TYPE), this->bytes_per_sector);
    double generation_duration = 0, write_duration = 0;

    HANDLE pfile = CreateFile(this->fname, GENERIC_WRITE, 0, 0, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL | FILE_FLAG_NO_BUFFERING, NULL);
    if (pfile == INVALID_HANDLE_VALUE) {
        printf("__FUNCTION__write_file(): Failed opening file with %d\n", GetLastError());
        return 1;
    }
    else {
        KEY_TYPE CREATE_VAR(next, 0) = rand();
        KEY_TYPE CREATE_VAR(next, 1) = rand();
        KEY_TYPE CREATE_VAR(next, 2) = rand();
        KEY_TYPE CREATE_VAR(next, 3) = rand();
        KEY_TYPE CREATE_VAR(next, 4) = rand();
        KEY_TYPE CREATE_VAR(next, 5) = rand();
        KEY_TYPE CREATE_VAR(next, 6) = rand();
        KEY_TYPE CREATE_VAR(next, 7) = rand();

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
                for (unsigned int i = 0; i < 2; i++) {
                    printf("buffer[%d] = %llu\n", i, buffer[i]);
                }
                for (unsigned int i = num_vals_to_gen - 2; i < num_vals_to_gen; i++) {
                    printf("buffer[%d] = %llu\n", i, buffer[i]);
                }
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
            QueryPerformanceCounter(&start);
            BOOL was_success = WriteFile(pfile, buffer, sizeof(KEY_TYPE) * new_num_vals_to_write, &num_bytes_written, NULL);
            QueryPerformanceCounter(&end);
            if (!(was_success)) {
                printf("%s: Failed writing to file with %d\n", __FUNCTION__, GetLastError());
                return 1;
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
        return 1;
    }
    if (!SetEndOfFile(pfile)) {
        printf("%s: Error with SetEndOfFile with %d\n", __FUNCTION__, GetLastError());
        return 1;
    }
    CloseHandle(pfile);
    this->windows_fs.QuadPart = before_sfp.QuadPart;

    _aligned_free(buffer);
    buffer = nullptr;

    this->number_elements_touched = number_written;
    this->generation_duration = generation_duration / freq.QuadPart;
    this->write_duration = write_duration / freq.QuadPart;
    return 0;
}


int external_sort::sort_file()
{
    printf("\n%s\n", __FUNCTION__);
    LARGE_INTEGER start = { 0 }, end = { 0 }, freq = { 0 }, num_bytes_written = { 0 };

    //unsigned int* write_buffer = (unsigned int*)_aligned_malloc(static_cast<size_t>(this->write_buffer_size) * sizeof(unsigned int), this->bytes_per_sector);
    KEY_TYPE* sort_buffer = (KEY_TYPE*)_aligned_malloc(static_cast<size_t>(this->chunk_size) * sizeof(KEY_TYPE), this->bytes_per_sector);
    unsigned long long int written = 0, number_read = 0;
    double sort_duration = 0, read_duration = 0;

    QueryPerformanceFrequency(&freq);

    HANDLE old_file = CreateFile(this->fname, GENERIC_READ, 0, 0, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL | FILE_FLAG_NO_BUFFERING, NULL);
    HANDLE chunk_sorted_file = CreateFile(this->chunk_sorted_fname, GENERIC_WRITE, 0, 0, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL | FILE_FLAG_NO_BUFFERING, NULL);
    if (old_file == INVALID_HANDLE_VALUE) {
        printf("%s: Failed opening populated file with %d\n", __FUNCTION__, GetLastError());
        return 1;
    }
    else if (chunk_sorted_file == INVALID_HANDLE_VALUE) {
        printf("%s: Failed opening new file for sort output with %d\n", __FUNCTION__, GetLastError());
        return 1;
    }
    while (number_read < this->file_size) {
        if (this->debug) {
            printf("number_read = %lu\n", number_read);
        }
        unsigned long num_vals_to_read = 0;
        unsigned long new_num_vals_to_read = 0;
        if (number_read + this->chunk_size > this->file_size) {
            if (number_read) {
                num_vals_to_read = this->file_size % number_read;
            }
            else {
                num_vals_to_read = this->file_size;
            }
            new_num_vals_to_read = (num_vals_to_read + 127) & (~127);
        }
        else {
            num_vals_to_read = this->chunk_size;
            new_num_vals_to_read = this->chunk_size;
        }
        if (this->debug) {
            printf("    num_vals_to_read = %lu\n", num_vals_to_read);
            printf("    new_num_vals_to_read = %lu\n", new_num_vals_to_read);
            printf("    this->chunk_size = %lu\n", this->chunk_size);
        }
        QueryPerformanceCounter(&start);
        DWORD num_bytes_touched;
        bool was_success = ReadFile(old_file, sort_buffer, sizeof(KEY_TYPE) * new_num_vals_to_read, &num_bytes_touched, NULL);
        QueryPerformanceCounter(&end);
        if (!(was_success)) {
            printf("%s: Failed reading from populated file with %d\n", __FUNCTION__, GetLastError());
            return 1;
        }
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
        QueryPerformanceCounter(&start);
        std::sort(sort_buffer, sort_buffer + num_vals_to_read);
        QueryPerformanceCounter(&end);
        if (this->give_vals) {
            for (unsigned int i = 0; i < 2 && i < num_vals_to_read; i++) {
                printf("buffer[%d] = %u\n", i, sort_buffer[i]);
            }
            if (num_vals_to_read > 6) {
                printf("buffer[%d] = %llu\n", num_vals_to_read / 2 - 3, sort_buffer[num_vals_to_read / 2 - 3]);
                printf("buffer[%d] = %llu\n", num_vals_to_read / 2 - 2, sort_buffer[num_vals_to_read / 2 - 2]);
                printf("buffer[%d] = %llu\n", num_vals_to_read / 2 - 1, sort_buffer[num_vals_to_read / 2 - 1]);
                printf("buffer[%d] = %llu\n", num_vals_to_read / 2, sort_buffer[num_vals_to_read / 2]);
                printf("buffer[%d] = %llu\n", num_vals_to_read / 2 + 1, sort_buffer[num_vals_to_read / 2 + 1]);
            }
            /*for (unsigned int i = num_vals_to_read - 2; i < num_vals_to_read && i >= 0; i++) {
                printf("buffer[%d] = %u\n", i, sort_buffer[i]);
            }*/
        }
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
                    num_vals_to_write = this->chunk_size;
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


            was_success = WriteFile(chunk_sorted_file, sort_buffer + loop_written, sizeof(KEY_TYPE) * new_num_vals_to_write, &num_bytes_touched, NULL);
            if (!(was_success)) {
                printf("%s: Failed writing to new file for sort output with %d\n", __FUNCTION__, GetLastError());
                return 1;
            }
            
            if (num_vals_to_write != new_num_vals_to_write) {
                CloseHandle(chunk_sorted_file);
                chunk_sorted_file = CreateFile(this->chunk_sorted_fname, GENERIC_WRITE, 0, 0, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);
                //LARGE_INTEGER before_sfp = { 0 };
                //before_sfp.QuadPart = this->windows_fs.QuadPart;
                DWORD num_moved = 0;
                num_moved = SetFilePointer(chunk_sorted_file, this->windows_fs.LowPart, NULL/*&this->windows_fs.HighPart*/, FILE_BEGIN);
                if (num_moved == INVALID_SET_FILE_POINTER) {
                    printf("%s: error in SetFilePointer with %d\n", __FUNCTION__, GetLastError());
                    return 1;
                }
                if (!SetEndOfFile(chunk_sorted_file)) {
                    printf("%s: error in SetEndOfFile with %d\n", __FUNCTION__, GetLastError());
                    return 1;
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

    //this->state = new state_vars[num_chunks];

    LARGE_INTEGER start = { 0 }, end = { 0 }, merge_start = { 0 }, merge_end = { 0 }, freq = { 0 }, num_bytes_written = { 0 };
    unsigned long long int written = 0, number_read = 0;
    double load_duration = 0, read_duration = 0, heap_duration = 0, write_duration = 0, merge_duration = 0;

    DWORD vals_per_chunk = 0;
    //if (num_chunks * this->chunk_size > this->file_size) {
    //    //vals_per_chunk = (this->file_size % num_chunks == 0) ? (this->file_size / num_chunks) : ((this->file_size / num_chunks) + 1);
    //    vals_per_chunk = this->file_size / num_chunks;
    //}
    //else {
        if (this->mergesort_buffer_size / num_chunks < this->chunk_size) {
            vals_per_chunk = this->mergesort_buffer_size / num_chunks;
        }
        else {
            vals_per_chunk = this->chunk_size;
        }
    //}

    if (this->debug) {
        printf("    file_size = %llu\n", this->file_size);
        printf("    file_size * sizeof(KEY_TYPE) = %llu\n", this->file_size * sizeof(KEY_TYPE));
        printf("    num_chunks = %llu\n", num_chunks);
        printf("    chunk_size / num_chunks = %llu\n", this->chunk_size / num_chunks);
        printf("    chunk_size = %llu\n", this->chunk_size);
        printf("    vals_per_chunk = %lu\n", vals_per_chunk);
    }

    HANDLE chunk_sorted_file = CreateFile(this->chunk_sorted_fname, GENERIC_READ, 0, 0, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL | FILE_FLAG_NO_BUFFERING, NULL);
    HANDLE full_sorted_file = CreateFile(this->full_sorted_fname, GENERIC_WRITE, 0, 0, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL | FILE_FLAG_NO_BUFFERING, NULL);
    if (chunk_sorted_file == INVALID_HANDLE_VALUE) {
        printf("%s: Failed opening populated file with %d\n", __FUNCTION__, GetLastError());
        return 1;
    }
    else if (full_sorted_file == INVALID_HANDLE_VALUE) {
        printf("%s: Failed opening new file for mergesort output with %d\n", __FUNCTION__, GetLastError());
        return 1;
    }
    
    DWORD total_bytes_touched = 0;
    DWORD num_bytes_touched;
    DWORD num_bytes_to_read = 0;

    // 1)  Create an array of size 1GB
    //          i)  Populate the 1GB array with the first n vals from each chunk

    KEY_TYPE* raw_num_buffer = (KEY_TYPE*)_aligned_malloc(this->mergesort_buffer_size * sizeof(KEY_TYPE), this->bytes_per_sector);
    //MinHeapNode* heap_buffer = new MinHeapNode[num_chunks];
    KEY_TYPE* sorted_num_buffer = (KEY_TYPE*)_aligned_malloc(this->write_buffer_size * sizeof(KEY_TYPE), this->bytes_per_sector);
    // defines state's values for every chunk
    INT64 running_file_offset = 0;
    INT64 running_buf_offset = 0;

    for (int i = 0; i < num_chunks; i++) {
        struct state_vars new_chunk;
        if (i != num_chunks - 1 && this->file_size % this->chunk_size != 0) {
            new_chunk.chunk_size = this->chunk_size;
        }
        else if (i == num_chunks - 1 && this->file_size % this->chunk_size != 0) {
            new_chunk.chunk_size = this->file_size % this->chunk_size;
        }
        else {
            new_chunk.chunk_size = this->chunk_size;
        }
        
        // cursor at the start of this chunk's buf's portion in the raw_num_buffer
        new_chunk.bufpos = raw_num_buffer + running_buf_offset;
        // how many vals are currently in the write buffer
        new_chunk.curr_buflen = 0;
        // where the start of the chunk's portion in the raw buffer is in the file
        new_chunk.chunk_ptr = running_file_offset * sizeof(KEY_TYPE);

        // start of the whole chunk in the file
        new_chunk.start_offset = running_file_offset * sizeof(KEY_TYPE);
        // end of the whole chunk in the file (equivalent to the start of the next chunk, if it exists)
        new_chunk.end_offset = (running_file_offset + new_chunk.chunk_size) * sizeof(KEY_TYPE);

        // next place in the file to start the next seek from the start_offset, assuming
        new_chunk.seek_offset = min(vals_per_chunk * sizeof(KEY_TYPE), new_chunk.end_offset - new_chunk.start_offset);

        // how big a portion this chunk gets in the raw_num_buffer
        new_chunk.bufsize = min((INT64)vals_per_chunk, (INT64)((new_chunk.end_offset - new_chunk.start_offset) / sizeof(KEY_TYPE)));
        new_chunk.nobuff_bufsize = (static_cast<INT64>(new_chunk.bufsize) + 127) & (~127);

        running_file_offset += new_chunk.chunk_size;
        running_buf_offset += new_chunk.bufsize;
        if (this->debug) {
            printf("\ni = %d\n", i);
            new_chunk.print();
        }
        this->state.push_back(new_chunk);

    }

    LARGE_INTEGER num_bytes_to_move = { 0 };
    QueryPerformanceFrequency(&freq);
    QueryPerformanceCounter(&start);
    for (int i = 0; i < num_chunks; i++)
    {
        KEY_TYPE* read_into_buffer = (KEY_TYPE*)_aligned_malloc(this->state[i].nobuff_bufsize * sizeof(KEY_TYPE), this->bytes_per_sector);
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
            return 1;
        }
        QueryPerformanceCounter(&start);
        bool was_success = ReadFile(chunk_sorted_file, read_into_buffer, this->state[i].nobuff_bufsize * sizeof(KEY_TYPE), &num_bytes_touched, NULL);
        QueryPerformanceCounter(&end);
        load_duration += end.QuadPart - start.QuadPart;

        QueryPerformanceCounter(&start);
        if (!(was_success)) {
            printf("%s: Failed reading from populated file with %d\n", __FUNCTION__, GetLastError());
            return 1;
        }
        if (this->debug) {
            printf("    num_bytes_touched = %llu\n", num_bytes_touched);
            printf("    num_moved = %lu\n\n", num_moved);
        }
        memcpy(this->state[i].bufpos, read_into_buffer, this->state[i].bufsize * sizeof(KEY_TYPE));
        if (this->debug) {
            printf("    read_into_buffer[0] = %llu\n", read_into_buffer[0]);
            printf("    bufpos[0] = %llu\n", this->state[i].bufpos[0]);
            printf("    this->state[i].bufsize = %llu\n", this->state[i].bufsize);
            printf("        raw_num_buffer[%d] = %llu\n", this->state[i].bufsize - 1, raw_num_buffer[this->state[i].bufsize - 1]);
            printf("        bufpos[%d] = %llu\n", this->state[i].bufsize - 1, this->state[i].bufpos[this->state[i].bufsize - 1]);
        }
        _aligned_free(read_into_buffer);
        read_into_buffer = NULL;
    }

    QueryPerformanceCounter(&end);
    load_duration += end.QuadPart - start.QuadPart;

    if (this->give_vals) {
        printf("    \n\nraw_num_buffer = %llu\n", raw_num_buffer);
        for (unsigned int i = 0; i < num_chunks; i += 1) {
            printf("    i = %u\n", i);
            printf("      this->state[i].bufsize = %llu\n", this->state[i].bufsize);
            printf("        this->state[i].bufpos[0] = %u\n", this->state[i].bufpos[0]);
            printf("        this->state[i].bufpos[1] = %u\n", this->state[i].bufpos[1]);
            printf("        this->state[i].bufpos[this->state[i].bufsize - 6] = %u\n", this->state[i].bufpos[this->state[i].bufsize - 6]);
            printf("        this->state[i].bufpos[this->state[i].bufsize - 5] = %u\n", this->state[i].bufpos[this->state[i].bufsize - 5]);
            printf("        this->state[i].bufpos[this->state[i].bufsize - 4] = %u\n", this->state[i].bufpos[this->state[i].bufsize - 4]);
            printf("        this->state[i].bufpos[this->state[i].bufsize - 3] = %u\n", this->state[i].bufpos[this->state[i].bufsize - 3]);
            printf("        this->state[i].bufpos[this->state[i].bufsize - 2] = %u\n", this->state[i].bufpos[this->state[i].bufsize - 2]);
            printf("        this->state[i].bufpos[this->state[i].bufsize - 1] = %u\n", this->state[i].bufpos[this->state[i].bufsize - 1]);
            printf("        this->state[i].bufpos[this->state[i].bufsize] = %u\n", this->state[i].bufpos[this->state[i].bufsize]);
        }
    }

    // 2)  Create MinHeapNodes with the first val from every chunk(now taken from the 1GB array) and insert into heap array
    // 3)  Create MinHeap from the array    

    priority_queue <MinHeapNode, vector<MinHeapNode>, my_lesser > mh;
    QueryPerformanceCounter(&start);
    for (int i = 0; i < num_chunks; i++) {
        MinHeapNode* new_node = new MinHeapNode{ 0 };
        new_node->val = this->state[i].bufpos[0];
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
    KEY_TYPE last_val = 0;
    QueryPerformanceCounter(&merge_start);
    while (mh.size()) {
        MinHeapNode root = mh.top();
        mh.pop();
        sorted_num_buffer[sorted_buf_size++] = root.val;
        
        if (sorted_buf_size == this->write_buffer_size) {
            QueryPerformanceCounter(&start);
            bool was_success = WriteFile(full_sorted_file, sorted_num_buffer, sizeof(KEY_TYPE) * this->write_buffer_size, &num_bytes_touched, NULL);
            QueryPerformanceCounter(&end);
            if (!(was_success)) {
                printf("%s: Failed writing to merge sorted file with %d\n", __FUNCTION__, GetLastError());
                return 1;
            }
            write_duration += end.QuadPart - start.QuadPart;
            sorted_buf_size = 0;
        }
        
        state_vars* sv = &this->state[root.chunk_index];
        if (root.val < last_val) {
            printf("%s: next_val (%llu) is less than last_val (%llu) [IDX = %u]\n", __FUNCTION__, root.val, last_val, root.chunk_index);
            printf("    sv->curr_buflen = %u\n", sv->curr_buflen);
            return 1;
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

        if ( sv->curr_buflen < sv->bufsize) {
            if (this->debug) {
                printf(" root.chunk_index = %u\n", root.chunk_index);
                printf("     root.val = %u\n", root.val);
            }
            root.val = sv->bufpos[sv->curr_buflen];
            if (this->debug) {
                printf("     root.val = %u\n", root.val);
            }

            QueryPerformanceCounter(&start);
            mh.push(root);
            QueryPerformanceCounter(&end);
            heap_duration += end.QuadPart - start.QuadPart;
        }
        else {
            if (sv->start_offset + sv->seek_offset < sv->end_offset) {
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
                    printf("    this->state[root.chunk_index->bufpos] + sv->seek_offset / (sizeof(KEY_TYPE) = %llu\n", *(this->state[root.chunk_index].bufpos + sv->seek_offset / (sizeof(KEY_TYPE))));
                    printf("    this->state[root.chunk_index->bufpos] + sv->seek_offset / (sizeof(KEY_TYPE) - 1= %llu\n", *(this->state[root.chunk_index].bufpos + (sv->seek_offset / (sizeof(KEY_TYPE))) - 1));
                }

                unsigned long long bytes_to_read = min(sv->end_offset - sv->seek_offset, (unsigned long long)sv->bufsize * sizeof(KEY_TYPE));
                unsigned long long new_bytes_to_read = ((bytes_to_read + (num_bytes_to_move.QuadPart - aligned_bytes_to_move.QuadPart)) + 511) & (~511);

                // since byts_to_move must be aligned on 512, there's a possibility that the new aligned_bytes_to_move and bytes_to_read wouldn't actually cover the data area
                // where the desired ints are held. So, may need to read more bytes to new_bytes_to_read to get to the area that holds the ints we care about
                while (new_bytes_to_read + aligned_bytes_to_move.QuadPart < sv->seek_offset + num_bytes_to_read) {
                    new_bytes_to_read += 512;
                }
                KEY_TYPE* read_into_buffer = (KEY_TYPE*)_aligned_malloc(new_bytes_to_read, this->bytes_per_sector);

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
                    return 1;
                }
                this->num_seeks++;

                if (this->debug) {
                    printf("    num_moved = %lu\n", num_moved);
                }

                bool was_success = ReadFile(chunk_sorted_file, read_into_buffer, new_bytes_to_read, &num_bytes_touched, NULL);
                QueryPerformanceCounter(&end);
                read_duration += end.QuadPart - start.QuadPart;

                if (!(was_success)) {
                    printf("%s: Failed reading from populated file with %d\n", __FUNCTION__, GetLastError());
                    return 1;
                }
                if (this->debug) {
                    printf("    read_into_buffer[0] = %llu\n", read_into_buffer[0]);
                    printf("    read_into_buffer[%d] = %llu\n", (unsigned long long)(num_bytes_to_move.QuadPart - aligned_bytes_to_move.QuadPart) / sizeof(KEY_TYPE), read_into_buffer[(unsigned long long)(num_bytes_to_move.QuadPart - aligned_bytes_to_move.QuadPart) / sizeof(KEY_TYPE)]);
                    printf("    (unsigned long long)(num_bytes_to_move.QuadPart - aligned_bytes_to_move.QuadPart) / sizeof(unsigned int) = %llu\n", (unsigned long long)(num_bytes_to_move.QuadPart - aligned_bytes_to_move.QuadPart) / sizeof(KEY_TYPE));
                }
                memcpy(sv->bufpos, read_into_buffer + (unsigned long long)(num_bytes_to_move.QuadPart - aligned_bytes_to_move.QuadPart) / sizeof(unsigned int), bytes_to_read);
                _aligned_free(read_into_buffer);
                sv->bufsize = min(sv->bufsize, (INT64)((sv->end_offset - (sv->start_offset + sv->seek_offset)) / sizeof(KEY_TYPE)));
                sv->seek_offset += bytes_to_read;
                sv->curr_buflen = 0;
                if (this->debug) {
                    printf("    sv->bufsize = %llu\n", sv->bufsize);
                    printf("    sv->seek_offset = %llu\n", sv->seek_offset);

                }

                if (this->give_vals) {
                    for (int i = 0; i < 2; i++) {
                        printf("    sv->bufpos[%d] = %llu\n", i, sv->bufpos[i]);
                    }

                    for (int i = bytes_to_read / sizeof(KEY_TYPE) - 1; i > bytes_to_read / sizeof(KEY_TYPE) - 2; i--) {
                        printf("    sv->bufpos[%d] = %llu\n", i, sv->bufpos[i]);
                    }
                }

                root.val = sv->bufpos[sv->curr_buflen];
                if (this->debug) {
                    printf("     root.val = %u\n", root.val);
                }
                QueryPerformanceCounter(&start);
                MinHeapNode curr_top = mh.top();
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
        bool was_success = WriteFile(full_sorted_file, sorted_num_buffer, sizeof(KEY_TYPE) * ns, &num_bytes_touched, NULL);
        if (!(was_success)) {
            printf("%s: Failed writing to merge sorted file with %d\n", __FUNCTION__, GetLastError());
            return 1;
        }
        if (!CloseHandle(full_sorted_file)) {
            printf("%s: failed to close handle with no buffering with %d\n", __FUNCTION__, GetLastError());
        }
        full_sorted_file = CreateFile(this->full_sorted_fname, GENERIC_WRITE, 0, 0, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);
        if ((SetFilePointer(full_sorted_file, this->file_size * sizeof(KEY_TYPE), NULL, FILE_BEGIN)) == INVALID_SET_FILE_POINTER) {
            printf("%s: Failed setting file pointer to truncate merged file with %d\n", __FUNCTION__, GetLastError());
            return 1;
        }
        this->num_seeks++;

        if (!SetEndOfFile(full_sorted_file)) {
            printf("%s: Failed setting end of file to truncate merged file with %d\n", __FUNCTION__, GetLastError());
            return 1;
        }
        QueryPerformanceCounter(&end);

        write_duration += end.QuadPart - start.QuadPart;
        sorted_buf_size = 0;
        if (!CloseHandle(full_sorted_file)) {
            printf("%s: failed to close handle full_sorted_file with %d\n", __FUNCTION__, GetLastError());
            return 1;
        }

        CloseHandle(chunk_sorted_file);
    }
    else {
        CloseHandle(chunk_sorted_file);
        if (!CloseHandle(full_sorted_file)) {
            printf("%s: failed to close handle full_sorted_file when sorted_buf_size == 0 with %d\n", __FUNCTION__, GetLastError());
            return 1;
        }
    }

    this->merge_duration = merge_duration / freq.QuadPart;
    this->load_duration = load_duration / freq.QuadPart;
    this->merge_read_duration = read_duration / freq.QuadPart;
    this->heap_duration = heap_duration / freq.QuadPart;
    this->merge_write_duration = write_duration / freq.QuadPart;
    _aligned_free(raw_num_buffer);
    _aligned_free(sorted_num_buffer);
    return 0;
}


void external_sort::print_metrics()
{
    if (this->debug) {
        printf("\n");
    }
    printf("file size = %f MB\n", this->file_size * sizeof(unsigned int) / 1e6);
    printf("Averages over %d runs\n", this->num_runs);
    printf("\n    Random File Generation Statistics\n");
    printf("    Generation time: %f s\n", this->total_generate_time / this->num_runs);
    printf("    Generation rate: %f million keys/s\n", this->file_size * this->num_runs / (this->total_generate_time * 1e6));
    printf("    Write time:      %f s\n", this->total_write_time / this->num_runs);
    printf("    Write rate:      %f MB/s\n", this->file_size * this->num_runs * sizeof(unsigned int) / (this->total_write_time * 1e6));
    printf("\n    Chunk Sort Statistics\n");
    printf("    Sort time:       %f s\n", this->total_sort_time / this->num_runs);
    printf("    Sort rate:       %f MB/s\n", this->file_size * this->num_runs * sizeof(unsigned int) / (this->total_sort_time * 1e6));
    printf("    Sort rate:       %f million keys/s\n", this->file_size * this->num_runs / (this->total_sort_time * 1e6));
    printf("    Read time:       %f s\n", this->total_read_time / this->num_runs);
    printf("    Read rate:       %f MB/s\n", this->file_size * this->num_runs * sizeof(unsigned int) / (this->total_read_time * 1e6));
    printf("\n    Merge Statistics\n");
    printf("    Merge time:      %f s\n", this->total_merge_time / this->num_runs);
    printf("    Merge rate:      %f MB/s\n", this->file_size * this->num_runs * sizeof(unsigned int) / (this->total_merge_time * 1e6));
    printf("    Load time:       %f s\n", this->total_load_time / this->num_runs);
    printf("    Load rate:       %f MB/s\n", static_cast<unsigned long long>((1 << 30)) * this->num_runs * sizeof(unsigned int) / (this->total_load_time * 1e6));
    printf("    Read time:       %f s\n", this->total_merge_read_time / this->num_runs);
    printf("    Read rate:       %f MB/s\n", this->file_size * this->num_runs * sizeof(unsigned int) / (this->total_merge_read_time * 1e6));
    printf("    Heap time:       %f s\n", this->total_heap_time / this->num_runs);
    printf("    Heap rate:       %f MB/s\n", this->file_size * this->num_runs * sizeof(unsigned int) / (this->total_heap_time * 1e6));
    printf("    Write time:      %f s\n", this->total_merge_write_time / this->num_runs);
    printf("    Write rate:      %f MB/s\n", this->file_size * this->num_runs * sizeof(unsigned int) / (this->total_merge_write_time * 1e6));
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
        s << this->file_size * sizeof(KEY_TYPE) / 1e6 << "," << this->chunk_size * sizeof(KEY_TYPE) / 1e6 << "," << this->num_seeks << "," << this->total_time / this->num_runs << ",,";
        // generation time and rate
        s << this->total_generate_time / this->num_runs << "," << this->file_size * this->num_runs / (this->total_generate_time * 1e6) << ",";
        // write time and rate
        s << this->total_write_time / this->num_runs << "," << this->file_size * this->num_runs * sizeof(KEY_TYPE) / (this->total_write_time * 1e6) << ",,";
        // sort time and rates
        s << this->total_sort_time / this->num_runs << "," << this->file_size * this->num_runs * sizeof(KEY_TYPE) / (this->total_sort_time * 1e6) << "," << this->file_size * this->num_runs / (this->total_sort_time * 1e6) << ",";
        // read time and rate
        s << this->total_read_time / this->num_runs << "," << this->file_size * this->num_runs * sizeof(KEY_TYPE) / (this->total_read_time * 1e6) << ",,";
        // merge time and rate
        s << this->total_merge_time / this->num_runs << "," << this->file_size * this->num_runs * sizeof(KEY_TYPE) / (this->total_merge_time * 1e6) << ",";
        // load time and rate
        s << this->total_load_time / this->num_runs << "," << static_cast<unsigned long long>((1 << 30)) * this->num_runs * sizeof(KEY_TYPE) / (this->total_load_time * 1e6) << ",";
        // read time and rate
        s << this->total_merge_read_time / this->num_runs << "," << this->file_size * this->num_runs * sizeof(KEY_TYPE) / (this->total_merge_read_time * 1e6) << ",";
        // heap time and rate
        s << this->total_heap_time / this->num_runs << "," << this->file_size * this->num_runs * sizeof(KEY_TYPE) / (this->total_heap_time * 1e6) << ",";
        // write time and rate
        s << this->total_merge_write_time / this->num_runs << "," << this->file_size * this->num_runs * sizeof(KEY_TYPE) / (this->total_merge_write_time * 1e6) << ",\n";
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
        int was_fail = write_file();
        if (was_fail)
        {
            return 1;
        }
        if (this->debug)
        {
            printf("Number of bytes written = %llu\n", this->number_elements_touched);
        }
        this->total_generate_time += this->generation_duration;
        this->total_write_time += this->write_duration;
        if (this->test_sort)
        {
            was_fail = sort_file();
            if (was_fail)
            {
                return 1;
            }
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

    }
    return 0;
}


int external_sort::shallow_validate() 
{
    //if (this->debug) {
        printf("\n%s\n", __FUNCTION__);
    //}

    LARGE_INTEGER num_bytes_written = { 0 };

    //unsigned int* write_buffer = (unsigned int*)_aligned_malloc(static_cast<size_t>(this->write_buffer_size) * sizeof(unsigned int), this->bytes_per_sector);
    KEY_TYPE* val_buffer = (KEY_TYPE*)_aligned_malloc(static_cast<size_t>(this->write_buffer_size) * sizeof(KEY_TYPE), this->bytes_per_sector);
    unsigned long long int written = 0, number_read = 0;
    double sort_duration = 0, read_duration = 0;


    HANDLE file = CreateFile(this->full_sorted_fname, GENERIC_READ, 0, 0, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL | FILE_FLAG_NO_BUFFERING, NULL);
    if (file == INVALID_HANDLE_VALUE) {
        printf("%s: Failed opening new file for sort output with %d\n", __FUNCTION__, GetLastError());
        return 1;
    }
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
        bool was_success = ReadFile(file, val_buffer, sizeof(KEY_TYPE) * new_num_vals_to_read, &num_bytes_touched, NULL);
        if (!(was_success)) {
            printf("%s: Failed reading from populated file with %d\n", __FUNCTION__, GetLastError());
            return 1;
        }
        number_read += num_vals_to_read;
        if (this->debug) {
            printf("    number_read after read = %llu\n", number_read);
        }

        for (int i = 0; i < num_vals_to_read; i++) {
            if (val_buffer[i] < last_val) {
                printf("%s: Merged file out of order: %u > %u at i = %d\n", __FUNCTION__, val_buffer[i], last_val, i);
                return 1;
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
    //if (this->debug) {
        printf("\n%s\n", __FUNCTION__);
    //}

    unordered_set<unsigned int>* original = new unordered_set<unsigned int>;
    unordered_set<unsigned int>* chunk_sorted = new unordered_set<unsigned int>;
    unordered_set<unsigned int>* merge_sorted = new unordered_set<unsigned int>;


    LARGE_INTEGER num_bytes_written = { 0 };

    //unsigned int* write_buffer = (unsigned int*)_aligned_malloc(static_cast<size_t>(this->write_buffer_size) * sizeof(unsigned int), this->bytes_per_sector);
    KEY_TYPE* val_buffer = (KEY_TYPE*)_aligned_malloc(static_cast<size_t>(this->write_buffer_size) * sizeof(KEY_TYPE), this->bytes_per_sector);
    unsigned long long int written = 0, number_read = 0;
    double sort_duration = 0, read_duration = 0;


    HANDLE foriginal = CreateFile(this->fname, GENERIC_READ, 0, 0, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL | FILE_FLAG_NO_BUFFERING, NULL);
    HANDLE fchunk_sorted = CreateFile(this->chunk_sorted_fname, GENERIC_READ, 0, 0, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL | FILE_FLAG_NO_BUFFERING, NULL);
    HANDLE fmerge_sorted = CreateFile(this->full_sorted_fname, GENERIC_READ, 0, 0, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL | FILE_FLAG_NO_BUFFERING, NULL);


    if (foriginal == INVALID_HANDLE_VALUE) {
        printf("%s: Failed opening new file foriginal for sort output with %d\n", __FUNCTION__, GetLastError());
        return 1;
    }
    if (fchunk_sorted == INVALID_HANDLE_VALUE) {
        printf("%s: Failed opening new file fchunk_sorted for sort output with %d\n", __FUNCTION__, GetLastError());
        return 1;
    }
    if (fmerge_sorted == INVALID_HANDLE_VALUE) {
        printf("%s: Failed opening new file fmerge_sorted for sort output with %d\n", __FUNCTION__, GetLastError());
        return 1;
    }


    while (number_read < this->file_size) {
        if (this->debug) {
            printf("number_read = %lu\n", number_read);
        }
        /*unsigned long num_vals_to_read = 0;
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
        }*/
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
        bool was_success = ReadFile(foriginal, val_buffer, sizeof(KEY_TYPE) * new_num_vals_to_read, &num_bytes_touched, NULL);
        if (!(was_success)) {
            printf("%s: Failed reading from foriginal with %d\n", __FUNCTION__, GetLastError());
            return 1;
        }

        for (int i = 0; i < num_vals_to_read; i++) {
            original->insert(val_buffer[i]);
        }

        // chunk sorted file
        was_success = ReadFile(fchunk_sorted, val_buffer, sizeof(KEY_TYPE) * new_num_vals_to_read, &num_bytes_touched, NULL);
        if (!(was_success)) {
            printf("%s: Failed reading from fchunk_sorted with %d\n", __FUNCTION__, GetLastError());
            return 1;
        }

        for (int i = 0; i < num_vals_to_read; i++) {
            chunk_sorted->insert(val_buffer[i]);
        }

        // merge sorted file
        was_success = ReadFile(fmerge_sorted, val_buffer, sizeof(KEY_TYPE) * new_num_vals_to_read, &num_bytes_touched, NULL);
        if (!(was_success)) {
            printf("%s: Failed reading from fmerge_sorted with %d\n", __FUNCTION__, GetLastError());
            return 1;
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
        return 1;
    }
    else if (*original != *merge_sorted) {
        printf("%s: original and merge_sorted contain different values\n", __FUNCTION__);
        printf("    original.size() = %d\n", original->size());
        printf("    merge_sorted.size() = %d\n", merge_sorted->size());
        return 1;
    }
    else if (*chunk_sorted != *merge_sorted) {
        printf("%s: merge_sorted and chunk_sorted contain different values\n", __FUNCTION__);
        printf("    chunk_sorted.size() = %d\n", chunk_sorted->size());
        printf("    merge_sorted.size() = %d\n", merge_sorted->size());
        return 1;
    }

    delete original;
    delete chunk_sorted;
    delete merge_sorted;

    printf("\n%s: All files contain the same values\n", __FUNCTION__);

    return 0;
}
