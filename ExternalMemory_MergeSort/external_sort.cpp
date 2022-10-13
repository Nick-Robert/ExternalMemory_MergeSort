#include "windows.h"
#include <stdio.h>      
#include <stdlib.h>  
#include <algorithm>
#include <time.h>
#include "external_sort.h"
#include "MinHeap.h"


#define CREATE_VAR1(X,Y) X##Y 
#define CREATE_VAR(X,Y) CREATE_VAR1(X,Y)

#define MAKE_KEY(k)                             \
    next##k = a * next##k + c;                  \
    buffer_aligned[i + k] = next##k;            \


external_sort::external_sort(unsigned long long int _FILE_SIZE, char _fname[], char _chunk_sorted_fname[], char _full_sorted_fname[],int _num_runs, bool _TEST_SORT, bool _GIVE_VALS, bool _DEBUG) 
    : file_size{ _FILE_SIZE }, fname{ _fname }, chunk_sorted_fname{ _chunk_sorted_fname }, full_sorted_fname{ _full_sorted_fname }, test_sort{ _TEST_SORT }, give_vals{ _GIVE_VALS }, debug{ _DEBUG }, num_runs{ _num_runs }
{
    this->buffer_size = (1 << 20) / sizeof(unsigned int);
    this->total_generate_time = 0;
    this->total_write_time = 0;
    this->total_sort_time = 0;
    this->total_read_time = 0;

    BOOL succeeded = GetDiskFreeSpaceA(NULL, NULL, &this->bytes_per_sector, NULL, NULL);
    if (!succeeded) {
        printf("__FUNCTION__main(): Failed getting disk information with %d\n", GetLastError());
    }

}


int external_sort::write_file()
{
    srand((unsigned int)time(0));
    LARGE_INTEGER start = { 0 }, end = { 0 }, freq = { 0 };

    QueryPerformanceFrequency(&freq);
    QueryPerformanceCounter(&start);

    unsigned long long int number_written = 0;

    unsigned int* buffer_aligned = (unsigned int*)_aligned_malloc(static_cast<size_t>(this->buffer_size) * 4, this->bytes_per_sector);
    double generation_duration = 0, write_duration = 0;

    HANDLE pfile = CreateFile(this->fname, GENERIC_WRITE, 0, 0, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL | FILE_FLAG_NO_BUFFERING, NULL);
    if (pfile == INVALID_HANDLE_VALUE) {
        printf("__FUNCTION__write_file(): Failed opening file with %d\n", GetLastError());
        return 1;
    }
    else {
        unsigned int CREATE_VAR(next, 0) = rand();
        unsigned int CREATE_VAR(next, 1) = rand();
        unsigned int CREATE_VAR(next, 2) = rand();
        unsigned int CREATE_VAR(next, 3) = rand();
        unsigned int CREATE_VAR(next, 4) = rand();
        unsigned int CREATE_VAR(next, 5) = rand();
        unsigned int CREATE_VAR(next, 6) = rand();
        unsigned int CREATE_VAR(next, 7) = rand();

        const unsigned int a = 214013;
        // const unsigned int m = 4096*4;
        const unsigned int c = 2531011;
        while (number_written < this->file_size) {
            // populate buffer - utilizes the Linear Congruential Generator method
            // https://en.wikipedia.org/wiki/Linear_congruential_generator
            QueryPerformanceCounter(&start);
            for (unsigned int i = 0; i < this->buffer_size; i += 8) {
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
                for (unsigned int i = 0; i < this->buffer_size; i++) {
                    if (i < 7) {
                        printf("buffer_aligned[%d] = %u\n", i, buffer_aligned[i]);
                    }
                }
            }

            DWORD num_bytes_written;
            QueryPerformanceCounter(&start);
            BOOL was_success = WriteFile(pfile, buffer_aligned, sizeof(unsigned int) * this->buffer_size, &num_bytes_written, NULL);
            QueryPerformanceCounter(&end);
            if (!(was_success)) {
                printf("%s: Failed writing to file with %d\n", __FUNCTION__, GetLastError());
                return 1;
            }
            write_duration += end.QuadPart - start.QuadPart;

            number_written += num_bytes_written / sizeof(unsigned int);
            if (this->debug) {
                printf("    number_written = %llu\n", number_written);
                printf("    bufsize = %d\n", this->buffer_size);
            }
        }
        if (this->debug)
        {
            printf("Generation Duration: %f\n", generation_duration);
            printf("Write Duration: %f\n", write_duration);
        }
    }
    _aligned_free(buffer_aligned);
    buffer_aligned = nullptr;

    CloseHandle(pfile);
    this->number_elements_touched = number_written;
    this->generation_duration = generation_duration / freq.QuadPart;
    this->write_duration = write_duration / freq.QuadPart;
    return 0;
}


int external_sort::sort_file()
{
    LARGE_INTEGER start = { 0 }, end = { 0 }, freq = { 0 }, num_bytes_written = { 0 };;

    unsigned int* buffer = (unsigned int*)_aligned_malloc(static_cast<size_t>(this->buffer_size) * 4, this->bytes_per_sector);
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
    else {
        while (number_read < this->file_size) {
            // populate buffer
            QueryPerformanceCounter(&start);
            DWORD num_bytes_touched;
            bool was_success = ReadFile(old_file, buffer, sizeof(unsigned int) * this->buffer_size, &num_bytes_touched, NULL);
            QueryPerformanceCounter(&end);
            if (!(was_success)) {
                printf("%s: Failed reading from populated file with %d\n", __FUNCTION__, GetLastError());
                return 1;
            }
            read_duration += end.QuadPart - start.QuadPart;
            number_read = number_read + num_bytes_touched / sizeof(unsigned int);

            // sort the buffer and get time info
            QueryPerformanceCounter(&start);
            std::sort(buffer, buffer + this->buffer_size);
            QueryPerformanceCounter(&end);
            sort_duration += end.QuadPart - start.QuadPart;

            was_success = WriteFile(chunk_sorted_file, buffer, sizeof(unsigned int) * this->buffer_size, &num_bytes_touched, NULL);
            if (!(was_success)) {
                printf("%s: Failed writing to new file for sort output with %d\n", __FUNCTION__, GetLastError());
                return 1;
            }
            num_bytes_written.QuadPart = num_bytes_written.QuadPart + num_bytes_touched;
            written = written + num_bytes_touched / sizeof(unsigned int);

            if (this->debug) {
                printf("    file_size = %llu\n", this->file_size);
                printf("    num_bytes_touched = %d\n", num_bytes_touched);
                printf("    number_read = %llu\n", number_read);
                printf("    written = %llu\n", written);
                printf("    num_bytes_written.QuadPart = %llu\n", num_bytes_written.QuadPart);
                printf("    bufsize = %d", this->buffer_size);
            }
        }
        if (this->debug)
        {
            printf("Sort Duration: %f\n", sort_duration);
            printf("Read Duration: %f\n", read_duration);
        }
    }

    _aligned_free(buffer);
    buffer = nullptr;
    CloseHandle(old_file);
    CloseHandle(chunk_sorted_file);

    this->number_elements_touched = number_read;
    this->sort_duration = sort_duration / freq.QuadPart;
    this->read_duration = read_duration / freq.QuadPart;
    return 0;
}


int external_sort::merge_sort()
{
    // STEPS
    //      1) Determine how many chunks were made and how much memory to allocate for each chunk
    //              Do we assume the system has 1 GB of RAM, parameter in class 
    //              # chunks = FILE_SIZE / BUFFER_SIZE
    //      2) The number of vals from each chunk to initialize the array is 
    //              #_of_vals_each = BUFFER_SIZE / num_chunks
    //      3) Create an array of size 1 GB and populate it with the first val from each chunk
    //              Problem: array needs to store MinHeapNodes, which are size 12 vs unsigned int size 4
    //      4) Insert first array into MinHeap, pop minimum's val to another buffer of BUFFER_SIZE. Insert next value from the same chunk
    //      5) When second buffer is full, append those values to the new file
    //      6) Repeat steps 3-5 until first array is empty

    /*
        Merge sort Version 1 Steps (i.e., all sizes line up for an easy implementation)
        1)  Create an array of size num_chunks of MinHeapNodes and an array of size buffersize 
        2)  Create MinHeapNodes with the first val from every chunk and insert into array
        3)  Create MinHeap from the array
        4)  While the MinHeap is not empty:
                While the buffersize array is not full:
                    i)  Pop heap and take the val from that node and append it to the buffersize array
                Write buffersize array to the end of the file and "empty" it
    */


    unsigned long long num_chunks = this->file_size / this->buffer_size;
    if (file_size % buffer_size != 0)
    {
        printf("%s: FILE_SIZE % BUFFER_SIZE == 0 expected, FILE_SIZE = %llu, BUFFER_SIZE = %u\n", __FUNCTION__, this->file_size, this->buffer_size);
    }
    printf("FILE_SIZE / BUFFER_SIZE = %llu\n", this->file_size / this->buffer_size);
    printf("BUFFER_SIZE / num_chunks = %llu\n", this->buffer_size / num_chunks);

    /*HANDLE chunk_sorted_file = CreateFileA((LPCSTR)this->chunk_sorted_fname, GENERIC_READ, 0, 0, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL | FILE_FLAG_NO_BUFFERING, NULL);
    HANDLE full_sorted_file = CreateFileA((LPCSTR)this->full_sorted_fname, GENERIC_WRITE, 0, 0, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL | FILE_FLAG_NO_BUFFERING, NULL);
    if (chunk_sorted_file == INVALID_HANDLE_VALUE) {
        printf("%s: Failed opening populated file with %d\n", __FUNCTION__, GetLastError());
        return 1;
    }
    else if (full_sorted_file == INVALID_HANDLE_VALUE) {
        printf("%s: Failed opening new file for mergesort output with %d\n", __FUNCTION__, GetLastError());
        return 1;
    }*/

    printf("sizeof(MinHeapNode) = %u\n", sizeof(MinHeapNode));


    /*CloseHandle(chunk_sorted_file);
    CloseHandle(full_sorted_file);*/
    return 0;
}

void external_sort::print_metrics()
{
    printf("\n");
    printf("Averages over %d runs\n", this->num_runs);
    printf("    Generation time: %f s\n", this->total_generate_time / this->num_runs);
    printf("    Generation rate: %f million keys/s\n", this->file_size * this->num_runs / (this->total_generate_time * 1e6));
    printf("    Write time:      %f s\n", this->total_write_time / this->num_runs);
    printf("    Write rate:      %f MB/s\n", this->file_size * this->num_runs * sizeof(unsigned int) / (this->total_write_time * 1e6));
    printf("    Sort time:       %f s\n", this->total_sort_time / this->num_runs);
    printf("    Sort rate:       %f MB/s\n", this->file_size * this->num_runs * sizeof(unsigned int) / (this->total_sort_time * 1e6));
    printf("    Sort rate:       %f million keys/s\n", this->file_size * this->num_runs / (this->total_sort_time * 1e6));
    printf("    Read time:       %f s\n", this->total_read_time / this->num_runs);
    printf("    Read rate:       %f MB/s\n", this->file_size * this->num_runs * sizeof(unsigned int) / (this->total_read_time * 1e6));
}

int external_sort::generate_averages()
{
    for (int i = 0; i < this->num_runs; i++)
    {
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
    }
    return 0;
}
