/*
    262144 1
    268435456 1
    1342177280 1
    2684354560 1
    5368709120 1
*/
#include <stdio.h>      
#include <stdlib.h>     
#include <time.h>       
#include <iostream>
#include <cstdlib>
#include <chrono>
#include <ctime>
#include <vector>
#include <typeinfo>
#include <windows.h>
#include <algorithm>
#include "external_sort.h"

#define CREATE_VAR1(X,Y) X##Y 
#define CREATE_VAR(X,Y) CREATE_VAR1(X,Y)

#define MAKE_KEY(k)                             \
    next##k = a * next##k + c;                  \
    buffer_aligned[i + k] = next##k;            \


struct return_vals {
    unsigned long long int number_elements_touched;
    double generation_duration;
    double write_duration;
    double sort_duration;
    double read_duration;
    int was_error;
};


return_vals write_file(const unsigned long long int file_size, const int bufsize, const char fname[], const unsigned long bytes_per_sector, bool validate=false, bool debug=false)
{
    return_vals write_return = { 0 };
    LARGE_INTEGER start = { 0 }, end = { 0 }, freq = { 0 };
    if (!QueryPerformanceFrequency(&freq))
    {
        printf("%s: Error getting freq with code %d\n", __FUNCTION__, GetLastError());
    }
    if (!QueryPerformanceCounter(&start))
    {
        printf("%s: Error getting start time with code %d\n", __FUNCTION__, GetLastError());
    }
    unsigned long long int number_written = 0;

    unsigned int* buffer_aligned = (unsigned int*)_aligned_malloc(static_cast<size_t>(bufsize) * 4, bytes_per_sector);
    double generation_duration = 0, write_duration = 0;

    HANDLE pfile = CreateFileA(fname, GENERIC_WRITE, 0, 0, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL | FILE_FLAG_NO_BUFFERING, NULL);
    if (pfile == INVALID_HANDLE_VALUE) {
        printf("__FUNCTION__write_file(): Failed opening file with %d\n", GetLastError());
        write_return.was_error = 1;
        return write_return;
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
        while (number_written < file_size) {
            // populate buffer - utilizes the Linear Congruential Generator method
            // https://en.wikipedia.org/wiki/Linear_congruential_generator
            if (!QueryPerformanceCounter(&start))
            {
                printf("%s: Error generation start time with code %d\n", __FUNCTION__, GetLastError());
            }
            for (int i = 0; i < bufsize; i+=8) {
                MAKE_KEY(0);
                MAKE_KEY(1);
                MAKE_KEY(2);
                MAKE_KEY(3);
                MAKE_KEY(4);
                MAKE_KEY(5);
                MAKE_KEY(6);
                MAKE_KEY(7);
            }
            if (!QueryPerformanceCounter(&end))
            {
                printf("%s: Error generation end time with code %d\n", __FUNCTION__, GetLastError());
            }
            generation_duration += end.QuadPart - start.QuadPart;

            if (validate) {
                for (int i = 0; i < bufsize; i++) {
                    if (i < 7) {
                        printf("buffer_aligned[%d] = %u\n", i, buffer_aligned[i]);
                    }
                }
            }

            DWORD num_bytes_written;
            if (!QueryPerformanceCounter(&start))
            {
                printf("%s: Error write start time with code %d\n", __FUNCTION__, GetLastError());
            }
            BOOL was_success = WriteFile(pfile, buffer_aligned, sizeof(unsigned int) * bufsize, &num_bytes_written, NULL);
            if (!QueryPerformanceCounter(&end))
            {
                printf("%s: Error write end time with code %d\n", __FUNCTION__, GetLastError());
            }
            if (!(was_success)) {
                printf("__FUNCTION__write_file(): Failed writing to file with %d\n", GetLastError());
                write_return.was_error = 1;
                return write_return;
            }
            write_duration += end.QuadPart - start.QuadPart;

            number_written += num_bytes_written / sizeof(unsigned int);
            if (debug) {
                printf("    number_written = %llu\n", number_written);
                printf("    bufsize = %d\n", bufsize);
            }
        }
        if (debug)
        {
            printf("Generation Duration: %f\n", generation_duration);
            printf("Write Duration: %f\n", write_duration);
        }
    }
    _aligned_free(buffer_aligned);
    buffer_aligned = nullptr;

    CloseHandle(pfile);
    write_return.number_elements_touched = number_written;
    write_return.generation_duration = generation_duration / freq.QuadPart;
    write_return.write_duration = write_duration / freq.QuadPart;
    return write_return;
}


return_vals sort_file(const unsigned long long int file_size, const int bufsize, const char fname[], const char new_fname[], const unsigned long bytes_per_sector, bool validate = false, bool debug = false) 
{
    return_vals sort_return = { 0 };
    LARGE_INTEGER start = { 0 }, end = { 0 }, freq = { 0 }, num_bytes_written = { 0 };;

    unsigned int* buffer = (unsigned int*)_aligned_malloc(static_cast<size_t>(bufsize) * 4, bytes_per_sector);
    unsigned long long int written = 0, number_read = 0;
    double sort_duration = 0, read_duration = 0;

    if (!QueryPerformanceFrequency(&freq))
    {
        printf("%s: Error getting freq with code %d\n", __FUNCTION__, GetLastError());
    }

    HANDLE old_file = CreateFileA((LPCSTR)fname, GENERIC_READ, 0, 0, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL | FILE_FLAG_NO_BUFFERING, NULL);
    HANDLE chunk_sorted_file = CreateFileA((LPCSTR)new_fname, GENERIC_WRITE, 0, 0, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL | FILE_FLAG_NO_BUFFERING, NULL);

    if (old_file == INVALID_HANDLE_VALUE) {
        printf("__FUNCTION__sort_file(): Failed opening populated file with %d\n", GetLastError());
        sort_return.was_error = 1;
        return sort_return;
    }
    else if (chunk_sorted_file == INVALID_HANDLE_VALUE) {
        printf("__FUNCTION__sort_file(): Failed opening new file for sort output with %d\n", GetLastError());
        sort_return.was_error = 1;
        return sort_return;
    }
    else {
        while (number_read < file_size) {
            // populate buffer
            if (!QueryPerformanceCounter(&start))
            {
                printf("%s: Error read start time with code %d\n", __FUNCTION__, GetLastError());
            }
            DWORD num_bytes_touched;
            bool was_success = ReadFile(old_file, buffer, sizeof(unsigned int) * bufsize, &num_bytes_touched, NULL);
            if (!QueryPerformanceCounter(&end))
            {
                printf("%s: Error read end time with code %d\n", __FUNCTION__, GetLastError());
            }
            if (!(was_success)) {
                printf("__FUNCTION__sort_file(): Failed reading from populated file with %d\n", GetLastError());
                sort_return.was_error = 1;
                return sort_return;
            }
            read_duration += end.QuadPart - start.QuadPart;
            number_read = number_read + num_bytes_touched / sizeof(unsigned int);

            // sort the buffer and get time info
            if (!QueryPerformanceCounter(&start))
            {
                printf("%s: Error sort start time with code %d\n", __FUNCTION__, GetLastError());
            }
            std::sort(buffer, buffer + bufsize);
            if (!QueryPerformanceCounter(&end))
            {
                printf("%s: Error sort end time with code %d\n", __FUNCTION__, GetLastError());
            }
            sort_duration += end.QuadPart - start.QuadPart;

            was_success = WriteFile(chunk_sorted_file, buffer, sizeof(unsigned int) * bufsize, &num_bytes_touched, NULL);
            if (!(was_success)) {
                printf("__FUNCTION__sort_file(): Failed writing to new file for sort output with %d\n", GetLastError());
                sort_return.was_error = 1;
                return sort_return;
            }
            num_bytes_written.QuadPart = num_bytes_written.QuadPart + num_bytes_touched;
            written = written + num_bytes_touched / sizeof(unsigned int);

            if (debug) {
                printf("    file_size = %llu\n", file_size);
                printf("    num_bytes_touched = %d\n", num_bytes_touched);
                printf("    number_read = %llu\n", number_read);
                printf("    written = %llu\n", written);
                printf("    num_bytes_written.QuadPart = %llu\n", num_bytes_written.QuadPart);
                printf("    bufsize = %d", bufsize);
            }
        }
        if (debug)
        {
            printf("Sort Duration: %f\n", sort_duration);
            printf("Read Duration: %f\n", read_duration);
        }
    }

    _aligned_free(buffer);
    buffer = nullptr;
    CloseHandle(old_file);
    CloseHandle(chunk_sorted_file);

    sort_return.number_elements_touched = number_read;
    sort_return.sort_duration = sort_duration / freq.QuadPart;
    sort_return.read_duration = read_duration / freq.QuadPart;
    return sort_return;
}


return_vals external_sort(const unsigned long long int file_size, const int bufsize, const char fname[], const char new_fname[], const unsigned long bytes_per_sector, bool validate = false, bool debug = false) 
{
    return_vals sort_return = { 0 };



    return sort_return;
}


int main(int argc, char** argv) 
{
    // INPUT ASSUMED TO BE IN # OF INTS
    // Conversions:
    //      1 GiB = 1073741824 bytes (268435456 ints)
    //      1 MiB = 1048576 bytes (262144 ints)
    //      1 KiB = 1024 bytes (256 ints)
    //          int has 4 bytes (32 bits), so 1 KiB of int array is 256 elements

    // FORMAT:
    //      ./driver.exe [# of ints] [TEST_SORT] [GIVE_VALS] [DEBUG]

    //      ./driver.exe 2684354560

    if (argc != 2 && argc != 3 && argc != 5) {
        std::cout << "Missing file size input (in bytes) or a debug indicator" << std::endl;
        return 1;
    }
    unsigned long long int FILE_SIZE = strtoull(argv[1], nullptr, 10);

    unsigned int BUFFER_SIZE = (1<<20) / sizeof(unsigned int);
    char fname[] = "output_files\\test.bin";
    char sorted_fname[] = "output_files\\sorted_test.bin";

    bool TEST_SORT = false;
    bool GIVE_VALS = false;
    bool DEBUG = false;
    if (argc == 3) {
        TEST_SORT = atoi(argv[2]);
    }
    else if (argc == 5) {
        TEST_SORT = atoi(argv[2]);
        GIVE_VALS = atoi(argv[3]);
        DEBUG = atoi(argv[4]);
    }

    unsigned long bytes_per_sector;
    unsigned long num_free_sectors;
    BOOL succeeded = GetDiskFreeSpaceA(NULL, NULL, &bytes_per_sector, &num_free_sectors, NULL);
    if (!succeeded) {
        printf("__FUNCTION__main(): Failed getting disk information with %d\n", GetLastError());
        return 1;
    }
    if (DEBUG)
    {
        printf("FILE_SIZE = %llu\n", FILE_SIZE);
        printf("BUFFER_SIZE = %u\n", BUFFER_SIZE);
        printf("BYTES_PER_SECTOR = %lu\n", bytes_per_sector);
        printf("FILE_SIZE % BYTES_PER_SECTOR = %lu\n", FILE_SIZE % bytes_per_sector);
        printf("\n");
    }


    return_vals write_return, sort_return;
    int num_runs = 1;
    double total_generate_time = 0;
    double total_write_time = 0;
    double total_sort_time = 0;
    double total_read_time = 0;

    srand((unsigned int)time(0));
    for (int i = 0; i < num_runs; i++) {
        write_return = write_file(FILE_SIZE, BUFFER_SIZE, fname, bytes_per_sector, GIVE_VALS, DEBUG);

        if (write_return.was_error == 1)
        {
            return 1;
        }
        if (DEBUG)
        {
            printf("Number of bytes written = %llu\n", write_return.number_elements_touched);
        }

        total_generate_time += write_return.generation_duration;
        total_write_time += write_return.write_duration;

        if (TEST_SORT)
        {
            sort_return = sort_file(FILE_SIZE, BUFFER_SIZE, fname, sorted_fname, bytes_per_sector, GIVE_VALS, DEBUG);
            if (sort_return.was_error == 1) {
                return 1;
            }
            total_sort_time += sort_return.sort_duration;
            total_read_time += sort_return.read_duration;
        }
    }

    printf("\n");
    printf("Averages over %d runs\n", num_runs);
    printf("    Generation time: %f s\n", total_generate_time / num_runs);
    printf("    Generation rate: %f million keys/s\n", FILE_SIZE * num_runs / (total_generate_time * 1e6));
    printf("    Write time:      %f s\n", total_write_time / num_runs);
    printf("    Write rate:      %f MB/s\n", FILE_SIZE * num_runs * sizeof(unsigned int) / (total_write_time * 1e6));
    printf("    Sort time:       %f s\n", total_sort_time / num_runs);
    printf("    Sort rate:       %f MB/s\n", FILE_SIZE * num_runs * sizeof(unsigned int) / (total_sort_time * 1e6));
    printf("    Sort rate:       %f million keys/s\n", FILE_SIZE * num_runs / (total_sort_time * 1e6));
    printf("    Read time:       %f s\n", total_read_time / num_runs);
    printf("    Read rate:       %f MB/s\n", FILE_SIZE * num_runs * sizeof(unsigned int) / (total_read_time * 1e6));

    return 0;
}