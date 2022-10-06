#include "external_sort.h"
#include "windows.h"
#include <stdio.h>      
#include <stdlib.h>  


#define CREATE_VAR1(X,Y) X##Y 
#define CREATE_VAR(X,Y) CREATE_VAR1(X,Y)

#define MAKE_KEY(k)                             \
    next##k = a * next##k + c;                  \
    buffer_aligned[i + k] = next##k;            \


external_sort::external_sort() 
{

}


int external_sort::write_file()
{
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

    unsigned int* buffer_aligned = (unsigned int*)_aligned_malloc(static_cast<size_t>(this->BUFFER_SIZE) * 4, this->bytes_per_sector);
    double generation_duration = 0, write_duration = 0;

    HANDLE pfile = CreateFileA(this->fname, GENERIC_WRITE, 0, 0, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL | FILE_FLAG_NO_BUFFERING, NULL);
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
        while (number_written < this->FILE_SIZE) {
            // populate buffer - utilizes the Linear Congruential Generator method
            // https://en.wikipedia.org/wiki/Linear_congruential_generator
            if (!QueryPerformanceCounter(&start))
            {
                printf("%s: Error generation start time with code %d\n", __FUNCTION__, GetLastError());
            }
            for (int i = 0; i < this->BUFFER_SIZE; i += 8) {
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

            if (this->GIVE_VALS) {
                for (int i = 0; i < this->BUFFER_SIZE; i++) {
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
            BOOL was_success = WriteFile(pfile, buffer_aligned, sizeof(unsigned int) * this->BUFFER_SIZE, &num_bytes_written, NULL);
            if (!QueryPerformanceCounter(&end))
            {
                printf("%s: Error write end time with code %d\n", __FUNCTION__, GetLastError());
            }
            if (!(was_success)) {
                printf("__FUNCTION__write_file(): Failed writing to file with %d\n", GetLastError());
                return 1;
            }
            write_duration += end.QuadPart - start.QuadPart;

            number_written += num_bytes_written / sizeof(unsigned int);
            if (this->DEBUG) {
                printf("    number_written = %llu\n", number_written);
                printf("    bufsize = %d\n", this->BUFFER_SIZE);
            }
        }
        if (this->DEBUG)
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

    unsigned int* buffer = (unsigned int*)_aligned_malloc(static_cast<size_t>(this->BUFFER_SIZE) * 4, this->bytes_per_sector);
    unsigned long long int written = 0, number_read = 0;
    double sort_duration = 0, read_duration = 0;

    if (!QueryPerformanceFrequency(&freq))
    {
        printf("%s: Error getting freq with code %d\n", __FUNCTION__, GetLastError());
    }

    HANDLE old_file = CreateFileA((LPCSTR)this->fname, GENERIC_READ, 0, 0, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL | FILE_FLAG_NO_BUFFERING, NULL);
    HANDLE chunk_sorted_file = CreateFileA((LPCSTR)this->sorted_fname, GENERIC_WRITE, 0, 0, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL | FILE_FLAG_NO_BUFFERING, NULL);

    if (old_file == INVALID_HANDLE_VALUE) {
        printf("__FUNCTION__sort_file(): Failed opening populated file with %d\n", GetLastError());
        return 1;
    }
    else if (chunk_sorted_file == INVALID_HANDLE_VALUE) {
        printf("__FUNCTION__sort_file(): Failed opening new file for sort output with %d\n", GetLastError());
        return 1;
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


int external_sort::merge_sort()
{

}

int external_sort::print_metrics()
{

}

int external_sort::generate_averages()
{

}
