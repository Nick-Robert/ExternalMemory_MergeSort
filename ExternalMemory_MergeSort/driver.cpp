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
    std::chrono::time_point<std::chrono::system_clock> start, end;
    std::chrono::duration<double> duration;
    unsigned long long int number_written = 0;

    unsigned int* buffer_aligned = (unsigned int*)_aligned_malloc(static_cast<size_t>(bufsize) * 4, bytes_per_sector);
    double generation_duration = 0;
    double write_duration = 0;

    HANDLE pfile = CreateFileA(fname, GENERIC_WRITE, 0, 0, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL | FILE_FLAG_NO_BUFFERING, NULL);
    if (pfile == INVALID_HANDLE_VALUE) {
        printf("__FUNCTION__write_file(): Failed opening file with %d\n", GetLastError());
        write_return.was_error = 1;
        return write_return;
    }
    else {
        unsigned int next_x1 = rand(), next_x2 = rand(), next_x3 = rand(), next_x4 = rand();
        const unsigned int a = 214013;
        // const unsigned int m = 4096*4;
        const unsigned int c = 2531011;

        while (number_written < file_size) {
            // populate buffer - utilizes the Linear Congruential Generator method
            // https://en.wikipedia.org/wiki/Linear_congruential_generator
            start = std::chrono::system_clock::now();
            for (int i = 0; i < bufsize; i+=4) {
                next_x1 = (a * next_x1 + c);
                buffer_aligned[i] = next_x1;

                next_x2 = (a * next_x2 + c);
                buffer_aligned[i + 1] = next_x2;

                next_x3 = (a * next_x3 + c);
                buffer_aligned[i + 2] = next_x3;

                next_x4 = (a * next_x4 + c);
                buffer_aligned[i + 3] = next_x4;
            }
            end = std::chrono::system_clock::now();
            duration = end - start;
            generation_duration += duration.count();
            
            if (validate) {
                for (int i = 0; i < bufsize; i++) {
                    if (i < 7) {
                        std::cout << "buffer_aligned[i] = " << buffer_aligned[i] << std::endl;
                        // std::cout << "buffer[i] = " << buffer[i] << std::endl;
                    }
                }
            }

            DWORD num_bytes_written;
            start = std::chrono::system_clock::now();
            BOOL was_success = WriteFile(pfile, buffer_aligned, sizeof(unsigned int) * bufsize, &num_bytes_written, NULL);
            end = std::chrono::system_clock::now();
            if (!(was_success)) {
                // perror("Error writing to file");
                printf("__FUNCTION__write_file(): Failed writing to file with %d\n", GetLastError());
                write_return.was_error = 1;
                return write_return;
            }
            duration = end - start;
            write_duration += duration.count();

            number_written += num_bytes_written / sizeof(unsigned int);
            /*if (debug) {
                std::cout << "  number_written = " << number_written << std::endl;
                std::cout << "  bufsize = " << bufsize << std::endl;
            }*/
        }
        if (debug)
        {
            std::cout << "Generation Duration: " << generation_duration << std::endl;
            std::cout << "Write Duration: " << write_duration << std::endl;
        }
    }
    _aligned_free(buffer_aligned);
    buffer_aligned = nullptr;

    CloseHandle(pfile);

    write_return.number_elements_touched = number_written;
    write_return.generation_duration = generation_duration;
    write_return.write_duration = write_duration;
    return write_return;
}


return_vals sort_file(const unsigned long long int file_size, const int bufsize, const char fname[], const char new_fname[], const unsigned long bytes_per_sector, bool validate = false, bool debug = false) 
{
    return_vals sort_return = { 0 };
    std::chrono::time_point<std::chrono::high_resolution_clock> start, end;
    std::chrono::duration<double> duration;
    //int* buffer = new int[bufsize];
    unsigned int* buffer = (unsigned int*)_aligned_malloc(bufsize * 4, bytes_per_sector);
    unsigned long long int written = 0;
    unsigned long long int number_read = 0;
    double sort_duration = 0;
    double read_duration = 0;
    LARGE_INTEGER num_bytes_written{};
    num_bytes_written.QuadPart = 0;
    int used_buf = bufsize;

    HANDLE pfile = CreateFileA((LPCSTR)fname, GENERIC_READ, 0, 0, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL | FILE_FLAG_NO_BUFFERING, NULL);
    HANDLE new_file = CreateFileA((LPCSTR)new_fname, GENERIC_WRITE, 0, 0, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL | FILE_FLAG_NO_BUFFERING, NULL);

    if (pfile == INVALID_HANDLE_VALUE) {
        printf("__FUNCTION__sort_file(): Failed opening populated file with %d\n", GetLastError());
        sort_return.was_error = 1;
        return sort_return;
    }
    else if (new_file == INVALID_HANDLE_VALUE) {
        printf("__FUNCTION__sort_file(): Failed opening new file for sort output with %d\n", GetLastError());
        sort_return.was_error = 1;
        return sort_return;
    }
    else {
        while (number_read < file_size) {
            // populate buffer
            start = std::chrono::high_resolution_clock::now();
            DWORD num_bytes_touched;
            bool was_success = ReadFile(pfile, buffer, sizeof(unsigned int) * used_buf, &num_bytes_touched, NULL);
            end = std::chrono::high_resolution_clock::now();
            if (!(was_success)) {
                printf("__FUNCTION__sort_file(): Failed reading from populated file with %d\n", GetLastError());
                sort_return.was_error = 1;
                return sort_return;
            }
            duration = end - start;
            read_duration = read_duration + duration.count();
            number_read = number_read + num_bytes_touched / sizeof(unsigned int);


            // sort the buffer and get time info
            start = std::chrono::high_resolution_clock::now();
            std::sort(buffer, buffer + bufsize);
            end = std::chrono::high_resolution_clock::now();
            duration = end - start;
            sort_duration = sort_duration + duration.count();

            was_success = WriteFile(new_file, buffer, sizeof(unsigned int) * used_buf, &num_bytes_touched, NULL);
            if (!(was_success)) {
                printf("__FUNCTION__sort_file(): Failed writing to new file for sort output with %d\n", GetLastError());
                sort_return.was_error = 1;
                return sort_return;
            }
            num_bytes_written.QuadPart = num_bytes_written.QuadPart + num_bytes_touched;
            written = written + num_bytes_touched / sizeof(unsigned int);

            /*if (debug) {
                std::cout << "  file_size = " << file_size << std::endl;
                std::cout << "  num_bytes_touched = " << num_bytes_touched << std::endl;
                std::cout << "  number_read = " << number_read << std::endl;
                std::cout << "  written = " << written << std::endl;
                std::cout << "  num_bytes_written.QuadPart = " << num_bytes_written.QuadPart << std::endl;
                std::cout << "  bufsize = " << bufsize << std::endl;
            }*/
        }
        if (debug)
        {
            std::cout << "Sort Duration: " << sort_duration << std::endl;
            std::cout << "Read Duration: " << read_duration << std::endl;
        }
    }

    _aligned_free(buffer);
    buffer = nullptr;
    CloseHandle(pfile);
    CloseHandle(new_file);

    sort_return.number_elements_touched = number_read;
    sort_return.sort_duration = sort_duration;
    sort_return.read_duration = read_duration;
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
        std::cout << "FILE_SIZE = " << FILE_SIZE << std::endl;
        std::cout << "BUFFER_SIZE = " << BUFFER_SIZE << std::endl;
        std::cout << "BYTES_PER_SECTOR = " << bytes_per_sector << std::endl;
        std::cout << "FILE_SIZE % BYTES_PER_SECTOR = " << FILE_SIZE % bytes_per_sector << std::endl;
        std::cout << std::endl;
    }


    return_vals write_return, sort_return;
    int num_runs = 5;
    double total_generate_time = 0;
    double total_write_time = 0;
    double total_sort_time = 0;

    srand(time(0));
    for (int i = 0; i < num_runs; i++) {
        write_return = write_file(FILE_SIZE, BUFFER_SIZE, fname, bytes_per_sector, GIVE_VALS, DEBUG);

        if (write_return.was_error == 1)
        {
            return 1;
        }
        if (DEBUG)
        {
            std::cout << "Number of bytes written: " << write_return.number_elements_touched << std::endl;
        }

        total_generate_time = total_generate_time + write_return.generation_duration;
        total_write_time = total_write_time + write_return.write_duration;

        if (TEST_SORT)
        {
            sort_return = sort_file(FILE_SIZE, BUFFER_SIZE, fname, sorted_fname, bytes_per_sector, GIVE_VALS, DEBUG);
            if (sort_return.was_error == 1) {
                return 1;
            }
            total_sort_time = total_sort_time + sort_return.sort_duration;
        }
    }

    std::cout << std::endl;
    std::cout << "Averages over " << num_runs << " files" << std::endl;
    std::cout << "  Generation time: " << total_generate_time / num_runs << " s" << std::endl;
    std::cout << "  Generation rate: " << FILE_SIZE * num_runs / (total_generate_time * 1e6) << " million keys/s" << std::endl;
    std::cout << "  Sort time:       " << total_sort_time / num_runs << " s" << std::endl;
    std::cout << "  Sort rate:       " << FILE_SIZE * num_runs * sizeof(unsigned int) / (total_sort_time * 1e6) << " MB/s" << std::endl;
    std::cout << "  Sort rate:       " << FILE_SIZE * num_runs / (total_sort_time * 1e6) << " million keys/s" << std::endl;
    std::cout << "  Write time:      " << total_write_time / num_runs << " s" << std::endl;
    std::cout << "  Write rate:      " << FILE_SIZE * num_runs * sizeof(unsigned int) / (total_write_time * 1e6) << " MB/s" << std::endl;

    return 0;
}