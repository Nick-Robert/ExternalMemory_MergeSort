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

// g++ -std=c++17 file.cpp
/*

    1) File generation
    2) Break file into chunks based off memory size
    3) Sort each chunk
        Save the sorted version into memory
    4) Mergesort each chunk
        In one pass using k-way merge
    5) Validation and optimization

    Notes:
        1) Visual studio release mode
        2) In create file. option to bypass os buffering
        3) Disable unicode
        4) Test number generation again

*/


std::pair<unsigned long long int, std::pair<double, double>> write_file(const unsigned long long int file_size, const int bufsize, const char fname[], const unsigned long bytes_per_sector, bool validate = false, bool debug = false)
{
    std::cout << "__FUNCTION__ write_file()" << std::endl;
    std::pair<unsigned long long int, std::pair<double, double>> write_return;
    std::chrono::time_point<std::chrono::system_clock> start, end;
    std::chrono::duration<double> duration;
    unsigned long long int number_written = 0;

    unsigned int* buffer_aligned = (unsigned int*)_aligned_malloc(bufsize * 4, bytes_per_sector);
    double generation_duration = 0;
    double write_duration = 0;

    int used_buf = bufsize;

    HANDLE pfile = CreateFileA((LPCSTR)fname, GENERIC_WRITE, 0, 0, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL | FILE_FLAG_NO_BUFFERING, NULL);

    if (pfile == INVALID_HANDLE_VALUE) {
        perror("Error creating file");
        write_return.second.first = -1.0;
        return write_return;
    }
    else {
        unsigned int x_seed = rand();
        while (number_written < file_size) {
            // ASK PROFESSOR IF THIS APPROACH IS FINE
            // used_buf will always need to be the needed size, even if there's fewer things to write
            // so, the file created may end up being a little larger than specified by the user

            // populate buffer
            // utilizes the Linear Congruential Generator method
            // https://en.wikipedia.org/wiki/Linear_congruential_generator
            start = std::chrono::system_clock::now();

            const unsigned int a = 214013;
            // const unsigned int m = 4096*4;
            const unsigned int c = 2531011;
            for (int i = 0; i < used_buf; i++) {
                unsigned int next_x = (a * x_seed + c) /* %m */;
                buffer_aligned[i] = next_x;
                x_seed = next_x;
                if (validate && i < 7) {
                    std::cout << "Generated: " << next_x << std::endl;
                }
            }
            if (validate) {
                for (int i = 0; i < used_buf; i++) {
                    if (i < 7) {
                        std::cout << "buffer_aligned[i] = " << buffer_aligned[i] << std::endl;
                        // std::cout << "buffer[i] = " << buffer[i] << std::endl;
                    }
                }
            }
            end = std::chrono::system_clock::now();
            duration = end - start;
            generation_duration = generation_duration + duration.count();

            start = std::chrono::system_clock::now();
            // int temp = fwrite(buffer, sizeof(int), used_buf, pfile);
            DWORD temp;
            BOOL was_success = WriteFile(pfile, buffer_aligned, sizeof(unsigned int) * used_buf, &temp, NULL);
            end = std::chrono::system_clock::now();
            if (!(was_success)) {
                // perror("Error writing to file");
                std::cout << "Error writing to file" << std::endl;
                std::cout << GetLastError() << std::endl;
                write_return.second.first = -1.0;
                return write_return;
            }
            duration = end - start;
            write_duration = write_duration + duration.count();

            number_written = number_written + temp;
            if (debug) {
                std::cout << "  number_written = " << number_written << std::endl;
                std::cout << "  bufsize = " << bufsize << std::endl;
                std::cout << "  used_buf = " << used_buf << std::endl;
            }
        }
        std::cout << "Generation Duration: " << generation_duration << std::endl;
        std::cout << "Write Duration: " << write_duration << std::endl;
        // fclose(pfile);
    }
    // delete buffer;
    _aligned_free(buffer_aligned);
    // buffer = nullptr;
    buffer_aligned = nullptr;

    CloseHandle(pfile);

    // write_return.first = written;
    write_return.first = number_written;
    write_return.second.first = generation_duration;
    write_return.second.second = write_duration;
    return write_return;
}


std::pair<unsigned long long int, std::pair<double, double>> sort_file(const unsigned long long int file_size, const int bufsize, const char fname[], const char new_fname[], const unsigned long bytes_per_sector, bool validate = false, bool debug = false) {
    std::cout << "__FUNCTION__ sort_file()" << std::endl;
    std::pair<unsigned long long int, std::pair<double, double>> write_return;
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
        perror("Error reading file for insertion sorting");
        write_return.second.first = -1.0;
        return write_return;
    }
    else if (new_file == INVALID_HANDLE_VALUE) {
        perror("Error writing file for insertion sorting");
        write_return.second.first = -1.0;
        return write_return;
    }
    else {
        while (number_read < file_size) {
            /*if ((file_size - number_read) % bufsize == 0) {
                used_buf = bufsize;
            }
            else {
                used_buf = (file_size - number_read) % bufsize;
            }*/

            // populate buffer
            start = std::chrono::high_resolution_clock::now();
            DWORD temp;
            bool was_success = ReadFile(pfile, buffer, sizeof(unsigned int) * used_buf, &temp, NULL);
            end = std::chrono::high_resolution_clock::now();
            if (!(was_success)) {
                // perror("Error writing to file");
                std::cout << "Error reading file" << std::endl;
                std::cout << GetLastError() << std::endl;
                write_return.second.first = -1.0;
                return write_return;
            }
            duration = end - start;
            read_duration = read_duration + duration.count();
            number_read = number_read + temp;


            // sort the buffer
            // utilizes insertion sort, reference https://www.geeksforgeeks.org/insertion-sort/
            // sorts each buffer and writes that to the sorted file
            // std::sort? certain implementations of quick sort?
            start = std::chrono::high_resolution_clock::now();
            /*int i, j;
            unsigned int key;
            for (i = 1; i < used_buf; i++) {
                key = buffer[i];
                j = i - 1;

                while (j >= 0 && buffer[j] > key) {
                    buffer[j + 1] = buffer[j];
                    j = j - 1;
                }
                buffer[j + 1] = key;
            }*/
            std::sort(buffer, buffer + bufsize);
            end = std::chrono::high_resolution_clock::now();
            duration = end - start;
            sort_duration = sort_duration + duration.count();

            was_success = WriteFile(new_file, buffer, sizeof(unsigned int) * used_buf, &temp, NULL);
            if (!(was_success)) {
                std::cout << "Error writing sorted to file" << std::endl;
                std::cout << GetLastError() << std::endl;
                write_return.second.first = -1.0;
                return write_return;
            }
            num_bytes_written.QuadPart = num_bytes_written.QuadPart + temp;
            written = written + temp;

            if (debug) {
                std::cout << "  file_size = " << file_size << std::endl;
                std::cout << "  temp = " << temp << std::endl;
                std::cout << "  number_read = " << number_read << std::endl;
                std::cout << "  written = " << written << std::endl;
                std::cout << "  num_bytes_written.QuadPart = " << num_bytes_written.QuadPart << std::endl;
                std::cout << "  bufsize = " << bufsize << std::endl;
            }
        }
        std::cout << "Sort Duration: " << sort_duration << std::endl;
        std::cout << "Read Duration: " << read_duration << std::endl;
    }

    _aligned_free(buffer);
    buffer = nullptr;
    CloseHandle(pfile);
    CloseHandle(new_file);

    write_return.first = number_read;
    write_return.second.first = sort_duration;
    write_return.second.second = read_duration;
    return write_return;
}


std::pair<unsigned long long int, std::pair<double, double>> external_sort(const unsigned long long int file_size, const int bufsize, const char fname[], const char new_fname[], const unsigned long bytes_per_sector, bool validate = false, bool debug = false) {

}
unsigned long long int read_file(const unsigned long long int file_size, const int bufsize, const char fname[], bool validate = false, bool debug = false) {
    unsigned int* buffer = new unsigned int[bufsize];
    unsigned long long int read = 0;
    FILE* pfile = fopen(fname, "rb");
    if (pfile == NULL) {
        perror("Error opening file");
        return -1;
    }
    else {
        unsigned long long int num_read = 0;
        while (num_read < file_size) {
            DWORD temp = fread(buffer, sizeof(unsigned int), bufsize, pfile);
            if (ferror(pfile)) {
                perror("Error: ");
                return -1;
            }
            else if (validate) {
                std::cout << "  temp = " << temp << std::endl;
                for (int i = 0; i < bufsize; i++) {
                    if (i < 7) {
                        std::cout << "Read: " << buffer[i] << std::endl;
                    }
                }
            }
            read = read + (unsigned long long int)temp * 4;
            num_read = num_read + bufsize;
            if (debug) {
                std::cout << "  num_read = " << num_read << std::endl;
                std::cout << "  read = " << read << std::endl;
                std::cout << "  bufsize = " << bufsize << std::endl;
            }
        }
        fclose(pfile);
    }
    delete[] buffer;
    buffer = nullptr;
    return read;
}


int main(int argc, char** argv) {
    // INPUT ASSUMED TO BE IN # OF INTS
    // Conversions:
    //      1 GiB = 1073741824 bytes (268435456 ints)
    //      1 MiB = 1048576 bytes (262144 ints)
    //      1 KiB = 1024 bytes (256 ints)
    //          int has 4 bytes (32 bits), so 1 KiB of int array is 256 elements

    // FORMAT:
    //      ./driver.exe [# of ints] [TEST_SORT] [GIVE_VALS] [TEST_READ] [DEBUG]

    //      ./driver.exe 2684354560

    // PARAMETERS
    if (argc != 2 && argc != 3 && argc != 6) {
        std::cout << "Missing file size input (in bytes) or a debug indicator" << std::endl;
        return 1;
    }
    unsigned long long int FILE_SIZE = strtoull(argv[1], nullptr, 10);
    // int BUFFER_SIZE = strtoull(argv[2], nullptr, 10) * strtoull(argv[3], nullptr, 10); // 256 * 4

    // buffer size is hard coded to be 1MB, which is 262144 ints
    int BUFFER_SIZE = 262144;
    char fname[] = "test.bin";
    char sorted_fname[] = "sorted_test.bin";

    std::cout << "FILE_SIZE = " << FILE_SIZE << std::endl;
    std::cout << "BUFFER_SIZE = " << BUFFER_SIZE << std::endl;

    bool TEST_SORT = false;
    bool GIVE_VALS = false;
    bool TEST_READ = false;
    bool DEBUG = false;
    if (argc == 3) {
        TEST_SORT = atoi(argv[2]);
    }
    else if (argc == 6) {
        TEST_SORT = atoi(argv[2]);
        GIVE_VALS = atoi(argv[3]);
        DEBUG = atoi(argv[4]);
        TEST_READ = atoi(argv[5]);
    }

    unsigned long bytes_per_sector;
    unsigned long num_free_sectors;
    BOOL succeeded;

    succeeded = GetDiskFreeSpaceA(NULL, NULL, &bytes_per_sector, &num_free_sectors, NULL);
    if (!succeeded) {
        std::cout << "Error in GetDiskFreeSpaceA" << std::endl;
        std::cout << GetLastError() << std::endl;
        return 1;
    }
    std::cout << "BYTES_PER_SECTOR = " << bytes_per_sector << std::endl;
    std::cout << "FILE_SIZE % BYTES_PER_SECTOR = " << FILE_SIZE % bytes_per_sector << std::endl;
    std::cout << std::endl;


    // supporting variables
    std::pair<unsigned long long int, std::pair<double, double>> write_return, sort_return;
    int num = 5;
    unsigned long long int num_written;
    double total_generate_time = 0;
    double total_write_time = 0;
    double total_sort_time = 0;
    double generate_time;
    double write_time;

    srand(time(0));
    for (int i = 0; i < num; i++) {
        write_return = write_file(FILE_SIZE, BUFFER_SIZE, fname, bytes_per_sector, GIVE_VALS, DEBUG);

        num_written = write_return.first;
        generate_time = write_return.second.first;
        write_time = write_return.second.second;

        if (generate_time == -1)
        {
            return 1;
        }
        std::cout << "Number of bytes written: " << num_written << std::endl;
        total_generate_time = total_generate_time + generate_time;
        total_write_time = total_write_time + write_time;

        if (TEST_SORT)
        {
            sort_return = sort_file(FILE_SIZE, BUFFER_SIZE, fname, sorted_fname, bytes_per_sector, GIVE_VALS, DEBUG);
            if (sort_return.second.first == -1) {
                return 1;
            }
            total_sort_time = total_sort_time + sort_return.second.first;
        }

        if (TEST_READ)
        {
            std::chrono::time_point<std::chrono::system_clock> read_start, read_end;
            read_start = std::chrono::system_clock::now();
            unsigned long long int num_read = read_file(FILE_SIZE, BUFFER_SIZE, fname, GIVE_VALS, DEBUG);
            read_end = std::chrono::system_clock::now();
            if (num_read == -1)
            {
                return 1;
            }
            else {
                std::cout << "Number of bytes read: " << num_read << std::endl;
            }
            std::chrono::duration<double> es_read = read_end - read_start;
            std::cout << "Read Duration: " << es_read.count() << std::endl;
        }

    }
    std::cout << std::endl;
    std::cout << "Averages over " << num << " files" << std::endl;
    std::cout << "  Average generate time:  " << total_generate_time / num << std::endl;
    std::cout << "  Average sort time:      " << total_sort_time / num << std::endl;
    std::cout << "  Average write time:     " << total_write_time / num << std::endl;

    return 0;
}