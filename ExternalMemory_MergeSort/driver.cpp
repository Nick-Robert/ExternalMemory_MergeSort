/*
    512 1
    262144 1
    524288
    2883584 1
    268435456 1
    268697600 1
    1073741824 1
    1342177280 1
    2147483648 (8 GB)
    2684354560 1
    4294967296 (16 GB)
    
*/
#include <stdio.h>      
#include <windows.h>
#include "external_sort.h"
//#include "commons.h"
//#include "Writer.h"
//#include "utils.h"
//#include "merge_utils.h"
//#include "sorter.h"


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
        printf("Missing file size input (in bytes) or a debug indicator\n");
        return 1;
    }
    unsigned long long int fs = strtoull(argv[1], nullptr, 10);
    // lowest this can be is (static_cast<unsigned long long>(1) << 9) / sizeof(unsigned int); since it results in 512 bytes
    // MUST BE A MULTIPLE OF 512
    unsigned long long int ms = (static_cast<unsigned long long>(1) << 30) / sizeof(Itemtype);

    //unsigned int BUFFER_SIZE = (1<<20) / sizeof(unsigned int);
    char fname[] = "output_files\\test.bin";
    char chunk_sorted_fname[] = "output_files\\sorted_test.bin";
    char full_sorted_fname[] = "output_files\\merge_test.bin";
    char metric_file_fname[] = "output_files\\benchmarks.csv";
    DeleteFile(metric_file_fname);

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
        printf("FILE_SIZE = %llu\n", fs);
        //printf("BUFFER_SIZE = %u\n", BUFFER_SIZE);
        printf("bytes_per_sector = %lu\n", bytes_per_sector);
        printf("FILE_SIZE mod BYTES_PER_SECTOR = %llu\n", fs % (unsigned long long)bytes_per_sector);
        printf("\n");
    }
    int num_runs = 1;

    unsigned long long fs_max = (32 * (static_cast<unsigned long long>(1) << 30)) / sizeof(Itemtype);          // 8 GB

    unsigned long long ms_max = /*2 **/ (static_cast<unsigned long long>(1) << 30) / sizeof(Itemtype);          // 2 GB

    unsigned long long fs_start, ms_start;
    fs_start = 1; /* (static_cast<unsigned long long>(1) << 20) / sizeof(Itemtype);*/                                  // 1 GB
    ms_start = /*100 * (static_cast<unsigned long long>(1) << 20) / sizeof(Itemtype)*/(static_cast<unsigned long long>(1) << 30) / sizeof(Itemtype);                               // 100 MB
    fs = fs_start;
    ms = ms_start;
    unsigned num_fs_iterations = 0, num_ms_iterations = 0, number_iterations = 0;
    while (fs <= fs_max) {
        num_fs_iterations++;
        fs *= 2;
    }
    while (ms <= ms_max) {
        num_ms_iterations++;
        ms *= 2;
    }
    number_iterations = num_fs_iterations * num_ms_iterations;
    unsigned itr = 0;
    unsigned curr_itr = 0;
    for (fs = fs_start; fs <= fs_max; fs *= 2) {
        for (ms = ms_start; ms <= ms_max; ms *= 2) {
            itr++;
            printf("Iteration %u / %u: fs = %llu, ms = %llu\n", itr, number_iterations, fs, ms);
            external_sort extsrt(fs, ms, fname, chunk_sorted_fname, full_sorted_fname, metric_file_fname, num_runs, TEST_SORT, GIVE_VALS, DEBUG);
            int was_fail = extsrt.generate_averages();
            if (was_fail)
            {
                printf("Failed in generate averages");
                return 1;
            }
            else {
                //extsrt.print_metrics();
                if (ms * 2 <= ms_max && curr_itr != 0)
                {
                    was_fail = extsrt.save_metrics();
                }
                else if (ms*2 > ms_max && curr_itr != 0) {
                    was_fail = extsrt.save_metrics(false, true);
                }
                else {
                    was_fail = extsrt.save_metrics(true, false);
                }

                if (was_fail) {
                    printf("Failed in save metrics");
                    return 1;
                }
            }

            was_fail = extsrt.shallow_validate();
            if (was_fail)
            {
                printf("Failed in shallow validate");
                return 1;
            }


            if (fs <= 2621440) {
                was_fail = extsrt.deep_validate();
                if (was_fail)
                {
                    printf("Failed in deep validate");
                    return 1;
                }
            }
            DeleteFile(fname);
            DeleteFile(chunk_sorted_fname);
            DeleteFile(full_sorted_fname);
            //*/
            curr_itr++;
        }
        curr_itr = 0;
    }

    return 0;
}