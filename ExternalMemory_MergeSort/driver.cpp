/*
    512 1
    262144 1
    524288
    2883584 1
    268435456 1
    268697600 1
    1073741824 1
    1342177280 1
    2684354560 1
    5368709120 1
*/
#include <stdio.h>      
#include <windows.h>
#include "external_sort.h"


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
    unsigned long long int FILE_SIZE = strtoull(argv[1], nullptr, 10);

    //unsigned int BUFFER_SIZE = (1<<20) / sizeof(unsigned int);
    char fname[] = "output_files\\test.bin";
    char chunk_sorted_fname[] = "output_files\\sorted_test.bin";
    char full_sorted_fname[] = "output_files\\merge_test.bin";

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
        //printf("BUFFER_SIZE = %u\n", BUFFER_SIZE);
        printf("bytes_per_sector = %lu\n", bytes_per_sector);
        printf("FILE_SIZE mod BYTES_PER_SECTOR = %llu\n", FILE_SIZE % (unsigned long long)bytes_per_sector);
        printf("\n");
    }
    int num_runs = 1;
    external_sort extsrt(FILE_SIZE, fname, chunk_sorted_fname, full_sorted_fname, num_runs, TEST_SORT, GIVE_VALS, DEBUG);
    int was_fail = extsrt.generate_averages();
    if (was_fail)
    {
        printf("Failed in generate averages");
        return 1;
    }
    else {
        extsrt.print_metrics();
    }

    was_fail = extsrt.shallow_validate();
    if (was_fail)
    {
        printf("Failed in shallow validate");
        return 1;
    }


    if (FILE_SIZE <= 2621440) {
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

    return 0;
}