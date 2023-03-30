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
    bool seq_run = true;
    //unsigned int BUFFER_SIZE = (1<<20) / sizeof(unsigned int);
    char seq_fname[] = "D:\\large_file.dat";
    char fname[] = "D:\\output_files\\test.bin";
    char chunk_sorted_fname[] = "D:\\output_files\\sorted_test.bin";
    char full_sorted_fname[] = "D:\\output_files\\merge_test.bin";
    char metric_file_fname[] = "D:\\output_files\\BENCH_origami_external_6TB_multifill_2limited_bigdata-compare_2GB-mem-1TB-file.csv";
    //char metric_file_fname[] = "D:\\output_files\\BENCH_origami_internal_sort_4_way.csv";
    //char metric_file_fname[] = "D:\\output_files\\BENCH_minheap_external_6TB.csv";
    /*char fname[] = "output_files\\test.bin";
    char chunk_sorted_fname[] = "output_files\\sorted_test.bin";
    char full_sorted_fname[] = "output_files\\merge_test.bin";
    char metric_file_fname[] = "output_files\\benchmarks.csv";*/
    DeleteFile(metric_file_fname);

    bool TEST_SORT = true;
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
    //BOOL succeeded = GetDiskFreeSpaceA(NULL, NULL, &bytes_per_sector, &num_free_sectors, NULL);
    BOOL succeeded = GetDiskFreeSpaceA("D:\\", NULL, &bytes_per_sector, &num_free_sectors, NULL);
    if (!succeeded) {
        printf("__FUNCTION__main(): Failed getting disk information with %d\n", GetLastError());
        return 1;
    }
    if (DEBUG)
    {
        printf("FILE_SIZE = %llu\n", fs);
        printf("bytes_per_sector = %lu\n", bytes_per_sector);
        printf("FILE_SIZE mod BYTES_PER_SECTOR = %llu\n", fs % (unsigned long long)bytes_per_sector);
        printf("\n");
    }
    int num_runs = 1;

    ULARGE_INTEGER free_ds = { 0 };

    succeeded = GetDiskFreeSpaceEx("D:\\output_files", NULL, NULL, &free_ds);
    if (!succeeded) {
        printf("driver: Failed getting disk information with %d\n", GetLastError());
    }

    //unsigned long long fs_max =  16 * ((static_cast<unsigned long long>(1) << 30)) / sizeof(Itemtype);          // 8 GB
    // mem_avail = static_cast<unsigned long long>(1) << (unsigned)log2(mem_avail);
    //unsigned long long seq_fs_size = 0;

    unsigned long long fs_max = 0;
    /*LARGE_INTEGER seq_fs_size = {0};
    if (seq_run)
    {
        HANDLE temp = CreateFile(seq_fname, GENERIC_READ, 0, 0, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);
        if (temp == INVALID_HANDLE_VALUE) {
            printf("%s: Failed opening large file with %d\n", __FUNCTION__, GetLastError());
            exit(1);
        }
        int retval = GetFileSizeEx(temp, &seq_fs_size);
        printf("seq_fs_size.QuadPart = %llu\n", fs_max);
        if (retval == 0)
        {
            printf("%s: Failed getting file size for large file with %d\n", __FUNCTION__, GetLastError());
            exit(1);
        }
        CloseHandle(temp);
        fs_max = 1LLU << (unsigned)log2(seq_fs_size.QuadPart / (3 * sizeof(Itemtype)));
    }
    else {
        fs_max = 1LLU << (unsigned)log2(free_ds.QuadPart / (3 * sizeof(Itemtype)));
    }
    printf("seq_fs_size.QuadPart = %llu\n", seq_fs_size.QuadPart);*/

    //fs_max = 1LLU << (unsigned)log2(free_ds.QuadPart / (3 * sizeof(Itemtype)));
    fs_max = 1LLU << (unsigned)log2(7340204097536LLU / (3 * sizeof(Itemtype)));
    unsigned long long fs_start;// , ms_start;

    //fs_start = 268435456 * 2LLU;
    //fs_start = 268435456 * 2LLU * (1LLU << 7);
    //fs_start = 268435456 * 2LLU * (1LLU << 6);
    //fs_start = (1LLU << 38) / sizeof(Itemtype);
    fs_start = fs_max / 2;

    fs_max = fs_start;


    fs = fs_start;

    unsigned num_fs_iterations = 0, num_ms_iterations = 0, number_iterations = 0;
    while (fs <= fs_max) {
        num_fs_iterations++;
        fs *= 2;
    }

    number_iterations = num_fs_iterations;// *num_ms_iterations;
    printf("fs_start = %llu\n", fs_start);
    printf("fs_max = %llu\n", fs_max);

    unsigned itr = 0;
    unsigned curr_itr = 0;
    // lowest fs can be is 512 bytes (may be lower bounded by something else too)
    for (fs = fs_start; fs <= fs_max; fs *= 2) {
        itr++;
        //printf("Iteration %u / %u: fs = %llu, ms = %llu\n", itr, number_iterations, fs, ms);
        printf("\n\nIteration %u / %u: fs = %llu B (%llu MB) (%llu vals)\n", itr, number_iterations, fs * sizeof(Itemtype), fs * sizeof(Itemtype) / (1LLU << 20), fs);
        if (seq_run) {
            external_sort extsrt(fs, ms, seq_fname, seq_fname, seq_fname, metric_file_fname, num_runs, TEST_SORT, GIVE_VALS, DEBUG);
            extsrt.generate_averages();
            extsrt.save_metrics(true, false);
            //extsrt.shallow_validate();
            if (fs <= 2621440) {
                extsrt.deep_validate();
            }
            printf("\n");
        }
        else
        {
            external_sort extsrt(fs, ms, fname, chunk_sorted_fname, full_sorted_fname, metric_file_fname, num_runs, TEST_SORT, GIVE_VALS, DEBUG);
            extsrt.generate_averages();
            extsrt.save_metrics(true, false);
            extsrt.shallow_validate();
            if (fs <= 2621440) {
                extsrt.deep_validate();
            }
            printf("\n");
            DeleteFile(fname);
            DeleteFile(chunk_sorted_fname);
            DeleteFile(full_sorted_fname);
        }
        //extsrt.print_metrics();
        /*if (ms * 2 <= ms_max && curr_itr != 0)
        {
            extsrt.save_metrics();
        }
        else if (ms*2 > ms_max && curr_itr != 0) {
            extsrt.save_metrics(false, true);
        }
        else {*/
        //}
        curr_itr = 0;
    }

    return 0;
}