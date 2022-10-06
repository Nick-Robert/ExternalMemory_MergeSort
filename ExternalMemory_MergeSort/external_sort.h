#ifndef EXTERNALSORT_H
#define EXTERNALSORT_H

class external_sort
{
    // core values
    unsigned long long int FILE_SIZE;
    unsigned int BUFFER_SIZE;
    char *fname;
    char *sorted_fname;

    bool TEST_SORT = false;
    bool GIVE_VALS = false;
    bool DEBUG = false;

    unsigned long bytes_per_sector;
    unsigned long num_free_sectors;

    // old return_vals struct values
    unsigned long long int number_elements_touched;
    double generation_duration;
    double write_duration;
    double sort_duration;
    double read_duration;

    // validation vals
    int num_runs = 1;
    double total_generate_time = 0;
    double total_write_time = 0;
    double total_sort_time = 0;
    double total_read_time = 0;


public:
    // constructor
    external_sort();

    int write_file();

    int sort_file();

    int merge_sort();

    int print_metrics();

    int generate_averages();
};

#endif

