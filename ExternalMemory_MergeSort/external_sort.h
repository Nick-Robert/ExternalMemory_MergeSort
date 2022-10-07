#ifndef EXTERNALSORT_H
#define EXTERNALSORT_H

class external_sort
{
    // core values
    unsigned long long int FILE_SIZE;
    unsigned int BUFFER_SIZE;
    char *fname;
    char *chunk_sorted_fname;
    char *full_sorted_fname;

    bool TEST_SORT = false;
    bool GIVE_VALS = false;
    bool DEBUG = false;

    unsigned long bytes_per_sector;

    // old return_vals struct values
    unsigned long long int number_elements_touched;
    double generation_duration;
    double write_duration;
    double sort_duration;
    double read_duration;

    // validation vals
    int num_runs;
    double total_generate_time;
    double total_write_time;
    double total_sort_time;
    double total_read_time;


public:
    // constructor
    external_sort(unsigned long long int _FILE_SIZE, unsigned int _BUFFER_SIZE, char _fname[], char _chunk_sorted_fname[], char _full_sorted_fname[], unsigned long _bytes_per_sector, int _num_runs = 1, bool _TEST_SORT = false, bool _GIVE_VALS = false, bool _DEBUG = false);

    int write_file();

    int sort_file();

    int merge_sort();

    void print_metrics();

    int generate_averages();
};

#endif

