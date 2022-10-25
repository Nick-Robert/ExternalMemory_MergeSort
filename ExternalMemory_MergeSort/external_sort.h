#ifndef EXTERNALSORT_H
#define EXTERNALSORT_H

struct state_vars {
    // pointer to the start of this chunk
    unsigned int* start_offset;
    // pointer to the end of this chunk
    unsigned int* end_offset;
    // pointer to the current position in this chunk
    unsigned int* bufpos;
    // size of this chunk
    INT64 bufsize;
    // the start of the next seek
    INT64 seek_offset;
//    // size of this chunk
//    INT64 chunk_size;
    // pointer to the start of this chunk
    INT64 chunk_ptr;
};

class external_sort
{
    // core values
    unsigned long long int file_size;
    unsigned int buffer_size;
    char *fname;
    char *chunk_sorted_fname;
    char *full_sorted_fname;

    bool test_sort = false;
    bool give_vals = false;
    bool debug = false;

    struct state_vars* state;

    unsigned long bytes_per_sector;

    // old return_vals struct values
    unsigned long long int number_elements_touched;
    double generation_duration;
    double write_duration;
    double sort_duration;
    double read_duration;
    double merge_duration;
    double load_duration;
    double merge_read_duration;
    double heap_duration;
    double merge_write_duration;

    // validation vals
    int num_runs;
    double total_generate_time;
    double total_write_time;
    double total_sort_time;
    double total_read_time;
    double total_merge_time;
    double total_load_time;
    double total_merge_read_time;
    double total_heap_time;
    double total_merge_write_time;


public:
    // constructor
    external_sort(unsigned long long int _FILE_SIZE, char _fname[], char _chunk_sorted_fname[], char _full_sorted_fname[], int _num_runs = 1, bool _TEST_SORT = false, bool _GIVE_VALS = false, bool _DEBUG = false);

    int write_file();

    int sort_file();

    int merge_sort();

    void print_metrics();

    int generate_averages();
};

#endif

