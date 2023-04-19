#ifndef EXTERNALSORT_H
#define EXTERNALSORT_H
//#include <vector>
//#include "commons.h"
#include "MinHeap.h"

enum distribution { LCG, FIB, REVERSE, ZIPF, dPARETO_B2B, dPARETO_SHUFF, dPARETO_NONUNIFORM, dMT };
enum fullsorted_location { FORMER, LATTER };


struct state_vars {
    /*  deals with file  */
    
    // in bytes
    unsigned long long start_offset;
    // in bytes
    unsigned long long end_offset;
    // in bytes
    uint64_t seek_offset;
    // in bytes
    unsigned long long chunk_ptr;

    // in number of vals
    uint64_t chunk_size;
    // number of vals in the current queue that have been touched
    uint64_t tot_bq_vals;
    // in number of blocks (1 start)
    uint64_t curr_block;
    // how many vals are in the last block
    std::vector<std::pair<uint64_t, uint64_t>> num_vals_last_block;
     //uint64_t num_vals_last_block;

    /*  deals with buffer  */

    // a queue of 1MB blocks of Itemtype vals
    std::queue<Itemtype*> bq;
    // number of blocks
    uint64_t num_blocks;

    // pointer 
    Itemtype* bufpos;
    // how many vals are currently in the buffer
    uint64_t curr_buflen;
    // in number of vals
    uint64_t bufsize;
    // in number of vals
    uint64_t nobuff_bufsize;

    unsigned int total_num_to_refill;

    void print() const {
        printf("File vals\n");
        printf("    start_offset = %llu\n", start_offset);
        printf("    end_offset = %llu\n", end_offset);
        printf("    seek_offset = %llu\n", seek_offset);
        printf("    chunk_ptr = %llu\n", chunk_ptr);
        printf("    chunk_size = %llu\n", chunk_size);
        printf("Buffer vals\n");
        printf("    bufpos = %llu\n", bufpos);
        printf("    bufsize = %llu\n", bufsize);
        printf("    nobuff_bufsize = %llu\n", nobuff_bufsize);
    }
};


class external_sort
{
    // core values
    unsigned long long int file_size;
    LARGE_INTEGER windows_fs;
    unsigned int write_buffer_size;
    unsigned long long int chunk_size;
    unsigned long long int block_size;
    char* fname;
    char* chunk_sorted_fname;
    char* full_sorted_fname;
    char* metrics_fname;

    std::queue<Itemtype*> free_blocks;
    

    bool test_sort = false;
    bool give_vals = false;
    bool debug = false;
    bool seq_run = false;

    // mergesort values
    //struct state_vars *state;
    //std::vector<state_vars> state;
    unsigned long long int mergesort_buffer_size;
    unsigned long long int mem_avail;
    unsigned long long int merge_mem_avail;
    //unsigned long long int num_chunks;
    //unsigned long long int tot_bytes_written;
    //unsigned long long int out_buf_size;
    //unsigned long long int in_buf_size;
    //char* X[MTREE_MAX_WAY + 1], * endX[MTREE_MAX_WAY + 1];
    //HANDLE fp[MTREE_MAX_WAY + 1];

    unsigned long bytes_per_sector;

    // old return_vals struct values
    unsigned long long int number_elements_touched;
    double generation_duration;
    double write_duration;
    double sort_duration;
    double sort_write_duration;
    double read_duration;
    double merge_duration;
    double load_duration;
    double merge_read_duration;
    double heap_duration;
    double merge_write_duration;
    double merge_populate_duration;

    // validation vals
    int num_runs;
    double total_time;
    double total_generate_time;
    double total_write_time;
    double total_sort_time;
    double total_read_time;
    double total_merge_time;
    double total_load_time;
    double total_merge_read_time;
    double total_heap_time;
    double total_merge_write_time;
    unsigned int num_seeks;


public:
    // constructor
    external_sort(unsigned long long int _FILE_SIZE, unsigned long long int _MEM_SIZE, char _fname[], char _chunk_sorted_fname[], char _full_sorted_fname[], char _metrics_fname[], int _num_runs = 1, bool _TEST_SORT = false, bool _GIVE_VALS = false, bool _DEBUG = false);

    int write_file();

    int sort_file();

    void init_buffers(char* buf);

    //uint64_t populate_blocks(unsigned idx, unsigned long long* remaining_vals, Itemtype* rbuff);

    int merge_sort(enum fullsorted_location loc);

    void print_metrics();

    int save_metrics(bool header=false, bool extra_space=false);

    int generate_averages();

    int shallow_validate();

    int deep_validate();
};

void process_buffer(int stream_idx, char** _p, char** _endp);

uint64_t populate_blocks(unsigned idx, unsigned long long remaining_vals, unsigned long long total_vals_to_read, HANDLE* f, unsigned num_blocks);
//uint64_t populate_blocks(unsigned idx, unsigned long long remaining_vals, unsigned long long total_vals_to_read, HANDLE* f);


//uint64_t normal_populate_blocks(unsigned idx, unsigned long long remaining_vals, unsigned long long total_vals_to_read, HANDLE* f, unsigned num_blocks);


#endif
