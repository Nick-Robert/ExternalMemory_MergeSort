#define NOMINMAX
#include "windows.h"
#include <stdio.h>      
#include <stdlib.h>  
#include <algorithm>
#include <time.h>
#include "external_sort.h"
#include "MinHeap.h"
#include <limits>


#define CREATE_VAR1(X,Y) X##Y 
#define CREATE_VAR(X,Y) CREATE_VAR1(X,Y)

#define MAKE_KEY(k)                             \
    next##k = a * next##k + c;                  \
    buffer_aligned[i + k] = next##k;            \


external_sort::external_sort(unsigned long long int _FILE_SIZE, char _fname[], char _chunk_sorted_fname[], char _full_sorted_fname[],int _num_runs, bool _TEST_SORT, bool _GIVE_VALS, bool _DEBUG) 
    : file_size{ _FILE_SIZE }, fname{ _fname }, chunk_sorted_fname{ _chunk_sorted_fname }, full_sorted_fname{ _full_sorted_fname }, test_sort{ _TEST_SORT }, give_vals{ _GIVE_VALS }, debug{ _DEBUG }, num_runs{ _num_runs }
{
    this->buffer_size = (static_cast<unsigned long long>(1) << 20) / sizeof(unsigned int);
    this->total_generate_time = 0;
    this->total_write_time = 0;
    this->total_sort_time = 0;
    this->total_read_time = 0;
    this->total_merge_time = 0;
    this->total_load_time = 0;
    this->total_merge_read_time = 0;
    this->total_heap_time = 0;
    this->total_merge_write_time = 0;

    BOOL succeeded = GetDiskFreeSpaceA(NULL, NULL, &this->bytes_per_sector, NULL, NULL);
    if (!succeeded) {
        printf("__FUNCTION__main(): Failed getting disk information with %d\n", GetLastError());
    }

}


int external_sort::write_file()
{
    srand((unsigned int)time(0));
    LARGE_INTEGER start = { 0 }, end = { 0 }, freq = { 0 };

    QueryPerformanceFrequency(&freq);
    QueryPerformanceCounter(&start);

    unsigned long long int number_written = 0;

    unsigned int* buffer_aligned = (unsigned int*)_aligned_malloc(static_cast<size_t>(this->buffer_size) * 4, this->bytes_per_sector);
    double generation_duration = 0, write_duration = 0;

    HANDLE pfile = CreateFile(this->fname, GENERIC_WRITE, 0, 0, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL | FILE_FLAG_NO_BUFFERING, NULL);
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
        while (number_written < this->file_size) {
            // populate buffer - utilizes the Linear Congruential Generator method
            // https://en.wikipedia.org/wiki/Linear_congruential_generator
            QueryPerformanceCounter(&start);
            for (unsigned int i = 0; i < this->buffer_size; i += 8) {
                MAKE_KEY(0);
                MAKE_KEY(1);
                MAKE_KEY(2);
                MAKE_KEY(3);
                MAKE_KEY(4);
                MAKE_KEY(5);
                MAKE_KEY(6);
                MAKE_KEY(7);
            }
            QueryPerformanceCounter(&end);
            generation_duration += end.QuadPart - start.QuadPart;

            if (this->give_vals) {
                for (unsigned int i = 0; i < 2; i++) {
                    printf("buffer_aligned[%d] = %u\n", i, buffer_aligned[i]);
                }
                for (unsigned int i = this->buffer_size - 2; i < this->buffer_size - 1; i++) {
                    printf("buffer_aligned[%d] = %u\n", i, buffer_aligned[i]);
                }
            }

            DWORD num_bytes_written;
            QueryPerformanceCounter(&start);
            BOOL was_success = WriteFile(pfile, buffer_aligned, sizeof(unsigned int) * this->buffer_size, &num_bytes_written, NULL);
            QueryPerformanceCounter(&end);
            if (!(was_success)) {
                printf("%s: Failed writing to file with %d\n", __FUNCTION__, GetLastError());
                return 1;
            }
            write_duration += end.QuadPart - start.QuadPart;

            number_written += num_bytes_written / sizeof(unsigned int);
            if (this->debug) {
                printf("    number_written = %llu\n", number_written);
                printf("    bufsize = %d\n", this->buffer_size);
            }
        }
        if (this->debug)
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
    LARGE_INTEGER start = { 0 }, end = { 0 }, freq = { 0 }, num_bytes_written = { 0 };

    unsigned int* buffer = (unsigned int*)_aligned_malloc(static_cast<size_t>(this->buffer_size) * sizeof(unsigned int), this->bytes_per_sector);
    unsigned long long int written = 0, number_read = 0;
    double sort_duration = 0, read_duration = 0;

    QueryPerformanceFrequency(&freq);

    HANDLE old_file = CreateFile(this->fname, GENERIC_READ, 0, 0, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL | FILE_FLAG_NO_BUFFERING, NULL);
    HANDLE chunk_sorted_file = CreateFile(this->chunk_sorted_fname, GENERIC_WRITE, 0, 0, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL | FILE_FLAG_NO_BUFFERING, NULL);

    if (old_file == INVALID_HANDLE_VALUE) {
        printf("%s: Failed opening populated file with %d\n", __FUNCTION__, GetLastError());
        return 1;
    }
    else if (chunk_sorted_file == INVALID_HANDLE_VALUE) {
        printf("%s: Failed opening new file for sort output with %d\n", __FUNCTION__, GetLastError());
        return 1;
    }
    while (number_read < this->file_size) {
        // populate buffer
        QueryPerformanceCounter(&start);
        DWORD num_bytes_touched;
        bool was_success = ReadFile(old_file, buffer, sizeof(unsigned int) * this->buffer_size, &num_bytes_touched, NULL);
        QueryPerformanceCounter(&end);
        if (!(was_success)) {
            printf("%s: Failed reading from populated file with %d\n", __FUNCTION__, GetLastError());
            return 1;
        }
        read_duration += end.QuadPart - start.QuadPart;
        number_read = number_read + num_bytes_touched / sizeof(unsigned int);

        // sort the buffer and get time info
        QueryPerformanceCounter(&start);
        std::sort(buffer, buffer + this->buffer_size);
        QueryPerformanceCounter(&end);
        if (this->give_vals) {
            for (unsigned int i = 0; i < 2; i++) {
                printf("buffer[%d] = %u\n", i, buffer[i]);
            }
            for (unsigned int i = this->buffer_size - 2; i < this->buffer_size; i++) {
                printf("buffer[%d] = %u\n", i, buffer[i]);
            }
        }
        sort_duration += end.QuadPart - start.QuadPart;

        was_success = WriteFile(chunk_sorted_file, buffer, sizeof(unsigned int) * this->buffer_size, &num_bytes_touched, NULL);
        if (!(was_success)) {
            printf("%s: Failed writing to new file for sort output with %d\n", __FUNCTION__, GetLastError());
            return 1;
        }
        num_bytes_written.QuadPart = num_bytes_written.QuadPart + num_bytes_touched;
        written = written + num_bytes_touched / sizeof(unsigned int);

        if (this->debug) {
            printf("    file_size = %llu\n", this->file_size);
            printf("    num_bytes_touched = %d\n", num_bytes_touched);
            printf("    number_read = %llu\n", number_read);
            printf("    written = %llu\n", written);
            printf("    num_bytes_written.QuadPart = %llu\n", num_bytes_written.QuadPart);
            printf("    bufsize = %d", this->buffer_size);
        }
    }
    if (this->debug)
    {
        printf("Sort Duration: %f\n", sort_duration);
        printf("Read Duration: %f\n", read_duration);
    }
    

    _aligned_free(buffer);
    buffer = nullptr;
    CloseHandle(old_file);
    CloseHandle(chunk_sorted_file);

    this->number_elements_touched = number_read;
    this->sort_duration = sort_duration / freq.QuadPart;
    this->read_duration = read_duration / freq.QuadPart;
    return 0;
}


int external_sort::merge_sort()
{
    /*
        Merge sort Version 1 Steps (i.e., all sizes line up for an easy implementation)
        1)  Create an array of size num_chunks of MinHeapNodes and an array of size 1GB
                i)  Populate the 1GB array with the first n vals from each chunk
        2)  Create MinHeapNodes with the first val from every chunk (now taken from the 1GB array) and insert into array
        3)  Create MinHeap from the array
        4)  While the MinHeap is not empty:
                While the buffersize array is not full:
                    i)  Pop heap and take the val from that node and append it to the buffersize array
                Write buffersize array to the end of the file and "empty" it
    */

    unsigned long long num_chunks = this->file_size / this->buffer_size;
    if (file_size % buffer_size != 0)
    {
        printf("    %s: FILE_SIZE mod BUFFER_SIZE == 0 expected, FILE_SIZE = %llu, BUFFER_SIZE = %u\n", __FUNCTION__, this->file_size, this->buffer_size);
        return 1;
    }
    
    unsigned long long gb = static_cast<unsigned long long>(1) << 30;
    LARGE_INTEGER start = { 0 }, end = { 0 }, merge_start = { 0 }, merge_end = { 0 }, freq = { 0 }, num_bytes_written = { 0 };
    unsigned long long int written = 0, number_read = 0;
    double load_duration = 0, read_duration = 0, heap_duration = 0, write_duration = 0, merge_duration = 0;

    DWORD vals_per_chunk = 0;
    if (gb / sizeof(unsigned int) / num_chunks < this->buffer_size) {
        vals_per_chunk = (gb / sizeof(unsigned int)) / num_chunks;
    }
    else {
        vals_per_chunk = this->buffer_size;
    }

    if (vals_per_chunk % (512 / sizeof(unsigned int)) != 0) {
        printf("    %s: vals_per_chunk mod disk alignment (128 ints) == 0 expected, vals_per_chunk = %lu, alignment = %lu\n", __FUNCTION__, vals_per_chunk, 512 / sizeof(unsigned int));
        return 1;
    }

    if ((this->buffer_size % vals_per_chunk) % (512 / sizeof(unsigned int)) != 0) {
        printf("    %s: (this->buffer_size mod vals_per_chunk) mod (512 / sizeof(unsigned int)) == 0 expected, buffer_size = %u, vals_per_chunk = %llu, alignment = %u\n", __FUNCTION__, this->buffer_size, vals_per_chunk, 512 / sizeof(unsigned int));
        return 1;
    }

    // some kind of similar logic may be needed when dealing with values that don't exactly align to 512 bytes as expected of this first iteration
    //    the checks above ensure that everything lines up correctly so NO_BUFFERING properly works
    /*while (vals_per_chunk % (512/sizeof(unsigned int)) != 0) {
        vals_per_chunk -= 1;
    }*/

    if (this->debug) {
        printf("    FILE_SIZE = %llu\n", this->file_size);
        printf("    FILE_SIZE*sizeof(unsigned int) = %llu\n", this->file_size * sizeof(unsigned int));
        printf("    num_chunks = %llu\n", num_chunks);
        printf("    BUFFER_SIZE / num_chunks = %llu\n", this->buffer_size / num_chunks);
        printf("    vals_per_chunk = %lu\n", vals_per_chunk);
    }
    HANDLE chunk_sorted_file = CreateFile(this->chunk_sorted_fname, GENERIC_READ, 0, 0, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL | FILE_FLAG_NO_BUFFERING, NULL);
    HANDLE full_sorted_file = CreateFile(this->full_sorted_fname, GENERIC_WRITE, 0, 0, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL | FILE_FLAG_NO_BUFFERING, NULL);
    if (chunk_sorted_file == INVALID_HANDLE_VALUE) {
        printf("%s: Failed opening populated file with %d\n", __FUNCTION__, GetLastError());
        return 1;
    }
    else if (full_sorted_file == INVALID_HANDLE_VALUE) {
        printf("%s: Failed opening new file for mergesort output with %d\n", __FUNCTION__, GetLastError());
        return 1;
    }
    
    DWORD total_bytes_touched = 0;
    DWORD num_bytes_touched;
    DWORD num_bytes_to_read = 0;

    // 1)  Create an array of size num_chunks of MinHeapNodes and an array of size 1GB
    //          i)  Populate the 1GB array with the first n vals from each chunk

    unsigned int* raw_num_buffer = (unsigned int*)_aligned_malloc(gb, this->bytes_per_sector);
    MinHeapNode* heap_buffer = new MinHeapNode[num_chunks];
    unsigned int* sorted_num_buffer = (unsigned int*)_aligned_malloc(this->buffer_size * sizeof(unsigned int), this->bytes_per_sector);

    LARGE_INTEGER num_bytes_to_move = { 0 };
    QueryPerformanceFrequency(&freq);
    QueryPerformanceCounter(&start);
    for (int i = 0; i < num_chunks; i++)
    {
        num_bytes_to_move.QuadPart = static_cast<LONGLONG>(this->buffer_size) * i * sizeof(unsigned int);
        DWORD num_moved = SetFilePointer(chunk_sorted_file, num_bytes_to_move.LowPart, &num_bytes_to_move.HighPart, FILE_BEGIN);
        if (num_moved == NULL && num_bytes_to_move.QuadPart != 0) {
            printf("%s: Failed setting file pointer in populated file with %d\n", __FUNCTION__, GetLastError());
            return 1;
        }
        bool was_success = ReadFile(chunk_sorted_file, raw_num_buffer + (i * vals_per_chunk), vals_per_chunk * sizeof(unsigned int), &num_bytes_touched, NULL);
        if (!(was_success)) {
            printf("%s: Failed reading from populated file with %d\n", __FUNCTION__, GetLastError());
            return 1;
        }
    }
    QueryPerformanceCounter(&end);
    load_duration += end.QuadPart - start.QuadPart;


    if (this->give_vals) {
        printf("    raw_num_buffer = %u\n", raw_num_buffer);
        for (unsigned int i = 0; i < num_chunks; i += 1) {
            printf("    i = %u\n", i);
            printf("        raw_num_buffer[i] = %u\n", raw_num_buffer[i * vals_per_chunk]);
            printf("        raw_num_buffer[i+1] = %u\n", raw_num_buffer[i * vals_per_chunk + 1]);
            printf("        raw_num_buffer[i+this->buffer_size-2] = %u\n", raw_num_buffer[i * vals_per_chunk + vals_per_chunk - 2]);
            printf("        raw_num_buffer[i+this->buffer_size-1] = %u\n", raw_num_buffer[i * vals_per_chunk + vals_per_chunk - 1]);
        }
    }

    // 2)  Create MinHeapNodes with the first val from every chunk(now taken from the 1GB array) and insert into heap array
    QueryPerformanceCounter(&start);
    for (int i = 0; i < num_chunks; i++) {
        heap_buffer[i].val = raw_num_buffer[i * vals_per_chunk];
        heap_buffer[i].chunk_index = i;
        heap_buffer[i].val_index = 0;
        heap_buffer[i].num_times_pulled = 1;
        heap_buffer[i].last_val_index = vals_per_chunk - 1;
    }
    
    // 3)  Create MinHeap from the array
    MinHeap hp(heap_buffer, num_chunks);
    QueryPerformanceCounter(&end);

    heap_duration += end.QuadPart - start.QuadPart;

    // 4)  While the MinHeap is not empty (i.e., while count < file_size):
    //          While the sorted_num_buffer array is not full :
    //              i)  Pop heapand take the val from that nodeand append it to the buffersize array
    //          Write buffersize array to the end of the file and "empty" it
    unsigned long long count = 0;
    unsigned curr_sorted_buf_size = 0;
    while (count < this->file_size) {
        QueryPerformanceCounter(&merge_start);
        MinHeapNode root = hp.getMin();
        sorted_num_buffer[curr_sorted_buf_size] = root.val;
        root.val = raw_num_buffer[root.chunk_index * vals_per_chunk + root.val_index+1];
        root.val_index += 1;
        if (root.val_index > root.last_val_index) {
            if (root.last_val_index + 1 == vals_per_chunk && root.num_times_pulled * (root.last_val_index + 1) < this->buffer_size) {
                QueryPerformanceCounter(&merge_end);
                merge_duration += merge_end.QuadPart - merge_start.QuadPart;


                unsigned num_vals_to_get = 0;
                if (root.num_times_pulled * vals_per_chunk + vals_per_chunk <= this->buffer_size) {
                    num_vals_to_get = vals_per_chunk;
                    root.last_val_index = vals_per_chunk - 1;
                }
                else {
                    num_vals_to_get = this->buffer_size % (root.num_times_pulled * vals_per_chunk);
                    root.last_val_index = num_vals_to_get - 1;
                }

                QueryPerformanceCounter(&start);
                num_bytes_to_move.QuadPart = static_cast<LONGLONG>(this->buffer_size) * root.chunk_index * sizeof(unsigned int) + ( static_cast<unsigned long long>(root.num_times_pulled) * vals_per_chunk * sizeof(unsigned int) );
                DWORD num_moved = SetFilePointer(chunk_sorted_file, num_bytes_to_move.LowPart, &num_bytes_to_move.HighPart, FILE_BEGIN);

                bool was_success = ReadFile(chunk_sorted_file, raw_num_buffer + (root.chunk_index * vals_per_chunk), num_vals_to_get * sizeof(unsigned int), &num_bytes_touched, NULL);
                QueryPerformanceCounter(&end);
                if (!(was_success)) {
                    printf("%s: Failed reading from populated file within mergesort while loop with %d\n", __FUNCTION__, GetLastError());
                    return 1;
                }
                read_duration += end.QuadPart - start.QuadPart;

                root.val = raw_num_buffer[root.chunk_index * vals_per_chunk];
                root.num_times_pulled += 1;
                root.val_index = 0;
                QueryPerformanceCounter(&merge_start);
            }
            else {
                root.val = std::numeric_limits<unsigned int>::max();
            }
        }
        QueryPerformanceCounter(&start);
        hp.replaceMin(root);
        QueryPerformanceCounter(&end);
        heap_duration += end.QuadPart - start.QuadPart;
        curr_sorted_buf_size++;
        if (curr_sorted_buf_size == this->buffer_size) {
            QueryPerformanceCounter(&merge_end);
            merge_duration += merge_end.QuadPart - merge_start.QuadPart;

            QueryPerformanceCounter(&start);
            bool was_success = WriteFile(full_sorted_file, sorted_num_buffer, sizeof(unsigned int) * this->buffer_size, &num_bytes_touched, NULL);
            if (!(was_success)) {
                printf("%s: Failed writing to merge sorted file with %d\n", __FUNCTION__, GetLastError());
                return 1;
            }
            curr_sorted_buf_size = 0;
            QueryPerformanceCounter(&end);
            write_duration += end.QuadPart - start.QuadPart;
        }
        else {
            QueryPerformanceCounter(&merge_end);
            merge_duration += merge_end.QuadPart - merge_start.QuadPart;
        }
        count++;
    }


    if (this->debug) {
        printf("\n");
        printf("AFTER WHILE LOOP, count = %llu\n", count);
        printf("    curr_sorted_buf_size = %u\n", curr_sorted_buf_size);
    }

    _aligned_free(raw_num_buffer);
    _aligned_free(sorted_num_buffer);
    delete[] heap_buffer;
    CloseHandle(chunk_sorted_file);
    CloseHandle(full_sorted_file);

    this->merge_duration = merge_duration / freq.QuadPart;
    this->load_duration = load_duration / freq.QuadPart;
    this->merge_read_duration = read_duration / freq.QuadPart;
    this->heap_duration = heap_duration / freq.QuadPart;
    this->merge_write_duration = write_duration / freq.QuadPart;

    return 0;
}


void external_sort::print_metrics()
{
    printf("\n");
    printf("file size = %f MB\n", this->file_size * sizeof(unsigned int) / 1e6);
    printf("Averages over %d runs\n", this->num_runs);
    printf("\n    Random File Generation Statistics\n");
    printf("    Generation time: %f s\n", this->total_generate_time / this->num_runs);
    printf("    Generation rate: %f million keys/s\n", this->file_size * this->num_runs / (this->total_generate_time * 1e6));
    printf("    Write time:      %f s\n", this->total_write_time / this->num_runs);
    printf("    Write rate:      %f MB/s\n", this->file_size * this->num_runs * sizeof(unsigned int) / (this->total_write_time * 1e6));
    printf("\n    Chunk Sort Statistics\n");
    printf("    Sort time:       %f s\n", this->total_sort_time / this->num_runs);
    printf("    Sort rate:       %f MB/s\n", this->file_size * this->num_runs * sizeof(unsigned int) / (this->total_sort_time * 1e6));
    printf("    Sort rate:       %f million keys/s\n", this->file_size * this->num_runs / (this->total_sort_time * 1e6));
    printf("    Read time:       %f s\n", this->total_read_time / this->num_runs);
    printf("    Read rate:       %f MB/s\n", this->file_size * this->num_runs * sizeof(unsigned int) / (this->total_read_time * 1e6));
    printf("\n    Merge Statistics\n");
    printf("    Merge time:      %f s\n", this->total_merge_time / this->num_runs);
    printf("    Merge rate:      %f MB/s\n", this->file_size * this->num_runs * sizeof(unsigned int) / (this->total_merge_time * 1e6));
    printf("    Load time:       %f s\n", this->total_load_time / this->num_runs);
    printf("    Load rate:       %f MB/s\n", static_cast<unsigned long long>((1 << 30)) * this->num_runs * sizeof(unsigned int) / (this->total_load_time * 1e6));
    printf("    Read time:       %f s\n", this->total_merge_read_time / this->num_runs);
    printf("    Read rate:       %f MB/s\n", this->file_size * this->num_runs * sizeof(unsigned int) / (this->total_merge_read_time * 1e6));
    printf("    Heap time:       %f s\n", this->total_heap_time / this->num_runs);
    printf("    Heap rate:       %f MB/s\n", this->file_size * this->num_runs * sizeof(unsigned int) / (this->total_heap_time * 1e6));
    printf("    Write time:      %f s\n", this->total_merge_write_time / this->num_runs);
    printf("    Write rate:      %f MB/s\n", this->file_size * this->num_runs * sizeof(unsigned int) / (this->total_merge_write_time * 1e6));
}

int external_sort::generate_averages()
{
    for (int i = 0; i < this->num_runs; i++)
    {
        int was_fail = write_file();
        if (was_fail)
        {
            return 1;
        }
        if (this->debug)
        {
            printf("Number of bytes written = %llu\n", this->number_elements_touched);
        }
        this->total_generate_time += this->generation_duration;
        this->total_write_time += this->write_duration;
        if (this->test_sort)
        {
            was_fail = sort_file();
            if (was_fail)
            {
                return 1;
            }
            this->total_sort_time += this->sort_duration;
            this->total_read_time += this->read_duration;
        }
        merge_sort();
        this->total_merge_time += this->merge_duration;
        this->total_load_time += this->load_duration;
        this->total_merge_read_time += this->merge_read_duration;
        this->total_heap_time += this->heap_duration;
        this->total_merge_write_time += this->merge_write_duration;
    }
    return 0;
}
