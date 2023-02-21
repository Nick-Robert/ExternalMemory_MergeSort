/*--------------------------------------------------------------------------------------------
 - Origami: A High-Performance Mergesort Framework											 -
 - Copyright(C) 2021 Arif Arman, Dmitri Loguinov											 -
 - Produced via research carried out by the Texas A&M Internet Research Lab                  -
 -                                                                                           -
 - This program is free software : you can redistribute it and/or modify                     -
 - it under the terms of the GNU General Public License as published by                      -
 - the Free Software Foundation, either version 3 of the License, or                         -
 - (at your option) any later version.                                                       -
 -                                                                                           -
 - This program is distributed in the hope that it will be useful,                           -
 - but WITHOUT ANY WARRANTY; without even the implied warranty of                            -
 - MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.See the                               -
 - GNU General Public License for more details.                                              -
 -                                                                                           -
 - You should have received a copy of the GNU General Public License                         -
 - along with this program. If not, see < http://www.gnu.org/licenses/>.                     -
 --------------------------------------------------------------------------------------------*/

#include "commons.h"
#include "utils.h"
#include "Writer.h"
#include "external_sorter.h"

 // ------------------
 // ------------------
 // user specifiable IO stuff
int way;
HANDLE fp[MTREE_MAX_WAY + 1];
ui64 bytes_left[MTREE_MAX_WAY + 1];
char* X[MTREE_MAX_WAY + 1], * endX[MTREE_MAX_WAY + 1];
ui64 in_buf_size, out_buf_size;
ui64 tot_bytes_written;

// callback function 
void process_buffer(int stream_idx, char** _p, char** _endp) {
	//#define _DEBUG_PRINT
	// define X and endX to be pointers on the current block for each stream
	// experiment with size of memory blocks to help minimize process buffer calls (1MB -> 16 MB -> 32 -> 64)
#ifdef _DEBUG_PRINT
	printf("Processing buffer for node: %d ...\n", stream_idx);
#endif 
	if (stream_idx == -1) {		// flush output buffer
		char* output = X[way];
		char* endpos = *_p;
		if (endpos != nullptr) {
			ui64 bytes = endpos - output;
			HANDLE h_write = fp[way];
			DWORD bytesWritten;
			int bWrt = WriteFile(h_write, output, bytes, &bytesWritten, NULL);
			if (bWrt == 0) {
				printf("WriteFile failed with %d\n", GetLastError());
				getchar();
				exit(-1);
			}
			tot_bytes_written += bytesWritten;
			if (tot_bytes_written % GB(1LLU) == 0) {
				printf("                                                                          \r");
				printf("Written: %llu", tot_bytes_written);

				//printf("Written: %llu B\n", tot_bytes_written);
			}
		}
		*_p = X[way];
		*_endp = endX[way];
	}
	else {						// fill input buffer
		HANDLE f = fp[stream_idx];
		ui64 bytes = min(in_buf_size, bytes_left[stream_idx]);
		DWORD bytes_read;
		char* p = X[stream_idx];
		ui64 tot_bytes_read = 0;
		DWORD max_read = 4294967295;
		while (bytes > 0) {
			DWORD bytes_to_read = min(max_read, bytes);
			BOOL bRet = ReadFile(
				f,
				p,
				bytes_to_read,
				&bytes_read,
				NULL
			);
			if (bRet == false) {
				printf("ReadFile failed with %d\n", GetLastError());
				getchar();
				exit(-1);
			}
			tot_bytes_read += bytes_read;
			bytes -= bytes_read;
			p += bytes_read;
		}
		*_p = X[stream_idx];
		*_endp = X[stream_idx] + tot_bytes_read;
		bytes_left[stream_idx] -= tot_bytes_read;
#ifdef _DEBUG_PRINT
		printf("Loaded: %llu, Left: %llu bytes\n", tot_bytes_read, bytes_left[stream_idx]);
#endif 
	}
#ifdef _DEBUG_PRINT
	printf("Returned: [%llX %llX]\n", *_p, *_endp);
#endif 
#undef _DEBUG_PRINT
}

void init_buffers(char* buf, ui64 mem_size) {
	// mem_size = RAM, buf = char array 
	out_buf_size = MB(1);
	ui64 delta = 0;
	ui64 largest_chunk = 0;
	// delta now in bytes
	delta = 2 * mem_size / (way * (static_cast<unsigned long long>(way) + 1));
	largest_chunk = way * delta;
	//in_buf_size = mem_size / way;
	in_buf_size = delta;

	char* p = buf;
	FOR(i, way, 1) {
		X[i] = p;
		endX[i] = p + in_buf_size;
		p += in_buf_size + 64;
		in_buf_size += delta;
	}
	// output buffer
	X[way] = p;
	endX[way] = p + out_buf_size;
}

// ------------------
// ------------------

// generate data for merge test
template <typename Item>
void generate_merge_data(ui64 total_size, ui WAY) {
	ui64 n = total_size / sizeof(Item);
	ui64 n_per_stream = n / WAY;
	Item* data = (Item*)VALLOC(total_size);
	hrc::time_point s, e;

	datagen::Writer<Item> writer;
	printf("Generating data (%llu bytes) ... \n", total_size);
	s = hrc::now();
	writer.generate(data, n, MT);
	e = hrc::now();
	printf("done in %.2f ms\n", ELAPSED_MS(s, e));


	printf("Sorting chunks (%llu bytes) ... ", n_per_stream * sizeof(Item));
	s = hrc::now();
	SortEvery<Item>(data, n, n_per_stream);
	e = hrc::now();
	double el = ELAPSED_MS(s, e);
	printf("done in %.2f ms; %llu bytes @ %.2f MB/s\n", el, total_size, total_size * 1.0 / el / 1e3);


	char fname[MAX_PATH_LEN];
	char _tmp[50];
	strcpy(fname, "D:\\people\\nicholas\\external-merge-data\\");
	strcat(fname, "merge_data_32bit_");
	strcat(fname, _itoa(WAY, _tmp, 10));
	strcat(fname, "way");
	//strcat(fname, _ltoa(total_size, _tmp, 10));
	strcat(fname, ".dat");
	printf("Writing to file %s ...", fname);
	HANDLE hWrite = CreateFile(
		fname,
		GENERIC_WRITE | GENERIC_WRITE,
		FILE_SHARE_WRITE | FILE_SHARE_DELETE | FILE_SHARE_WRITE,
		NULL,
		CREATE_ALWAYS,
		FILE_ATTRIBUTE_NORMAL,
		NULL
	);
	DWORD bytesWritten;
	DWORD max_write = 4294967295;
	ui64 bytes_left = total_size;
	char* p = (char*)data;
	s = hrc::now();
	while (bytes_left > 0) {
		DWORD bytes_to_write = min(max_write, bytes_left);
		int bWrt = WriteFile(hWrite, p, bytes_to_write, &bytesWritten, NULL);
		if (bWrt == 0) {
			printf("WriteFile failed with %d\n", GetLastError());
			exit(-1);
		}
		bytes_left -= bytesWritten;
		p += bytesWritten;
	}

	e = hrc::now();
	el = ELAPSED_MS(s, e);
	printf("done in %.2f ms; %llu bytes @ %.2f MB/s\n", el, total_size, total_size * 1.0 / el / 1e3);

	CloseHandle(hWrite);
	VFREE(data);
}


const char* sorted_strs[] = { "NOT SORTED", "SORTED" };
template <typename Item>
bool sorted(char* f) {
	HANDLE h_read = CreateFile(
		f,
		GENERIC_READ | GENERIC_WRITE,
		FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
		NULL,
		OPEN_EXISTING,
		FILE_ATTRIBUTE_NORMAL,
		NULL

	);

	if (h_read == INVALID_HANDLE_VALUE) {
		printf("file open error %d\n", GetLastError());
		exit(-1);
	}

	LARGE_INTEGER li;
	BOOL bRet = GetFileSizeEx(h_read, &li);
	if (bRet == 0) {
		printf("GetFileSizeEx error %d\n", GetLastError());
		exit(-1);
	}
	ui64 fsize_bytes = li.QuadPart;
	ui64 n = fsize_bytes / sizeof(Item);
	printf("Opened file %s: %llu bytes\n", f, fsize_bytes);

	// check correctness
	// prototype assumption file < actual ram size
	Item* dat = (Item*)VALLOC(fsize_bytes);
	if (ReadFile(h_read, dat, fsize_bytes, NULL, NULL) == false)
		ReportError("failed to load file");
	printf("Checking sortedness ... ");
	FOR(i, n - 1, 1) {
		if (dat[i] > dat[i + 1]) {
			printf("Correctness error @llu: %ld, %ld\n", i, dat[i], dat[i + 1]);
			return false;
		}
	}
	printf("done\n");

	VFREE(dat);
	return true;
}

template <typename Reg, typename Item>
void mtree_single_thread(int argc, char** argv) {
	SetThreadAffinityMask(GetCurrentThread(), 1 << 4);
	/**
	* Expected params:
	* input file
	* <GB>
	* way
	* output file
	*/


	if (argc < 5) {
		printf("Insufficient args. * Expected params:\n"
			"* input file\n"
			"* <GB>\n"
			"* way\n"
			"* output file");
		exit(-1);
	}

	char* _input_file = argv[1];
	ui64 total_size = GB(_atoi64(argv[2]));
	way = atoi(argv[3]);
	char* _output_file = argv[4];
	const ui64 mem_size = RAM;

	/*const char* _input_file = "D:\\people\\arif\\external-merge-data\\merge_data_32bit_16way_8589934592.dat";
	const char* _output_file = "D:\\people\\arif\\external-merge-data\\merged.dat";
	way = 16;
	ui64 total_size = GB(8LLU);*/

	ui64 delta = 0;
	ui64 largest_chunk = 0;
	delta = 2 * mem_size / (way * (static_cast<unsigned long long>(way) + 1));
	largest_chunk = way * delta;

	unsigned long long tot_bytes_from_delta = 0;
	for (unsigned int i = 0; i < num_chunks; i++)
	{
		tot_bytes_from_delta += (1LLU + i) * delta;
	}

	printf("tot_bytes_from_delta = %llu\n", tot_bytes_from_delta);

	FOR(i, way, 1) bytes_left[i] = delta * (i + 1);

	LARGE_INTEGER li_dist;
	ui64 cur_byte = 0;
	FOR(i, way, 1) {
		HANDLE h_tmp = CreateFile(
			_input_file,
			GENERIC_READ | GENERIC_WRITE,
			FILE_SHARE_READ | FILE_SHARE_DELETE | FILE_SHARE_WRITE,
			NULL,
			OPEN_EXISTING,
			FILE_ATTRIBUTE_NORMAL,
			NULL
		);
		li_dist.QuadPart = cur_byte;
		BOOL bRet = SetFilePointerEx(
			h_tmp,
			li_dist,
			NULL,
			FILE_BEGIN
		);
		if (bRet < 0) {
			printf("SetFilePointerEx error %d\n", GetLastError());
			exit(-1);
		}

		fp[i] = h_tmp;
		cur_byte += bytes_left[i];
	}

	fp[way] = CreateFile(
		_output_file,
		GENERIC_WRITE | GENERIC_WRITE,
		FILE_SHARE_WRITE | FILE_SHARE_DELETE | FILE_SHARE_WRITE,
		NULL,
		CREATE_ALWAYS,
		FILE_ATTRIBUTE_NORMAL,
		NULL
	);

	// setup in-memory buffers
	char* buf = (char*)VALLOC(mem_size + MB(16));
	init_buffers(buf, mem_size);


	void (*f)(int, char**, char**);
	f = &process_buffer;

	printf("Merging %s ... \n", _input_file);
	hrc::time_point s, e;
	s = hrc::now();
	origami_external_sorter::merge<Reg, Item>(f, way);
	e = hrc::now();
	double el = ELAPSED_MS(s, e);
	printf("\nDone in %.2f ms, Speed: %.2f M/s\n", el, total_size * 1.0 / el / 1e3);

	// cleanup
	FOR(i, way + 1, 1) CloseHandle(fp[i]);
	VFREE(buf);
}

int main(int argc, char** argv) {
	using Reg = sse;
	using Item = ui;

	/**
	* Instructions:
	*
	* Step 1: Generate merge data -- generates a group of sorted segments and writes to a file. This prepares the data for running the merge tree.
	* Step 2: Run the merge tree -- reads from the generated data in Step 1 and writes the merged output to a file
	* Step 3: Check correctness -- read the output file and check if merge resulted in a single sorted sequence
	*
	*/

	// 1. generate 16 sorted sequences in an 8 GB file
	generate_merge_data<Item>(GB(2LLU), 4);
	//return 0;

	// 2. run merge tree; need to specify the input file, the merge WAY, and output file in cmd args
	if (argc < 5) {
		printf("Origami.exe <input file path> <GB> <WAY> <output file path>");
		getchar();
		exit(-1);
	}
	mtree_single_thread<Reg, Item>(argc, argv);

	// 3. check correctness; need to specify the output file in cmd args
	printf("File %s: %s\n", argv[4], sorted_strs[sorted<Item>(argv[4])]);

	//char outf[] = "D:\\people\\arif\\merged.dat";
	//printf("File %s: %s\n", outf, sorted_strs[sorted<Item>(outf)]);

	system("pause");

	return 0;
}