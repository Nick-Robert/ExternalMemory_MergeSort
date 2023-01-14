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

#pragma once
#include "commons.h"
#include "merge_utils.h"
#include "merge_tree.h"

namespace origami_external_sorter {

#define _DEBUG
	template <typename Reg, typename Item>
	class ExternalMemorySort {
		char input_file[MAX_PATH_LEN];
		const char* base_path = "A:\\external\\";			// for temp files
		
		origami_utils::IOHelper IO;
		
		char* buf, * treebuf;
		ui64 sort_size_bytes;
		ui64 mem_size;
	public:
		ExternalMemorySort(char* _input_file, ui64 _mem_size, char* _output_file, ui WAY) {
			strcpy(input_file, _input_file);
			mem_size = _mem_size;
			HANDLE h_read = CreateFile(
				input_file,
				GENERIC_READ | GENERIC_WRITE,
				FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
				NULL,
				OPEN_EXISTING,
				FILE_ATTRIBUTE_NORMAL,
				NULL
			);

			LARGE_INTEGER li;
			BOOL bRet = GetFileSizeEx(h_read, &li);
			if (bRet == 0) {
				printf("GetFileSizeEx error %d\n", GetLastError());
				exit(-1);
			}
			sort_size_bytes = li.QuadPart;

			// initialize file ptrs
			init_file_ptrs(_input_file, sort_size_bytes / WAY, _output_file);
		}

		~ExternalMemorySort() {
			if (buf) VFREE(buf);
			if (treebuf) VFREE(treebuf);
		}

		void in_memory_sort() {}

		void init_file_ptrs(char* _input_file, ui64 sorted_size, char* _output_file) {
			/*char file_path[MAX_PATH_LEN];
			strcpy(file_path, base_path);
			strcat(file_path, fname);*/
			IO.init_file_ptrs(_input_file, sorted_size, _output_file);
		}

		void cleanup_fileptrs() {
			IO.cleanup_fileptrs();
		}
		
		void init_buffers(ui WAY) {
			buf = (char*)VALLOC(mem_size + MB(16));
			treebuf = (char*)VALLOC(MB(256));
			IO.init_buffers(buf, WAY, mem_size);
		}

		void cleanup_buffers() {
			VFREE(buf);
			VFREE(treebuf);
			IO.cleanup_buffers();
		}

		void sort() {
			// in-memory sort
			in_memory_sort();
		}
		
		void mtree_bench(ui WAY) {
			// merge
			init_buffers(WAY);

			ui l1_buff_n = 32;
			ui l2_buff_n = 1024;
			origami_merge_tree::MergeTree<Reg, Item, true>* kway_tree = nullptr;
			ui WAY_POW = (ui)(log2(WAY));
			if (WAY_POW & 1) kway_tree = new origami_merge_tree::MergeTreeOdd<Reg, Item, true>();
			else kway_tree = new origami_merge_tree::MergeTreeEven<Reg, Item, true>();
			kway_tree->merge_init(WAY, (Item*)treebuf, l1_buff_n, l2_buff_n, &IO);

			// bench
			printf("Merging, %llu bytes, %lu ways, mem: %llu bytes ... ", sort_size_bytes, WAY, mem_size);
			hrc::time_point s, e;
			s = hrc::now();
			FOR(i, WAY, 1)
				IO.fill_buffer(i);
			kway_tree->merge((Item**)IO.X, (Item**)IO.endX, (Item*)IO.out, (IO.out_buff_size / sizeof(Item)), l1_buff_n, l2_buff_n, (Item*)treebuf, WAY);
			e = hrc::now();
			double el = ELAPSED_MS(s, e);
			printf("done in %.2f ms @ %.2f M/s\n", el, (sort_size_bytes / sizeof(Item)) / el / 1e3);

			kway_tree->merge_cleanup();
			delete kway_tree;

			cleanup_fileptrs();
			cleanup_buffers();
		}

	};
#undef _DEBUG
}
