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
	void merge(void (*process_buffer)(int, char**, char**), ui _way) {
		Item* X[MTREE_MAX_WAY], * endX[MTREE_MAX_WAY];
		Item* output = nullptr, * outputEnd = nullptr;
		ui way_pow = (ui)(log2(_way));
		way_pow += (1 << way_pow) != _way;

		ui l1_buff_n = _MT_L1_BUFF_N;
		ui l2_buff_n = _MT_L2_BUFF_N;
		origami_merge_tree::MergeTree<Reg, Item, true>* kway_tree = nullptr;
		char* treebuf = (char*)VALLOC(MB(16));

		if (way_pow & 1) kway_tree = new origami_merge_tree::MergeTreeOdd<Reg, Item, true>(_way, (Item*)treebuf, l1_buff_n, l2_buff_n, process_buffer);
		else kway_tree = new origami_merge_tree::MergeTreeEven<Reg, Item, true>(_way, (Item*)treebuf, l1_buff_n, l2_buff_n, process_buffer);

		// bench
		FOR(i, _way, 1)
			process_buffer(i, (char**)&X[i], (char**)&endX[i]);					// get input buffers
		process_buffer(-1, (char**)&output, (char**)&outputEnd);				// get output buffer

		kway_tree->merge(X, endX, output, outputEnd);

		delete kway_tree;
		VFREE(treebuf);
	}
#undef _DEBUG
}
