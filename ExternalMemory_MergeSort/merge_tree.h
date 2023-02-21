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

namespace origami_merge_tree {
	//#define MTREE_DBG_PRINT

		// IO callbacks
	void (*process_buffer)(int node_idx, char** _p, char** _endp);

	ui64 tot_sorted = 0;
	ui64 refill = 0;

	template <typename Reg, typename Item, bool external>
	FORCEINLINE void merge_leaf_to_internal(Item** _loadFrom, Item** _opposite, Reg& a0, Reg& a1, Item* outbuf, Item** _endA, Item** _endB, Item** _endoutput, ui& exhaust, ui base_idx = 0) {
#ifdef MTREE_DBG_PRINT
		printf("Merge leaf to internal ... \n");
		origami_utils::print<Reg, Item>(a0);
		origami_utils::print<Reg, Item>(a1);
		Item* p = (*_loadFrom < *_opposite ? *_loadFrom : *_opposite), * endp = endA;
		printf("[L]: ");
		while (p < endp) printf("%lX ", *p++); printf("\n");

		p = (*_loadFrom < *_opposite ? *_opposite : *_loadFrom); endp = endB;
		printf("[R]: ");
		while (p < endp) printf("%lX ", *p++); printf("\n");

		p = outbuf;
#endif 
		// exhaust 0 -> both children have Items; 1 -> loadFrom empty; 2 -> opposite empty i.e. all empty
		constexpr ui ITEMS_PER_REG = sizeof(Reg) / sizeof(Item);
		Item* endoutput = *_endoutput;
		if (outbuf != endoutput && exhaust < 2) {
			Item* loadFrom = *_loadFrom;
			Item* opposite = *_opposite;
			Item* endA = *_endA;
			Item* endB = *_endB;
			// run if both children have Items
			if (exhaust == 0) {
				while (true) {
					while (outbuf != endoutput && loadFrom != endA && loadFrom != endB) {
						//printf("%llX, %llX\n", outbuf, endoutput);
						bool first = *loadFrom < *opposite;
						Item* tmp0 = first ? loadFrom : opposite;
						opposite = first ? opposite : loadFrom;
						loadFrom = tmp0;

						origami_utils::rswap<Reg, Item>(a0, a1);
						origami_utils::store<Reg, false>(a0, (Reg*)outbuf);
						outbuf += ITEMS_PER_REG;
						origami_utils::load<Reg>(a0, (Reg*)loadFrom);
						_mm_prefetch((char*)(loadFrom + 64), _MM_HINT_T2);
						loadFrom += ITEMS_PER_REG;
					}
					if constexpr (external) {
						// buffer full?
						if (outbuf == endoutput) break;
						// else: some leaf node buffer ran out
						ui empty_idx = (loadFrom == endA) ? base_idx : (base_idx + 1);
						Item** end_ptr = (loadFrom == endA) ? _endA : _endB;
						Item* p, * endp;
						process_buffer(empty_idx, (char**)&p, (char**)&endp);
						if (p < endp) {
							loadFrom = p;
							*end_ptr = endp;
						}
						else {		// node exhausted
							//printf("** Node %lu exhausted\n", empty_idx);
							origami_utils::rswap<Reg, Item>(a0, a1);
							origami_utils::store<Reg, false>(a0, (Reg*)outbuf); outbuf += ITEMS_PER_REG;
							exhaust = 1;
							break;
						}
					}
					else break;
				}
			}
			// if one of the streams run out
			if (outbuf != endoutput) {
				if constexpr (external) {
					Item* endOpposite = (loadFrom == endA) ? endB : endA;
					ui opposite_idx = (loadFrom == endA) ? (base_idx + 1) : base_idx;
					Item** opposite_end_ptr = (loadFrom == endA) ? _endB : _endA;
					while (true) {
						while (outbuf != endoutput && opposite != endOpposite) {
							origami_utils::load<Reg>(a0, (Reg*)opposite); opposite += ITEMS_PER_REG;
							origami_utils::rswap<Reg, Item>(a0, a1);
							origami_utils::store<Reg, false>(a0, (Reg*)outbuf); outbuf += ITEMS_PER_REG;
						}
						if (outbuf == endoutput) break;
						// refill opposite buffer
						//printf("Buffer %lu empty\n", opposite_idx);
						Item* p, * endp;
						process_buffer(opposite_idx, (char**)&p, (char**)&endp);
						if (p < endp) {
							opposite = p;
							*opposite_end_ptr = endp;
						}
						else {
							//printf("** Node %lu exhausted\n", opposite_idx);
							origami_utils::store<Reg, false>(a1, (Reg*)outbuf); outbuf += ITEMS_PER_REG;
							exhaust = 2;
							endoutput = outbuf;
							break;
						}
					}
				}
				else {
					if (!exhaust) {
						origami_utils::rswap<Reg, Item>(a0, a1);
						origami_utils::store<Reg, false>(a0, (Reg*)outbuf); outbuf += ITEMS_PER_REG;
						exhaust = 1;
						//printf("Leaf exhausted\n");
					}
					Item* endOpposite = (loadFrom == endA) ? endB : endA;
					while (outbuf != endoutput && opposite != endOpposite) {
						origami_utils::load<Reg>(a0, (Reg*)opposite); opposite += ITEMS_PER_REG;
						origami_utils::rswap<Reg, Item>(a0, a1);
						origami_utils::store<Reg, false>(a0, (Reg*)outbuf); outbuf += ITEMS_PER_REG;
					}
					if (outbuf != endoutput && exhaust != 2) {
						origami_utils::store<Reg, false>(a1, (Reg*)outbuf); outbuf += ITEMS_PER_REG;
						exhaust = 2;
						endoutput = outbuf;		// we ran out of items so update end boundary
						//printf("Both leaf exhausted\n");
					}
				}

			}
			*_loadFrom = loadFrom;
			*_opposite = opposite;
			*_endoutput = endoutput;
		}
#ifdef MTREE_DBG_PRINT
		printf("[O]: ");
		while (p < endoutput) printf("%lX ", *p++); printf("\n");
		printf("Exiting merge leaf to internal ... \n");
#endif 
	}

	template <typename Reg, typename Item>
	FORCEINLINE Item* merge_internal_to_internal(Item** _loadFrom, Item** _opposite, Reg& a0, Reg& a1, Item* outbuf, Item* endA, Item* endB, Item** _endoutput, ui exhaust0, ui exhaust1, ui& exhaust) {
		// exhaust 0 -> both children have keys; 1 -> loadFrom empty; 2 -> opposite empty i.e. all empty
#ifdef MTREE_DBG_PRINT
		printf("Merging internal to internal ... \n");
		origami_utils::print<Reg, Item>(a0);
		origami_utils::print<Reg, Item>(a1);
		Item* p = (*_loadFrom < *_opposite ? *_loadFrom : *_opposite), * endp = endA;
		printf("[L]: ");
		while (p < endp) printf("%lX ", *p++); printf("\n");

		p = (*_loadFrom < *_opposite ? *_opposite : *_loadFrom); endp = endB;
		printf("[R]: ");
		while (p < endp) printf("%lX ", *p++); printf("\n");

		p = outbuf;
#endif 
		constexpr ui ITEMS_PER_REG = sizeof(Reg) / sizeof(Item);
		if (exhaust < 2) {
			Item* endoutput = *_endoutput;
			Item* loadFrom = *_loadFrom;
			Item* opposite = *_opposite;

			// run if both children have keys
			while (outbuf != endoutput && loadFrom != endA && loadFrom != endB) {
				bool first = *loadFrom < *opposite;
				Item* tmp0 = first ? loadFrom : opposite;
				opposite = first ? opposite : loadFrom;
				loadFrom = tmp0;

				//		PAVX(a0); PAVX(a1);
				origami_utils::rswap<Reg, Item>(a0, a1);				//origami_utils::print<Reg, Item>(a0);
				origami_utils::store<Reg, false>(a0, (Reg*)outbuf);
				outbuf += ITEMS_PER_REG;
				//PAVX(a0);// PAVX(a1);
				origami_utils::load<Reg>(a0, (Reg*)loadFrom);
				_mm_prefetch((char*)(loadFrom + 64), _MM_HINT_T2);
				loadFrom += ITEMS_PER_REG;
			}

			// loadFrom exhausted
			if (outbuf != endoutput) {
				// this was the last batch from A
				if (exhaust0 == 2 && loadFrom == endA) {
					if (!exhaust) {
						origami_utils::rswap<Reg, Item>(a0, a1);
						origami_utils::store<Reg, false>(a0, (Reg*)outbuf); outbuf += ITEMS_PER_REG;
						exhaust = 1;
					}
					Item* endOpposite = endB;
					while (outbuf != endoutput && opposite != endOpposite) {
						origami_utils::load<Reg>(a0, (Reg*)opposite); opposite += ITEMS_PER_REG;
						origami_utils::rswap<Reg, Item>(a0, a1);
						origami_utils::store<Reg, false>(a0, (Reg*)outbuf); outbuf += ITEMS_PER_REG;
					}
					if (outbuf != endoutput) {
						if (exhaust1 == 2) {
							origami_utils::store<Reg, false>(a1, (Reg*)outbuf); outbuf += ITEMS_PER_REG;
							exhaust = 2;
							endoutput = outbuf;
						}
						Item* tmp = loadFrom; loadFrom = opposite; opposite = tmp;
					}
				}
				// or last batch from B
				else if (exhaust1 == 2 && loadFrom == endB) {
					if (!exhaust) {
						origami_utils::rswap<Reg, Item>(a0, a1);
						origami_utils::store<Reg, false>(a0, (Reg*)outbuf); outbuf += ITEMS_PER_REG;
						exhaust = 1;
					}
					Item* endOpposite = endA;
					while (outbuf != endoutput && opposite != endOpposite) {
						origami_utils::load<Reg>(a0, (Reg*)opposite); opposite += ITEMS_PER_REG;
						origami_utils::rswap<Reg, Item>(a0, a1);
						origami_utils::store<Reg, false>(a0, (Reg*)outbuf); outbuf += ITEMS_PER_REG;
					}
					if (outbuf != endoutput) {
						if (exhaust0 == 2) {
							origami_utils::store<Reg, false>(a1, (Reg*)outbuf); outbuf += ITEMS_PER_REG;
							exhaust = 2;
							endoutput = outbuf;
						}
						Item* tmp = loadFrom; loadFrom = opposite; opposite = tmp;
					}
				}
			}

			*_loadFrom = loadFrom;
			*_opposite = opposite;
			*_endoutput = endoutput;
			//*_endoutput = outbuf;
		}
		else *_endoutput = outbuf;

#ifdef MTREE_DBG_PRINT
		printf("[O]: ");
		while (p < *_endoutput) printf("%lX ", *p++); printf("\n");

		printf("Exiting merge internal to internal ... \n");
#endif 
		return outbuf;
	}

	template <typename Reg, typename Item>
	FORCEINLINE void merge_root_aligned(Item** _loadFrom, Item** _opposite, Reg& a1, Item** _output, Item* endA, Item* endB, ui exhaust0, ui exhaust1) {
		constexpr ui ITEMS_PER_REG = sizeof(Reg) / sizeof(Item);
		Item* loadFrom = *_loadFrom;
		Item* opposite = *_opposite;
		Item* output = *_output;
		Reg a0;

		//printf("LF: %llX, OP: %llX, ENDA: %llX, ENDB: %llX\n", loadFrom, opposite, endA, endB);
		//printf("Exhaust0: %lu, Exhaust1: %lu\n", exhaust0, exhaust1); 

		while (loadFrom != endA && loadFrom != endB) {
			origami_utils::load<Reg>(a0, (Reg*)loadFrom); loadFrom += ITEMS_PER_REG;
			bool first = *loadFrom < *opposite;
			Item* tmp0 = first ? loadFrom : opposite;
			opposite = first ? opposite : loadFrom;
			loadFrom = tmp0;

			origami_utils::rswap<Reg, Item>(a0, a1);
			origami_utils::store<Reg, true>(a0, (Reg*)output);
			output += ITEMS_PER_REG;
			//PAVX(a0);
		}
		//printf("LF: %llX, OP: %llX, ENDA: %llX, ENDB: %llX\n", loadFrom, opposite, endA, endB);
		// handle tail
		if ((exhaust0 == 2 && loadFrom == endA)) {
			//printf("Handling tail\n");
			Item* endOpposite = endB;
			while (opposite != endOpposite) {
				origami_utils::load<Reg>(a0, (Reg*)opposite); opposite += ITEMS_PER_REG;
				origami_utils::rswap<Reg, Item>(a0, a1);
				origami_utils::store<Reg, true>(a0, (Reg*)output); output += ITEMS_PER_REG;
			}
			if (exhaust1 == 2) {
				origami_utils::store<Reg, true>(a1, (Reg*)output); output += ITEMS_PER_REG;
			}
			Item* tmp = loadFrom; loadFrom = opposite; opposite = tmp;	// std::swap is expensive
		}
		else if ((exhaust1 == 2 && loadFrom == endB)) {
			//printf("Handling tail\n");
			Item* endOpposite = endA;
			while (opposite != endOpposite) {
				origami_utils::load<Reg>(a0, (Reg*)opposite); opposite += ITEMS_PER_REG;
				origami_utils::rswap<Reg, Item>(a0, a1);
				origami_utils::store<Reg, true>(a0, (Reg*)output); output += ITEMS_PER_REG;
			}
			if (exhaust0 == 2) {
				origami_utils::store<Reg, true>(a1, (Reg*)output); output += ITEMS_PER_REG;
			}
			Item* tmp = loadFrom; loadFrom = opposite; opposite = tmp;
		}
		//printf("LF: %llX, OP: %llX, ENDA: %llX, ENDB: %llX\n", loadFrom, opposite, endA, endB);
		//origami_utils::store<Reg, true>(a0, (Reg*)output); output += ITEMS_PER_REG;
		*_loadFrom = loadFrom;
		*_opposite = opposite;
		*_output = output;
	}

	template <typename Reg, typename Item, bool external>
	FORCEINLINE void merge_root_unaligned(Item** _loadFrom, Item** _opposite, Reg& a1, Item** _output, Item* endA, Item* endB, ui exhaust0, ui exhaust1, Item** _outputEnd) {
		constexpr ui ITEMS_PER_REG = sizeof(Reg) / sizeof(Item);
		Item* loadFrom = *_loadFrom;
		Item* opposite = *_opposite;
		Item* output = *_output;
		Item* outputEnd = *_outputEnd;
		Reg a0;

		//printf("Extern: %d\n", external);
#ifdef MTREE_DBG_PRINT
		origami_utils::print<Reg, Item>(a1);
		printf("Merging at root ...\n");
		Item* p = (*_loadFrom < *_opposite ? *_loadFrom : *_opposite), * endp = endA;
		printf("[L]: ");
		while (p < endp) printf("%lX ", *p++); printf("\n");

		p = (*_loadFrom < *_opposite ? *_opposite : *_loadFrom); endp = endB;
		printf("[R]: ");
		while (p < endp) printf("%lX ", *p++); printf("\n");

		p = output;
#endif 

		while (true) {
			while (loadFrom != endA && loadFrom != endB && output != outputEnd) { // 
				origami_utils::load<Reg>(a0, (Reg*)loadFrom); loadFrom += ITEMS_PER_REG;
				bool first = *loadFrom < *opposite;
				Item* tmp0 = first ? loadFrom : opposite;
				opposite = first ? opposite : loadFrom;
				loadFrom = tmp0;

				origami_utils::rswap<Reg, Item>(a0, a1);
				origami_utils::store<Reg, true>(a0, (Reg*)output);

				output += ITEMS_PER_REG;
			}
			if constexpr (external) {
				if (output != outputEnd) break;
				// else: output buffer full, need to flush; function returns pointer to the beginning of output buffer
				process_buffer(-1, (char**)&output, (char**)&outputEnd);
			}
			else break;
		}

		// handle tail
		if (output != outputEnd) {
			if (exhaust0 == 2 && loadFrom == endA) {
				//printf("Handling tail\n");
				Item* endOpposite = endB;
				while (true) {
					while (opposite != endOpposite && output != outputEnd) {
						origami_utils::load<Reg>(a0, (Reg*)opposite); opposite += ITEMS_PER_REG;
						origami_utils::rswap<Reg, Item>(a0, a1);
						origami_utils::store<Reg, true>(a0, (Reg*)output); output += ITEMS_PER_REG;
					}
					if constexpr (external) {
						if (output == outputEnd) {
							//printf("Output buffer full ... writing to file\n");
							Item* p, * endp;
							process_buffer(-1, (char**)&output, (char**)&outputEnd);
							continue;
						}
						if (exhaust1 == 2) {
							origami_utils::store<Reg, true>(a1, (Reg*)output); output += ITEMS_PER_REG;
							process_buffer(-1, (char**)&output, (char**)&output);
							output = outputEnd;		// overwrite to output ptr since this is the last batch of data
						}
						break;
					}
					else {
						if (exhaust1 == 2 && output != outputEnd) {
							origami_utils::store<Reg, true>(a1, (Reg*)output); output += ITEMS_PER_REG;
						}
						break;
					}
				}
				Item* tmp = loadFrom; loadFrom = opposite; opposite = tmp;	// std::swap is expensive
			}
			else if (exhaust1 == 2 && loadFrom == endB) {
				//printf("Handling tail\n");
				Item* endOpposite = endA;
				while (true) {
					while (opposite != endOpposite && output != outputEnd) {
						origami_utils::load<Reg>(a0, (Reg*)opposite); opposite += ITEMS_PER_REG;
						origami_utils::rswap<Reg, Item>(a0, a1);
						origami_utils::store<Reg, true>(a0, (Reg*)output); output += ITEMS_PER_REG;
					}
					if constexpr (external) {
						if (output == outputEnd) {
							//printf("Output buffer full ... writing to file\n");
							process_buffer(-1, (char**)&output, (char**)&outputEnd);
							continue;
						}
						if (exhaust0 == 2) {
							origami_utils::store<Reg, true>(a1, (Reg*)output); output += ITEMS_PER_REG;
							process_buffer(-1, (char**)&output, (char**)&output);
							output = outputEnd;		// overwrite to output ptr since this is the last batch of data
						}
						break;
					}
					else {
						if (exhaust0 == 2 && output != outputEnd) {
							origami_utils::store<Reg, true>(a1, (Reg*)output); output += ITEMS_PER_REG;
						}
						break;
					}
				}
				Item* tmp = loadFrom; loadFrom = opposite; opposite = tmp;
			}
		}
		//printf("LF: %llX, OP: %llX, ENDA: %llX, ENDB: %llX\n", loadFrom, opposite, endA, endB);
		//origami_utils::store<Reg, true>(a0, (Reg*)output); output += ITEMS_PER_REG;
		*_loadFrom = loadFrom;
		*_opposite = opposite;
		*_output = output;
		*_outputEnd = outputEnd;

#ifdef MTREE_DBG_PRINT
		tot_sorted += (output - p);
		printf("[O]: ");
		while (p < output) printf("%lX ", *p++); printf("\n");
		printf("Exiting root merge\n");

		printf("Total sorted: %llu\n", tot_sorted);
#endif 
	}

	template <typename Reg, typename Item>
	FORCEINLINE void merge_root_unaligned_skip_prior(Item** _loadFrom, Item** _opposite, Reg& a1, Item* endA, Item* endB, ui exhaust0, ui exhaust1, ui* prior_unalign_items) {
		constexpr ui ITEMS_PER_REG = sizeof(Reg) / sizeof(Item);
		Item* loadFrom = *_loadFrom;
		Item* opposite = *_opposite;
		ui prior_unalign_items_loc = *prior_unalign_items;
		Reg a0;

		while (loadFrom != endA && loadFrom != endB && prior_unalign_items_loc > 0) { // 
			origami_utils::load<Reg>(a0, (Reg*)loadFrom); loadFrom += ITEMS_PER_REG;
			bool first = *loadFrom < *opposite;
			Item* tmp0 = first ? loadFrom : opposite;
			opposite = first ? opposite : loadFrom;
			loadFrom = tmp0;

			origami_utils::rswap<Reg, Item>(a0, a1);

			prior_unalign_items_loc -= ITEMS_PER_REG;
		}

		*_loadFrom = loadFrom;
		*_opposite = opposite;
		*prior_unalign_items = prior_unalign_items_loc;
	}

	template <typename Reg, typename Item, bool external = false>
	class Merge4WayNode {
	public:
		Item* l1Buf0, * l1Buf1, * l1endBuf0, * l1endBuf1;
		Item* output, * outputEnd;
		Reg a0, a1, a2, a3, a4, a5;
		Item* leafEnd0, * leafEnd1, * leafEnd2, * leafEnd3;
		Item* loadFrom0, * opposite0, * loadFrom1, * opposite1, * loadFrom, * opposite;
		ui exhaust0, exhaust1, exhaust2, exhaust3, exhaust4, exhaust5, exhaust;
		Item* l1endBuf0_back, * l1endBuf1_back, * outputEnd_back;

		void print_node() {
			printf("[%llX %llX]\n", output, outputEnd);
			printf("[%llX %llX] [%llX %llX]\n", l1Buf0, l1endBuf0, l1Buf1, l1endBuf1);
			printf("[%llX %llX] [%llX %llX] [%llX %llX] [%llX %llX]\n", loadFrom0, leafEnd0, opposite0, leafEnd1, loadFrom1, leafEnd2, opposite1, leafEnd3);

			// print data
			printf("Child 0: ");
			Item* p = (loadFrom0 < opposite0) ? loadFrom0 : opposite0, * endp = leafEnd0;
			while (p < endp) printf("%lX ", *p++); printf("\n");
			printf("Child 1: ");
			p = (loadFrom0 < opposite0) ? opposite0 : loadFrom0, endp = leafEnd1;
			while (p < endp) printf("%lX ", *p++); printf("\n");
			printf("Child 2: ");
			p = (loadFrom1 < opposite1) ? loadFrom1 : opposite1, endp = leafEnd2;
			while (p < endp) printf("%lX ", *p++); printf("\n");
			printf("Child 3: ");
			p = (loadFrom1 < opposite1) ? opposite1 : loadFrom1, endp = leafEnd3;
			while (p < endp) printf("%lX ", *p++); printf("\n");
			/*printf("Internal 0: ");
			p = l1Buf0, endp = l1endBuf0;
			while (p < endp) printf("%lX ", *p++); printf("\n");
			printf("Internal 1: ");
			p = l1Buf1, endp = l1endBuf1;
			while (p < endp) printf("%lX ", *p++); printf("\n");*/
			/*printf("Root: ");
			p = output, endp = outputEnd;
			while (p < endp) printf("%lX ", *p++); printf("\n");*/
			PRINT_DASH(20);

		}

		void merge_leaf_to_root_init(ui base_idx = 0) {
			constexpr ui ITEMS_PER_REG = sizeof(Reg) / sizeof(Item);
			Item* loadFrom0 = this->loadFrom0;
			Item* loadFrom1 = this->loadFrom1;
			Item* loadFrom = this->loadFrom;
			Item* opposite0 = this->opposite0;
			Item* opposite1 = this->opposite1;
			Item* opposite = this->opposite;
			Item* l1Buf0 = this->l1Buf0;
			Item* l1Buf1 = this->l1Buf1;
			Item* l1endBuf0 = this->l1endBuf0;
			Item* l1endBuf1 = this->l1endBuf1;

			Reg a0 = this->a0;
			Reg a1 = this->a1;
			Reg a2 = this->a2;
			Reg a3 = this->a3;
			Reg a5 = this->a5;

			Item* leafEnd0 = this->leafEnd0;
			Item* leafEnd1 = this->leafEnd1;
			Item* leafEnd2 = this->leafEnd2;
			Item* leafEnd3 = this->leafEnd3;

			origami_utils::load<Reg>(a0, (Reg*)loadFrom0); loadFrom0 += ITEMS_PER_REG;
			origami_utils::load<Reg>(a1, (Reg*)opposite0); opposite0 += ITEMS_PER_REG;
			origami_utils::load<Reg>(a2, (Reg*)loadFrom1); loadFrom1 += ITEMS_PER_REG;
			origami_utils::load<Reg>(a3, (Reg*)opposite1); opposite1 += ITEMS_PER_REG;

			origami_merge_tree::merge_leaf_to_internal<Reg, Item, external>(&loadFrom0, &opposite0, a0, a1, l1Buf0, &leafEnd0, &leafEnd1, &l1endBuf0, exhaust0, base_idx);
			origami_merge_tree::merge_leaf_to_internal<Reg, Item, external>(&loadFrom1, &opposite1, a2, a3, l1Buf1, &leafEnd2, &leafEnd3, &l1endBuf1, exhaust1, base_idx + 2);

			origami_utils::load<Reg>(a5, (Reg*)opposite); opposite += ITEMS_PER_REG;

			this->loadFrom0 = loadFrom0;
			this->loadFrom1 = loadFrom1;
			this->loadFrom = loadFrom;
			this->opposite0 = opposite0;
			this->opposite1 = opposite1;
			this->opposite = opposite;

			this->a0 = a0;
			this->a1 = a1;
			this->a2 = a2;
			this->a3 = a3;
			this->a5 = a5;

			this->l1endBuf0 = l1endBuf0;
			this->l1endBuf1 = l1endBuf1;
			this->leafEnd0 = leafEnd0;
			this->leafEnd1 = leafEnd1;
			this->leafEnd2 = leafEnd2;
			this->leafEnd3 = leafEnd3;
		}

		void merge_leaf_to_internal_init(ui base_idx = 0) {
			/*printf("Leaf to internal init merging ... \n");
			this->print_node();*/

			constexpr ui ITEMS_PER_REG = sizeof(Reg) / sizeof(Item);
			Item* loadFrom0 = this->loadFrom0;
			Item* loadFrom1 = this->loadFrom1;
			Item* loadFrom = this->loadFrom;
			Item* opposite0 = this->opposite0;
			Item* opposite1 = this->opposite1;
			Item* opposite = this->opposite;
			Item* l1Buf0 = this->l1Buf0;
			Item* l1Buf1 = this->l1Buf1;
			Item* l1endBuf0 = this->l1endBuf0;
			Item* l1endBuf1 = this->l1endBuf1;

			Reg a0 = this->a0;
			Reg a1 = this->a1;
			Reg a2 = this->a2;
			Reg a3 = this->a3;
			Reg a5 = this->a5;

			Item* leafEnd0 = this->leafEnd0;
			Item* leafEnd1 = this->leafEnd1;
			Item* leafEnd2 = this->leafEnd2;
			Item* leafEnd3 = this->leafEnd3;

			origami_utils::load<Reg>(a0, (Reg*)loadFrom0); loadFrom0 += ITEMS_PER_REG;
			origami_utils::load<Reg>(a1, (Reg*)opposite0); opposite0 += ITEMS_PER_REG;
			origami_utils::load<Reg>(a2, (Reg*)loadFrom1); loadFrom1 += ITEMS_PER_REG;
			origami_utils::load<Reg>(a3, (Reg*)opposite1); opposite1 += ITEMS_PER_REG;

			origami_merge_tree::merge_leaf_to_internal<Reg, Item, external>(&loadFrom0, &opposite0, a0, a1, l1Buf0, &leafEnd0, &leafEnd1, &l1endBuf0, exhaust0, base_idx);
			origami_merge_tree::merge_leaf_to_internal<Reg, Item, external>(&loadFrom1, &opposite1, a2, a3, l1Buf1, &leafEnd2, &leafEnd3, &l1endBuf1, exhaust1, base_idx + 2);

			origami_utils::load<Reg>(a4, (Reg*)loadFrom); loadFrom += ITEMS_PER_REG;
			origami_utils::load<Reg>(a5, (Reg*)opposite); opposite += ITEMS_PER_REG;

			this->loadFrom0 = loadFrom0;
			this->loadFrom1 = loadFrom1;
			this->loadFrom = loadFrom;
			this->opposite0 = opposite0;
			this->opposite1 = opposite1;
			this->opposite = opposite;
			this->l1endBuf0 = l1endBuf0;
			this->l1endBuf1 = l1endBuf1;
			this->leafEnd0 = leafEnd0;
			this->leafEnd1 = leafEnd1;
			this->leafEnd2 = leafEnd2;
			this->leafEnd3 = leafEnd3;

			this->a0 = a0;
			this->a1 = a1;
			this->a2 = a2;
			this->a3 = a3;
			this->a4 = a4;
			this->a5 = a5;

			/*printf("Leaf to internal init after merge ... \n");
			this->print_node();*/
		}

		void merge_internal_to_internal_init() {
			constexpr ui ITEMS_PER_REG = sizeof(Reg) / sizeof(Item);
			Item* loadFrom0 = this->loadFrom0;
			Item* loadFrom1 = this->loadFrom1;
			Item* loadFrom = this->loadFrom;
			Item* opposite0 = this->opposite0;
			Item* opposite1 = this->opposite1;
			Item* opposite = this->opposite;
			Item* l1Buf0 = this->l1Buf0;
			Item* l1Buf1 = this->l1Buf1;
			Item* l1endBuf0 = this->l1endBuf0;
			Item* l1endBuf1 = this->l1endBuf1;

			Reg a0 = this->a0;
			Reg a1 = this->a1;
			Reg a2 = this->a2;
			Reg a3 = this->a3;
			Reg a5 = this->a5;

			Item* leafEnd0 = this->leafEnd0;
			Item* leafEnd1 = this->leafEnd1;
			Item* leafEnd2 = this->leafEnd2;
			Item* leafEnd3 = this->leafEnd3;

			ui exhaust0 = this->exhaust0;
			ui exhaust1 = this->exhaust1;
			ui exhaust2 = this->exhaust2;
			ui exhaust3 = this->exhaust3;
			ui exhaust4 = this->exhaust4;
			ui exhaust5 = this->exhaust5;

			origami_utils::load<Reg>(a0, (Reg*)loadFrom0); loadFrom0 += ITEMS_PER_REG;
			origami_utils::load<Reg>(a1, (Reg*)opposite0); opposite0 += ITEMS_PER_REG;
			origami_utils::load<Reg>(a2, (Reg*)loadFrom1); loadFrom1 += ITEMS_PER_REG;
			origami_utils::load<Reg>(a3, (Reg*)opposite1); opposite1 += ITEMS_PER_REG;

			origami_merge_tree::merge_internal_to_internal(&loadFrom0, &opposite0, a0, a1, l1Buf0, leafEnd0, leafEnd1, &l1endBuf0, exhaust0, exhaust1, exhaust4);
			origami_merge_tree::merge_internal_to_internal(&loadFrom1, &opposite1, a2, a3, l1Buf1, leafEnd2, leafEnd3, &l1endBuf1, exhaust2, exhaust3, exhaust5);


			origami_utils::load<Reg>(a4, (Reg*)loadFrom); loadFrom += ITEMS_PER_REG;
			origami_utils::load<Reg>(a5, (Reg*)opposite); opposite += ITEMS_PER_REG;

			this->loadFrom0 = loadFrom0;
			this->loadFrom1 = loadFrom1;
			this->loadFrom = loadFrom;
			this->opposite0 = opposite0;
			this->opposite1 = opposite1;
			this->opposite = opposite;
			this->l1endBuf0 = l1endBuf0;
			this->l1endBuf1 = l1endBuf1;

			this->a0 = a0;
			this->a1 = a1;
			this->a2 = a2;
			this->a3 = a3;
			this->a4 = a4;
			this->a5 = a5;

			this->exhaust4 = exhaust4;
			this->exhaust5 = exhaust5;
		}

		void merge_internal_to_root_init() {
			/*printf("Internal to root init merging ... \n");
			this->print_node();*/

			constexpr ui ITEMS_PER_REG = sizeof(Reg) / sizeof(Item);
			Item* loadFrom0 = this->loadFrom0;
			Item* loadFrom1 = this->loadFrom1;
			Item* loadFrom = this->loadFrom;
			Item* opposite0 = this->opposite0;
			Item* opposite1 = this->opposite1;
			Item* opposite = this->opposite;
			Item* l1Buf0 = this->l1Buf0;
			Item* l1Buf1 = this->l1Buf1;
			Item* l1endBuf0 = this->l1endBuf0;
			Item* l1endBuf1 = this->l1endBuf1;

			Reg a0 = this->a0;
			Reg a1 = this->a1;
			Reg a2 = this->a2;
			Reg a3 = this->a3;
			Reg a5 = this->a5;

			Item* leafEnd0 = this->leafEnd0;
			Item* leafEnd1 = this->leafEnd1;
			Item* leafEnd2 = this->leafEnd2;
			Item* leafEnd3 = this->leafEnd3;

			ui exhaust4 = this->exhaust4;
			ui exhaust5 = this->exhaust5;

			origami_utils::load<Reg>(a0, (Reg*)loadFrom0); loadFrom0 += ITEMS_PER_REG;
			origami_utils::load<Reg>(a1, (Reg*)opposite0); opposite0 += ITEMS_PER_REG;
			origami_utils::load<Reg>(a2, (Reg*)loadFrom1); loadFrom1 += ITEMS_PER_REG;
			origami_utils::load<Reg>(a3, (Reg*)opposite1); opposite1 += ITEMS_PER_REG;

			origami_merge_tree::merge_internal_to_internal(&loadFrom0, &opposite0, a0, a1, l1Buf0, leafEnd0, leafEnd1, &l1endBuf0, exhaust0, exhaust1, exhaust4);
			origami_merge_tree::merge_internal_to_internal(&loadFrom1, &opposite1, a2, a3, l1Buf1, leafEnd2, leafEnd3, &l1endBuf1, exhaust2, exhaust3, exhaust5);

			//origami_utils::load<Reg>(a4, (Reg*)loadFrom); loadFrom += ITEMS_PER_REG;
			origami_utils::load<Reg>(a5, (Reg*)opposite); opposite += ITEMS_PER_REG;

			this->loadFrom0 = loadFrom0;
			this->loadFrom1 = loadFrom1;
			this->loadFrom = loadFrom;
			this->opposite0 = opposite0;
			this->opposite1 = opposite1;
			this->opposite = opposite;
			this->l1endBuf0 = l1endBuf0;
			this->l1endBuf1 = l1endBuf1;

			this->a0 = a0;
			this->a1 = a1;
			this->a2 = a2;
			this->a3 = a3;
			//this->a4 = a4;
			this->a5 = a5;

			this->exhaust4 = exhaust4;
			this->exhaust5 = exhaust5;

			/*printf("Internal to root init after merging ... \n");
			this->print_node();*/
		}

		void merge_leaf_to_root() {
			constexpr ui ITEMS_PER_REG = sizeof(Reg) / sizeof(Item);
			Item* loadFrom0 = this->loadFrom0;
			Item* loadFrom1 = this->loadFrom1;
			Item* loadFrom = this->loadFrom;
			Item* opposite0 = this->opposite0;
			Item* opposite1 = this->opposite1;
			Item* opposite = this->opposite;
			Item* l1Buf0 = this->l1Buf0;
			Item* l1Buf1 = this->l1Buf1;
			Item* l1endBuf0 = this->l1endBuf0;
			Item* l1endBuf1 = this->l1endBuf1;
			Item* output = this->output;
			Item* outputEnd = this->outputEnd;
			Reg a0 = this->a0;
			Reg a1 = this->a1;
			Reg a2 = this->a2;
			Reg a3 = this->a3;
			Reg a5 = this->a5;
			Item* leafEnd0 = this->leafEnd0;
			Item* leafEnd1 = this->leafEnd1;
			Item* leafEnd2 = this->leafEnd2;
			Item* leafEnd3 = this->leafEnd3;



			// merge
			Item* o = output;
			while (output < outputEnd) {
				merge_root_aligned(&loadFrom, &opposite, a5, &output, l1endBuf0, l1endBuf1, exhaust0, exhaust1);
				//printf("Merged: %llu\n", output - o);

				// refill empty child
				if (exhaust0 < 2 && loadFrom == l1endBuf0) {
					//merge_leaf_to_internal<Reg, Item, external>(&loadFrom0, &opposite0, a0, a1, l1Buf0, &leafEnd0, &leafEnd1, &l1endBuf0, exhaust0, 0);
					if constexpr (external) merge_leaf_to_internal<Reg, Item>(&loadFrom0, &opposite0, a0, a1, l1Buf0, &leafEnd0, &leafEnd1, &l1endBuf0, exhaust0, 0);
					loadFrom = l1Buf0;
				}
				else if (exhaust1 < 2 && loadFrom == l1endBuf1) {
					//merge_leaf_to_internal<Reg, Item, external>(&loadFrom1, &opposite1, a2, a3, l1Buf1, &leafEnd2, &leafEnd3, &l1endBuf1, exhaust1, 2);
					if constexpr (external) merge_leaf_to_internal<Reg, Item, external>(&loadFrom1, &opposite1, a2, a3, l1Buf1, &leafEnd2, &leafEnd3, &l1endBuf1, exhaust1, 2);
					loadFrom = l1Buf1;
				}
				bool first = *loadFrom < *opposite;
				Item* tmp0 = first ? loadFrom : opposite;
				opposite = first ? opposite : loadFrom;
				loadFrom = tmp0;
			}
			this->loadFrom0 = loadFrom0;
			this->loadFrom1 = loadFrom1;
			this->loadFrom = loadFrom;
			this->opposite0 = opposite0;
			this->opposite1 = opposite1;
			this->opposite = opposite;

			this->leafEnd0 = leafEnd0;
			this->leafEnd1 = leafEnd1;
			this->leafEnd2 = leafEnd2;
			this->leafEnd3 = leafEnd3;

			this->l1Buf0 = l1Buf0;
			this->l1Buf1 = l1Buf1;

			this->output = output;
			this->outputEnd = outputEnd;
			this->a0 = a0;
			this->a1 = a1;
			this->a2 = a2;
			this->a3 = a3;
			this->a5 = a5;
		}

		void merge_leaf_to_root_unaligned(ui* prior_unalign_items) {
			constexpr ui ITEMS_PER_REG = sizeof(Reg) / sizeof(Item);
			Item* loadFrom0 = this->loadFrom0;
			Item* loadFrom1 = this->loadFrom1;
			Item* loadFrom = this->loadFrom;
			Item* opposite0 = this->opposite0;
			Item* opposite1 = this->opposite1;
			Item* opposite = this->opposite;
			Item* l1Buf0 = this->l1Buf0;
			Item* l1Buf1 = this->l1Buf1;
			Item* l1endBuf0 = this->l1endBuf0;
			Item* l1endBuf1 = this->l1endBuf1;
			Item* output = this->output;
			Item* outputEnd = this->outputEnd;
			Reg a0 = this->a0;
			Reg a1 = this->a1;
			Reg a2 = this->a2;
			Reg a3 = this->a3;
			Reg a5 = this->a5;
			Item* leafEnd0 = this->leafEnd0;
			Item* leafEnd1 = this->leafEnd1;
			Item* leafEnd2 = this->leafEnd2;
			Item* leafEnd3 = this->leafEnd3;

			// merge

			// skip prior unaligned items
			while (1) {
				merge_root_unaligned_skip_prior(&loadFrom, &opposite, a5, l1endBuf0, l1endBuf1, exhaust0, exhaust1, prior_unalign_items);
				if (*prior_unalign_items == 0) break;
				// refill empty child
				if (exhaust0 < 2 && loadFrom == l1endBuf0) {
					origami_merge_tree::merge_leaf_to_internal<Reg, Item, external>(&loadFrom0, &opposite0, a0, a1, l1Buf0, &leafEnd0, &leafEnd1, &l1endBuf0, exhaust0, 0);
					loadFrom = l1Buf0;
				}
				else if (exhaust1 < 2 && loadFrom == l1endBuf1) {
					origami_merge_tree::merge_leaf_to_internal<Reg, Item, external>(&loadFrom1, &opposite1, a2, a3, l1Buf1, &leafEnd2, &leafEnd3, &l1endBuf1, exhaust1, 2);
					loadFrom = l1Buf1;
				}
				bool first = *loadFrom < *opposite;
				Item* tmp0 = first ? loadFrom : opposite;
				opposite = first ? opposite : loadFrom;
				loadFrom = tmp0;
			}


			Item* o = output;
			while (output < outputEnd) {
				merge_root_unaligned<Reg, Item, external>(&loadFrom, &opposite, a5, &output, l1endBuf0, l1endBuf1, exhaust0, exhaust1, &outputEnd);
				//printf("Merged: %llu\n", output - o);

				// refill empty child
				if (exhaust0 < 2 && loadFrom == l1endBuf0) {
					origami_merge_tree::merge_leaf_to_internal<Reg, Item, external>(&loadFrom0, &opposite0, a0, a1, l1Buf0, &leafEnd0, &leafEnd1, &l1endBuf0, exhaust0, 0);
					loadFrom = l1Buf0;
				}
				else if (exhaust1 < 2 && loadFrom == l1endBuf1) {
					origami_merge_tree::merge_leaf_to_internal<Reg, Item, external>(&loadFrom1, &opposite1, a2, a3, l1Buf1, &leafEnd2, &leafEnd3, &l1endBuf1, exhaust1, 2);
					loadFrom = l1Buf1;
				}
				bool first = *loadFrom < *opposite;
				Item* tmp0 = first ? loadFrom : opposite;
				opposite = first ? opposite : loadFrom;
				loadFrom = tmp0;
			}
			this->loadFrom0 = loadFrom0;
			this->loadFrom1 = loadFrom1;
			this->loadFrom = loadFrom;
			this->opposite0 = opposite0;
			this->opposite1 = opposite1;
			this->opposite = opposite;

			this->l1Buf0 = l1Buf0;
			this->l1Buf1 = l1Buf1;
			this->leafEnd0 = leafEnd0;
			this->leafEnd1 = leafEnd1;
			this->leafEnd2 = leafEnd2;
			this->leafEnd3 = leafEnd3;

			this->output = output;
			this->outputEnd = outputEnd;
			this->a0 = a0;
			this->a1 = a1;
			this->a2 = a2;
			this->a3 = a3;
			this->a5 = a5;
		}

		void merge_leaf_to_internal(ui base_idx = 0) {
			constexpr ui ITEMS_PER_REG = sizeof(Reg) / sizeof(Item);
			Item* loadFrom0 = this->loadFrom0;
			Item* loadFrom1 = this->loadFrom1;
			Item* loadFrom = this->loadFrom;
			Item* opposite0 = this->opposite0;
			Item* opposite1 = this->opposite1;
			Item* opposite = this->opposite;

			Item* l1Buf0 = this->l1Buf0;
			Item* l1Buf1 = this->l1Buf1;
			Item* l1endBuf0 = this->l1endBuf0;
			Item* l1endBuf1 = this->l1endBuf1;

			Item* output = this->output;
			Item* outputEnd = this->outputEnd;

			//printf("Filling leaf to internal: [%llX %llX]\n", output, outputEnd);

			Reg a0 = this->a0;
			Reg a1 = this->a1;
			Reg a2 = this->a2;
			Reg a3 = this->a3;
			Reg a4 = this->a4;
			Reg a5 = this->a5;

			Item* leafEnd0 = this->leafEnd0;
			Item* leafEnd1 = this->leafEnd1;
			Item* leafEnd2 = this->leafEnd2;
			Item* leafEnd3 = this->leafEnd3;

			ui exhaust0 = this->exhaust0;
			ui exhaust1 = this->exhaust1;
			ui exhaust = this->exhaust;

			// merge
			ui64 cnt = 0;
			while (output < outputEnd) {
				//do {
					// refill empty child
				if (exhaust0 < 2 && loadFrom == l1endBuf0) {
					origami_merge_tree::merge_leaf_to_internal<Reg, Item, external>(&loadFrom0, &opposite0, a0, a1, l1Buf0, &leafEnd0, &leafEnd1, &l1endBuf0, exhaust0, base_idx);
					loadFrom = l1Buf0;
				}
				else if (exhaust1 < 2 && loadFrom == l1endBuf1) {
					origami_merge_tree::merge_leaf_to_internal<Reg, Item, external>(&loadFrom1, &opposite1, a2, a3, l1Buf1, &leafEnd2, &leafEnd3, &l1endBuf1, exhaust1, base_idx + 2);
					loadFrom = l1Buf1;
				}
				bool first = *(loadFrom) < *(opposite);
				Item* tmp0 = first ? loadFrom : opposite;
				opposite = first ? opposite : loadFrom;
				loadFrom = tmp0;
				//Item* o = output;
				output = origami_merge_tree::merge_internal_to_internal(&loadFrom, &opposite, a4, a5, output, l1endBuf0, l1endBuf1, &outputEnd, exhaust0, exhaust1, exhaust);
				//PRINT_ARR64(o, output - o); cnt += (output - o); printf("Tot: %llu\n", cnt);
			}
			//} while (output < outputEnd);
			this->loadFrom0 = loadFrom0;
			this->loadFrom1 = loadFrom1;
			this->loadFrom = loadFrom;
			this->opposite0 = opposite0;
			this->opposite1 = opposite1;
			this->opposite = opposite;

			this->l1Buf0 = l1Buf0;
			this->l1Buf1 = l1Buf1;
			this->l1endBuf0 = l1endBuf0;
			this->l1endBuf1 = l1endBuf1;
			this->leafEnd0 = leafEnd0;
			this->leafEnd1 = leafEnd1;
			this->leafEnd2 = leafEnd2;
			this->leafEnd3 = leafEnd3;

			this->exhaust0 = exhaust0;
			this->exhaust1 = exhaust1;
			this->exhaust = exhaust;

			this->outputEnd = outputEnd;
			this->a0 = a0;
			this->a1 = a1;
			this->a2 = a2;
			this->a3 = a3;
			this->a4 = a4;
			this->a5 = a5;
		}

		ui merge_internal_to_internal() {
			constexpr ui ITEMS_PER_REG = sizeof(Reg) / sizeof(Item);
			Item* loadFrom0 = this->loadFrom0;
			Item* loadFrom1 = this->loadFrom1;
			Item* loadFrom = this->loadFrom;
			Item* opposite0 = this->opposite0;
			Item* opposite1 = this->opposite1;
			Item* opposite = this->opposite;
			Item* l1Buf0 = this->l1Buf0;
			Item* l1Buf1 = this->l1Buf1;
			Item* l1endBuf0 = this->l1endBuf0;
			Item* l1endBuf1 = this->l1endBuf1;
			Item* output = this->output;
			Item* outputEnd = this->outputEnd;
			Reg a0 = this->a0;
			Reg a1 = this->a1;
			Reg a2 = this->a2;
			Reg a3 = this->a3;
			Reg a4 = this->a4;
			Reg a5 = this->a5;
			Item* leafEnd0 = this->leafEnd0;
			Item* leafEnd1 = this->leafEnd1;
			Item* leafEnd2 = this->leafEnd2;
			Item* leafEnd3 = this->leafEnd3;

			ui exhaust0 = this->exhaust0;
			ui exhaust1 = this->exhaust1;
			ui exhaust2 = this->exhaust2;
			ui exhaust3 = this->exhaust3;
			ui exhaust4 = this->exhaust4;
			ui exhaust5 = this->exhaust5;
			ui exhaust = this->exhaust;

			ui flag = 0;
			outputEnd = outputEnd_back;

			do {
				// refill empty child
				if (exhaust4 < 2 && loadFrom == l1endBuf0) {
					l1endBuf0 = l1endBuf0_back;
					l1endBuf0 = origami_merge_tree::merge_internal_to_internal(&loadFrom0, &opposite0, a0, a1, l1Buf0, leafEnd0, leafEnd1, &l1endBuf0, exhaust0, exhaust1, exhaust4);
					loadFrom = l1Buf0;
				}
				else if (exhaust5 < 2 && loadFrom == l1endBuf1) {
					l1endBuf1 = l1endBuf1_back;
					l1endBuf1 = origami_merge_tree::merge_internal_to_internal(&loadFrom1, &opposite1, a2, a3, l1Buf1, leafEnd2, leafEnd3, &l1endBuf1, exhaust2, exhaust3, exhaust5);
					loadFrom = l1Buf1;
				}
				bool first = *(loadFrom) < *(opposite);
				Item* tmp0 = first ? loadFrom : opposite;
				opposite = first ? opposite : loadFrom;
				loadFrom = tmp0;

				output = origami_merge_tree::merge_internal_to_internal(&loadFrom, &opposite, a4, a5, output, l1endBuf0, l1endBuf1, &outputEnd, exhaust4, exhaust5, exhaust);
				//PRINT_ARR(o, output - o);

				// check if any leaf node is empty
				ui empty0 = loadFrom0 == leafEnd0;
				ui empty1 = loadFrom0 == leafEnd1;
				ui empty2 = loadFrom1 == leafEnd2;
				ui empty3 = loadFrom1 == leafEnd3;
				flag = empty0 | (empty1 << 1) | (empty2 << 2) | (empty3 << 3);
				//printf("Flag: %llx\n", flag);

			} while (flag == 0 && output < outputEnd);
			outputEnd = output;

			this->loadFrom0 = loadFrom0;
			this->loadFrom1 = loadFrom1;
			this->loadFrom = loadFrom;
			this->opposite0 = opposite0;
			this->opposite1 = opposite1;
			this->opposite = opposite;

			this->l1Buf0 = l1Buf0;
			this->l1Buf1 = l1Buf1;
			this->l1endBuf0 = l1endBuf0;
			this->l1endBuf1 = l1endBuf1;

			//this->output = output;
			this->outputEnd = outputEnd;

			this->exhaust0 = exhaust0;
			this->exhaust1 = exhaust1;
			this->exhaust2 = exhaust2;
			this->exhaust3 = exhaust3;
			this->exhaust4 = exhaust4;
			this->exhaust5 = exhaust5;
			this->exhaust = exhaust;

			this->a0 = a0;
			this->a1 = a1;
			this->a2 = a2;
			this->a3 = a3;
			this->a4 = a4;
			this->a5 = a5;

			return flag;
		}

		ui merge_internal_to_root() {
			constexpr ui ITEMS_PER_REG = sizeof(Reg) / sizeof(Item);
			Item* loadFrom0 = this->loadFrom0;
			Item* loadFrom1 = this->loadFrom1;
			Item* loadFrom = this->loadFrom;
			Item* opposite0 = this->opposite0;
			Item* opposite1 = this->opposite1;
			Item* opposite = this->opposite;
			Item* l1Buf0 = this->l1Buf0;
			Item* l1Buf1 = this->l1Buf1;
			Item* l1endBuf0 = this->l1endBuf0;
			Item* l1endBuf1 = this->l1endBuf1;
			Item* output = this->output;
			Item* outputEnd = this->outputEnd;
			Reg a0 = this->a0;
			Reg a1 = this->a1;
			Reg a2 = this->a2;
			Reg a3 = this->a3;
			Reg a4 = this->a4;
			Reg a5 = this->a5;
			Item* leafEnd0 = this->leafEnd0;
			Item* leafEnd1 = this->leafEnd1;
			Item* leafEnd2 = this->leafEnd2;
			Item* leafEnd3 = this->leafEnd3;

			ui exhaust0 = this->exhaust0;
			ui exhaust1 = this->exhaust1;
			ui exhaust2 = this->exhaust2;
			ui exhaust3 = this->exhaust3;
			ui exhaust4 = this->exhaust4;
			ui exhaust5 = this->exhaust5;
			ui exhaust = this->exhaust;
			// merge
			ui flag = 0;
			do {
				// refill empty child
				if (exhaust4 < 2 && loadFrom == l1endBuf0) {
					l1endBuf0 = l1endBuf0_back;
					l1endBuf0 = merge_internal_to_internal(&loadFrom0, &opposite0, a0, a1, l1Buf0, leafEnd0, leafEnd1, &l1endBuf0, exhaust0, exhaust1, exhaust4);
					loadFrom = l1Buf0;
				}
				else if (exhaust5 < 2 && loadFrom == l1endBuf1) {
					l1endBuf1 = l1endBuf1_back;
					l1endBuf1 = merge_internal_to_internal(&loadFrom1, &opposite1, a2, a3, l1Buf1, leafEnd2, leafEnd3, &l1endBuf1, exhaust2, exhaust3, exhaust5);
					loadFrom = l1Buf1;
				}
				bool first = *(loadFrom) < *(opposite);
				Item* tmp0 = first ? loadFrom : opposite;
				opposite = first ? opposite : loadFrom;
				loadFrom = tmp0;

				// from L1 to root

				Item* o = output;
				merge_root_unaligned<Reg, Item, external>(&loadFrom, &opposite, a5, &output, l1endBuf0, l1endBuf1, exhaust4, exhaust5, outputEnd);

				//PRINT_ARR(o, output - o);

				// check if any leaf node is empty
				ui empty0 = (loadFrom0 == leafEnd0);// && (exhaust0 < 2);
				ui empty1 = (loadFrom0 == leafEnd1);// && (exhaust1 < 2);
				ui empty2 = (loadFrom1 == leafEnd2);// && (exhaust2 < 2);
				ui empty3 = (loadFrom1 == leafEnd3);// && (exhaust3 < 2);
				flag = empty0 | (empty1 << 1) | (empty2 << 2) | (empty3 << 3);
				//printf("Flag: %llx\n", flag);

			} while (flag == 0 && output < outputEnd);

			this->loadFrom0 = loadFrom0;
			this->loadFrom1 = loadFrom1;
			this->loadFrom = loadFrom;
			this->opposite0 = opposite0;
			this->opposite1 = opposite1;
			this->opposite = opposite;

			this->l1Buf0 = l1Buf0;
			this->l1Buf1 = l1Buf1;
			this->l1endBuf0 = l1endBuf0;
			this->l1endBuf1 = l1endBuf1;

			this->output = output;
			this->outputEnd = outputEnd;

			this->exhaust0 = exhaust0;
			this->exhaust1 = exhaust1;
			this->exhaust2 = exhaust2;
			this->exhaust3 = exhaust3;
			this->exhaust4 = exhaust4;
			this->exhaust5 = exhaust5;
			this->exhaust = exhaust;

			this->a0 = a0;
			this->a1 = a1;
			this->a2 = a2;
			this->a3 = a3;
			this->a4 = a4;
			this->a5 = a5;

			return flag;
		}

		ui merge_internal_to_root_unaligned(ui* prior_unalign_items) {
			/*printf("Internal to root merging ... \n");
			this->print_node();*/

			constexpr ui ITEMS_PER_REG = sizeof(Reg) / sizeof(Item);
			Item* loadFrom0 = this->loadFrom0;
			Item* loadFrom1 = this->loadFrom1;
			Item* loadFrom = this->loadFrom;
			Item* opposite0 = this->opposite0;
			Item* opposite1 = this->opposite1;
			Item* opposite = this->opposite;
			Item* l1Buf0 = this->l1Buf0;
			Item* l1Buf1 = this->l1Buf1;
			Item* l1endBuf0 = this->l1endBuf0;
			Item* l1endBuf1 = this->l1endBuf1;
			Item* output = this->output;
			Item* outputEnd = this->outputEnd;
			Reg a0 = this->a0;
			Reg a1 = this->a1;
			Reg a2 = this->a2;
			Reg a3 = this->a3;
			Reg a4 = this->a4;
			Reg a5 = this->a5;
			Item* leafEnd0 = this->leafEnd0;
			Item* leafEnd1 = this->leafEnd1;
			Item* leafEnd2 = this->leafEnd2;
			Item* leafEnd3 = this->leafEnd3;

			ui exhaust0 = this->exhaust0;
			ui exhaust1 = this->exhaust1;
			ui exhaust2 = this->exhaust2;
			ui exhaust3 = this->exhaust3;
			ui exhaust4 = this->exhaust4;
			ui exhaust5 = this->exhaust5;
			ui exhaust = this->exhaust;

			// merge

			ui flag = 0;
			// skip over prior partition keys
			while (*prior_unalign_items > 0 && flag == 0) {
				// refill empty child
				if (exhaust4 < 2 && loadFrom == l1endBuf0) {
					l1endBuf0 = l1endBuf0_back;
					l1endBuf0 = origami_merge_tree::merge_internal_to_internal(&loadFrom0, &opposite0, a0, a1, l1Buf0, leafEnd0, leafEnd1, &l1endBuf0, exhaust0, exhaust1, exhaust4);
					loadFrom = l1Buf0;
				}
				else if (exhaust5 < 2 && loadFrom == l1endBuf1) {
					l1endBuf1 = l1endBuf1_back;
					l1endBuf1 = origami_merge_tree::merge_internal_to_internal(&loadFrom1, &opposite1, a2, a3, l1Buf1, leafEnd2, leafEnd3, &l1endBuf1, exhaust2, exhaust3, exhaust5);
					loadFrom = l1Buf1;
				}
				bool first = *(loadFrom) < *(opposite);
				Item* tmp0 = first ? loadFrom : opposite;
				opposite = first ? opposite : loadFrom;
				loadFrom = tmp0;

				Item* o = output;
				merge_root_unaligned_skip_prior(&loadFrom, &opposite, a5, l1endBuf0, l1endBuf1, exhaust4, exhaust5, prior_unalign_items);

				// check if any leaf node is empty
				ui empty0 = (loadFrom0 == leafEnd0);
				ui empty1 = (loadFrom0 == leafEnd1);
				ui empty2 = (loadFrom1 == leafEnd2);
				ui empty3 = (loadFrom1 == leafEnd3);
				flag = empty0 | (empty1 << 1) | (empty2 << 2) | (empty3 << 3);

			}

			// previous parition keys skipped
			do {
				// refill empty child
				if (exhaust4 < 2 && loadFrom == l1endBuf0) {
					l1endBuf0 = l1endBuf0_back;
					l1endBuf0 = origami_merge_tree::merge_internal_to_internal(&loadFrom0, &opposite0, a0, a1, l1Buf0, leafEnd0, leafEnd1, &l1endBuf0, exhaust0, exhaust1, exhaust4);
					loadFrom = l1Buf0;
				}
				else if (exhaust5 < 2 && loadFrom == l1endBuf1) {
					l1endBuf1 = l1endBuf1_back;
					l1endBuf1 = origami_merge_tree::merge_internal_to_internal(&loadFrom1, &opposite1, a2, a3, l1Buf1, leafEnd2, leafEnd3, &l1endBuf1, exhaust2, exhaust3, exhaust5);
					loadFrom = l1Buf1;
				}
				bool first = *(loadFrom) < *(opposite);
				Item* tmp0 = first ? loadFrom : opposite;
				opposite = first ? opposite : loadFrom;
				loadFrom = tmp0;

				// from L1 to root

				Item* o = output;
				merge_root_unaligned<Reg, Item, external>(&loadFrom, &opposite, a5, &output, l1endBuf0, l1endBuf1, exhaust4, exhaust5, &outputEnd);

				// check if any leaf node is empty
				ui empty0 = (loadFrom0 == leafEnd0);// && (exhaust0 < 2);
				ui empty1 = (loadFrom0 == leafEnd1);// && (exhaust1 < 2);
				ui empty2 = (loadFrom1 == leafEnd2);// && (exhaust2 < 2);
				ui empty3 = (loadFrom1 == leafEnd3);// && (exhaust3 < 2);
				flag = empty0 | (empty1 << 1) | (empty2 << 2) | (empty3 << 3);

			} while (flag == 0 && output < outputEnd);

			this->loadFrom0 = loadFrom0;
			this->loadFrom1 = loadFrom1;
			this->loadFrom = loadFrom;
			this->opposite0 = opposite0;
			this->opposite1 = opposite1;
			this->opposite = opposite;

			this->l1Buf0 = l1Buf0;
			this->l1Buf1 = l1Buf1;
			this->l1endBuf0 = l1endBuf0;
			this->l1endBuf1 = l1endBuf1;

			this->output = output;
			this->outputEnd = outputEnd;

			this->exhaust0 = exhaust0;
			this->exhaust1 = exhaust1;
			this->exhaust2 = exhaust2;
			this->exhaust3 = exhaust3;
			this->exhaust4 = exhaust4;
			this->exhaust5 = exhaust5;
			this->exhaust = exhaust;

			this->a0 = a0;
			this->a1 = a1;
			this->a2 = a2;
			this->a3 = a3;
			this->a4 = a4;
			this->a5 = a5;

			/*printf("Internal to root after merge ... \n");
			this->print_node();*/

			return flag;
		}

		void initialize(Item* _leafBuf0, Item* _leafBuf1, Item* _leafBuf2, Item* _leafBuf3,
			Item* _leafEnd0, Item* _leafEnd1, Item* _leafEnd2, Item* _leafEnd3,
			Item* _l1Buf0, Item* _l1Buf1, Item* _l1endBuf0, Item* _l1endBuf1,
			Item* _output, Item* _outputEnd) {

			loadFrom0 = _leafBuf0;	opposite0 = _leafBuf1;  loadFrom1 = _leafBuf2;  opposite1 = _leafBuf3;
			leafEnd0 = _leafEnd0;	leafEnd1 = _leafEnd1; 	leafEnd2 = _leafEnd2; 	leafEnd3 = _leafEnd3;

			l1Buf0 = _l1Buf0;	l1Buf1 = _l1Buf1; 	l1endBuf0 = _l1endBuf0; l1endBuf1 = _l1endBuf1;
			l1endBuf0_back = _l1endBuf0; l1endBuf1_back = _l1endBuf1;

			loadFrom = l1Buf0; opposite = l1Buf1;

			output = _output; outputEnd = _outputEnd; outputEnd_back = _outputEnd;

			exhaust0 = exhaust1 = exhaust2 = exhaust3 = exhaust4 = exhaust5 = exhaust = 0;

#ifdef MTREE_DBG_PRINT
			this->print_node();
#endif
		}
	};


	template <typename Reg, typename Item, bool external = false>
	class MergeTree {
	public:
		Merge4WayNode<Reg, Item, external>** nodes = nullptr;
		Item* bufptrs[MTREE_MAX_LEVEL * MTREE_MAX_WAY], * bufptrsEnd[MTREE_MAX_LEVEL * MTREE_MAX_WAY];
		ui LEVELS, LEAF_LEVEL, ROOT_LEVEL = 0;
		ui WAY, _WAY, LEVELS_4WAY, NODES;					// _WAY: original merge-way; WAY: rounded up merge-way
		ui prior_unalign_items = 0, post_unalign_items = 0;
		Item* buf, * ffs;
		ui buf_n, l2_buf_n;

		FORCEINLINE virtual void merge(Item** _X, Item** _endX, Item* output, Item* outputEnd = nullptr) = 0;

		// note: need to pass in nodes because it can be either from the left or right for the Odd tree
		void RefillNode(Merge4WayNode<Reg, Item, external>** nodes, ui node_idx, ui level, ui LEVELS_4WAY, ui leaf_base = 0, ui buff_base = 0) {
			//printf("Refill node %d\n", node_idx);
			//refill++;

			if (level == (LEVELS_4WAY - 1)) {		// leaf level
				ui buf_idx = buff_base + ((node_idx - leaf_base) << 2);
				//printf("Node idx: %lu, Buff base: %lu, Buf idx: %lu, Leaf base: %lu\n", node_idx, buff_base, buf_idx, leaf_base);
				nodes[node_idx]->merge_leaf_to_internal(buf_idx);
				return;
			}

			ui empty = nodes[node_idx]->merge_internal_to_internal();

			Item* _loadFrom0 = nodes[node_idx]->loadFrom0;
			Item* _loadFrom1 = nodes[node_idx]->loadFrom1;
			Item* _opposite0 = nodes[node_idx]->opposite0;
			Item* _opposite1 = nodes[node_idx]->opposite1;

			ui c_idx_base = (node_idx << 2) + 1;
			ui j = 0;
			while (j < 2) {
				ui c_idx = c_idx_base + j;
				if (((empty >> j) & 1) && (nodes[c_idx]->exhaust < 2)) {
					RefillNode(nodes, c_idx, level + 1, LEVELS_4WAY, leaf_base, buff_base);
					_loadFrom0 = nodes[c_idx]->output;

					bool first = *_loadFrom0 < *_opposite0;
					Item* tmp0 = first ? _loadFrom0 : _opposite0;
					_opposite0 = first ? _opposite0 : _loadFrom0;
					_loadFrom0 = tmp0;
				}
				++j;
			}
			while (j < 4) {
				ui c_idx = c_idx_base + j;
				if (((empty >> j) & 1) && (nodes[c_idx]->exhaust < 2)) {
					RefillNode(nodes, c_idx, level + 1, LEVELS_4WAY, leaf_base, buff_base);
					_loadFrom1 = nodes[c_idx]->output;

					bool first = *_loadFrom1 < *_opposite1;
					Item* tmp0 = first ? _loadFrom1 : _opposite1;
					_opposite1 = first ? _opposite1 : _loadFrom1;
					_loadFrom1 = tmp0;
				}
				++j;
			}
			nodes[node_idx]->exhaust0 = nodes[c_idx_base]->exhaust;			nodes[node_idx]->leafEnd0 = nodes[c_idx_base]->outputEnd;
			nodes[node_idx]->exhaust1 = nodes[c_idx_base + 1]->exhaust;		nodes[node_idx]->leafEnd1 = nodes[c_idx_base + 1]->outputEnd;
			nodes[node_idx]->exhaust2 = nodes[c_idx_base + 2]->exhaust;		nodes[node_idx]->leafEnd2 = nodes[c_idx_base + 2]->outputEnd;
			nodes[node_idx]->exhaust3 = nodes[c_idx_base + 3]->exhaust;		nodes[node_idx]->leafEnd3 = nodes[c_idx_base + 3]->outputEnd;

			nodes[node_idx]->loadFrom0 = _loadFrom0;
			nodes[node_idx]->loadFrom1 = _loadFrom1;
			nodes[node_idx]->opposite0 = _opposite0;
			nodes[node_idx]->opposite1 = _opposite1;
		}
	};

	template <typename Reg, typename Item, bool external = false>
	class MergeTreeEven : public MergeTree<Reg, Item, external> {
	public:
		MergeTreeEven(ui _WAY, Item* _buf, ui buf_n, ui l2_buf_n, void (*f)(int, char**, char**) = nullptr) {
			process_buffer = f;
			ui WAY_POW = (ui)(log2(_WAY));
			WAY_POW += ((1 << WAY_POW) != _WAY);
			ui WAY = 1 << WAY_POW;
			this->WAY = WAY;
			this->_WAY = _WAY;
			this->LEVELS = (ui)(log2(WAY)) + 1;
			this->LEAF_LEVEL = this->LEVELS - 1;
			this->LEVELS_4WAY = this->LEVELS >> 1;
			this->NODES = 0;
			FOR(i, this->LEVELS_4WAY, 1) this->NODES += pow(4, i);
			this->nodes = new Merge4WayNode<Reg, Item, external> *[this->NODES];
			FOR(i, this->NODES, 1) this->nodes[i] = new Merge4WayNode<Reg, Item, external>();

			// init interim buffer start and end pointers
			this->buf = _buf;
			Item* p = this->buf;
			FOR_INIT(i, 1, this->LEAF_LEVEL, 1) {
				ui nodes_at_level = 1U << i;
				ui level_offset = i * WAY;
				Item** _bufptr = this->bufptrs + level_offset;
				Item** _bufptrEnd = this->bufptrsEnd + level_offset;
				// for large l2 buffers
				if ((i & 1) == 0) {	// level 2, 4 and so on
					FOR(j, nodes_at_level, 1) {
						*_bufptr = p; _bufptr++;
						*_bufptrEnd = p + l2_buf_n; _bufptrEnd++;
						p += l2_buf_n + (64 / sizeof(Item));
					}
				}
				else {
					FOR(j, nodes_at_level, 1) {
						*_bufptr = p; _bufptr++;
						*_bufptrEnd = p + buf_n; _bufptrEnd++;
						p += buf_n + (64 / sizeof(Item));
					}
				}
			}

			ui64 ffs_size_bytes = buf_n * sizeof(Item);		// *WAY; // sizeof(Reg) * (WAY - _WAY);
			this->ffs = new Item[ffs_size_bytes];
			memset(this->ffs, 0xFF, ffs_size_bytes);
			memset(this->buf, 0, (p - this->buf) * sizeof(Item));
			this->buf_n = buf_n;
			this->l2_buf_n = l2_buf_n;
		}

		~MergeTreeEven() {
			FOR(i, this->NODES, 1) delete this->nodes[i];
			delete[] this->nodes;
			delete[] this->ffs;
		}

		FORCEINLINE void merge(Item** _X, Item** _endX, Item* C, Item* Cend = nullptr) override {
			ui WAY = this->WAY;
			ui _WAY = this->_WAY;
			ui64 n = 0;
			ui buf_n = this->buf_n;
			ui l2_buf_n = this->l2_buf_n;
			// debug verify correctness of partitioning
			/*Item* O = C;
			FOR(i, WAY, 1) {
				memcpy(O, _X[i], (_endX[i] - _X[i]) * sizeof(Item));
				O += (_endX[i] - _X[i]);
			}
			std::sort(C, C + n);
			return; */

			// debug: check if lists sorted
			/*FOR(i, WAY, 1) {
				printf("Checking stream %u w/ %llu items ... ", i, (_endX[i] - _X[i]));
				SortCorrectnessChecker<Item>(_X[i], (_endX[i] - _X[i]));
				printf("done\n");
			}
			return;*/


			constexpr ui ITEMS_PER_REG = sizeof(Reg) / sizeof(Item);
			this->prior_unalign_items = 0;
			this->post_unalign_items = 0;
			// create local vars
			Merge4WayNode<Reg, Item, external>** nodes = this->nodes;
			Item** bufptrs = this->bufptrs, ** bufptrsEnd = this->bufptrsEnd;
			ui LEAF_LEVEL = this->LEAF_LEVEL;

			ui64 n_temp = 0;
			C = (Item*)(origami_utils::align<Reg, MTREE_NREG>((char*)C));

			// initialization begin 
			// 1. init leaf pointers

			ui leaf_level_offset = LEAF_LEVEL * WAY;
			Item** _bufptr = bufptrs + leaf_level_offset;
			Item** _bufptrEnd = bufptrsEnd + leaf_level_offset;
			FOR(i, _WAY, 1) {
				//printf("[%llX %llX]\n", _X[i], _endX[i]);
				Item* x = _X[i];
				Item* aligned_x = (Item*)origami_utils::align<Reg, MTREE_NREG>((char*)x);
				Item* endx = _endX[i];
				n += endx - x;
				bool endx_aligned = origami_utils::aligned<Reg, MTREE_NREG>((char*)endx);
				Item* endx2 = (Item*)origami_utils::align<Reg, MTREE_NREG>((char*)endx) + ITEMS_PER_REG;
				Item* aligned_endx = endx_aligned ? endx : endx2;

				//printf("Prior unalign: %lu, Post unalign: %lu\n", x - aligned_x, aligned_endx - endx);
				//printf("[%llX %llX], [%llX %llX]\n", x, aligned_x, endx, aligned_endx);
				this->prior_unalign_items += x - aligned_x;
				this->post_unalign_items += aligned_endx - endx;

				*_bufptr = aligned_x;
				*_bufptrEnd = aligned_endx;
				n_temp += (endx - x);
				_bufptr++; _bufptrEnd++;
			}
			Item* t = this->ffs;
			FOR_INIT(i, _WAY, WAY, 1) {
				//printf("%llX %llX\n", t, t + ITEMS_PER_REG);
				*_bufptr = t;
				*_bufptrEnd = t + buf_n; // ITEMS_PER_REG;
				_bufptr++; _bufptrEnd++;
				//t += ITEMS_PER_REG;
			}

			//n = n_temp;

			/*printf("Tot prior unalign items: %lu, post unalign items: %lu\n", prior_unalign_items, post_unalign_items);
			printf("N: %lu\n", n);*/



			// 2. init root ptr as output buffer
			Item* output = C, * outputEnd;
			if constexpr (external) outputEnd = Cend;
			else outputEnd = C + n;
			bufptrs[0] = output;
			bufptrsEnd[0] = outputEnd;

			ui idx_offset = 0; // NODES - 1;
			//for (int curr_level = LEAF_LEVEL - 2; curr_level >= 0; curr_level -= 2) {
			FOR(curr_level, LEAF_LEVEL, 2) {
				ui nodes_at_level = 1 << curr_level;
				FOR(i, nodes_at_level, 1) {
					ui node_idx = curr_level * WAY + i;
					ui lidx = (curr_level + 1) * WAY + (i << 1); ui lidx2 = (curr_level + 2) * WAY + (i << 2);
					nodes[idx_offset]->initialize(bufptrs[lidx2], bufptrs[lidx2 + 1], bufptrs[lidx2 + 2], bufptrs[lidx2 + 3],
						bufptrsEnd[lidx2], bufptrsEnd[lidx2 + 1], bufptrsEnd[lidx2 + 2], bufptrsEnd[lidx2 + 3],
						bufptrs[lidx], bufptrs[lidx + 1], bufptrsEnd[lidx], bufptrsEnd[lidx + 1],
						bufptrs[node_idx], bufptrsEnd[node_idx]);
					++idx_offset;
				}
			}

			// Initialization
			if (this->LEVELS_4WAY == 1)
				nodes[0]->merge_leaf_to_root_init(0);
			else if (this->LEVELS_4WAY == 2) {
				FOR_INIT(i, 1, 5, 1) {
					nodes[i]->merge_leaf_to_internal_init((i - 1) << 2);
					nodes[i]->merge_leaf_to_internal((i - 1) << 2);
					//printf("Internal node %lu contains: %llu; Exhaust: %lu\n", i, nodes[i]->outputEnd - nodes[i]->output, nodes[i]->exhaust);
				}
				// to handle the case when initilization empties out these nodes ** NOTE: need to add this to the following else condition
				nodes[0]->exhaust0 = nodes[1]->exhaust;	nodes[0]->leafEnd0 = nodes[1]->outputEnd;
				nodes[0]->exhaust1 = nodes[2]->exhaust;	nodes[0]->leafEnd1 = nodes[2]->outputEnd;
				nodes[0]->exhaust2 = nodes[3]->exhaust;	nodes[0]->leafEnd2 = nodes[3]->outputEnd;
				nodes[0]->exhaust3 = nodes[4]->exhaust;	nodes[0]->leafEnd3 = nodes[4]->outputEnd;
				nodes[0]->merge_internal_to_root_init();
			}
			else {
				ui nodes_last_level = 1LU << ((this->LEVELS_4WAY - 1) << 1);
				ui idx = this->NODES - 1;
				ui leaf_base = this->NODES - (WAY >> 2);
				FOR(i, nodes_last_level, 1) {
					ui buf_idx = (idx - leaf_base) << 2;
					nodes[idx]->merge_leaf_to_internal_init(buf_idx);
					nodes[idx]->merge_leaf_to_internal(buf_idx);
					ui pidx = (idx - 1) >> 2;
					ui exhaust = nodes[idx]->exhaust;
					Item* _outputEnd = nodes[idx]->outputEnd;
					switch (idx & 3) {
					case 1:
						nodes[pidx]->exhaust0 = exhaust;
						nodes[pidx]->leafEnd0 = _outputEnd;
						break;
					case 2:
						nodes[pidx]->exhaust1 = exhaust;
						nodes[pidx]->leafEnd1 = _outputEnd;
						break;
					case 3:
						nodes[pidx]->exhaust2 = exhaust;
						nodes[pidx]->leafEnd2 = _outputEnd;
						break;
					case 0:	// last node
						nodes[pidx]->exhaust3 = exhaust;
						nodes[pidx]->leafEnd3 = _outputEnd;
						break;
					}
					--idx;
				}
				for (int i = this->LEVELS_4WAY - 2; i > 0; --i) {
					ui nodes_level = 1LU << (i << 1);
					FOR(j, nodes_level, 1) {
						nodes[idx]->merge_internal_to_internal_init();
						nodes[idx]->merge_internal_to_internal();
						ui pidx = (idx - 1) >> 2;
						ui exhaust = nodes[idx]->exhaust;
						Item* _outputEnd = nodes[idx]->outputEnd;
						switch (idx & 3) {
						case 1:
							nodes[pidx]->exhaust0 = exhaust;
							nodes[pidx]->leafEnd0 = _outputEnd;
							break;
						case 2:
							nodes[pidx]->exhaust1 = exhaust;
							nodes[pidx]->leafEnd1 = _outputEnd;
							break;
						case 3:
							nodes[pidx]->exhaust2 = exhaust;
							nodes[pidx]->leafEnd2 = _outputEnd;
							break;
						case 0:	// last node
							nodes[pidx]->exhaust3 = exhaust;
							nodes[pidx]->leafEnd3 = _outputEnd;
							break;
						}
						--idx;
					}
				}
				nodes[0]->merge_internal_to_root_init();
			}


			// Merge 
			if (this->LEVELS_4WAY == 1) {
				nodes[0]->merge_leaf_to_root_unaligned(&this->prior_unalign_items);
				return;
			}
			ui empty = 0;
			ui tot = 0;
			//ui lcnt = 0;
			while (nodes[0]->output < outputEnd) {
				empty = nodes[0]->merge_internal_to_root_unaligned(&this->prior_unalign_items);

				Item* loadFrom0 = nodes[0]->loadFrom0;
				Item* loadFrom1 = nodes[0]->loadFrom1;
				Item* opposite0 = nodes[0]->opposite0;
				Item* opposite1 = nodes[0]->opposite1;

				ui i = 0;
				while (i < 2) {
					ui cidx = i + 1;
					if (((empty >> i) & 1) && (nodes[cidx]->exhaust < 2)) {
						this->RefillNode(nodes, cidx, 1, this->LEVELS_4WAY, this->NODES - (WAY >> 2), 0);
						loadFrom0 = nodes[cidx]->output;

						bool first = *loadFrom0 < *opposite0;
						Item* tmp0 = first ? loadFrom0 : opposite0;
						opposite0 = first ? opposite0 : loadFrom0;
						loadFrom0 = tmp0;
					}
					++i;
				}
				while (i < 4) {
					ui cidx = i + 1;
					if (((empty >> i) & 1) && (nodes[cidx]->exhaust < 2)) {
						this->RefillNode(nodes, cidx, 1, this->LEVELS_4WAY, this->NODES - (WAY >> 2), 0);
						loadFrom1 = nodes[cidx]->output;

						bool first = *loadFrom1 < *opposite1;
						Item* tmp0 = first ? loadFrom1 : opposite1;
						opposite1 = first ? opposite1 : loadFrom1;
						loadFrom1 = tmp0;
					}
					++i;
				}

				nodes[0]->exhaust0 = nodes[1]->exhaust;	nodes[0]->leafEnd0 = nodes[1]->outputEnd;
				nodes[0]->exhaust1 = nodes[2]->exhaust;	nodes[0]->leafEnd1 = nodes[2]->outputEnd;
				nodes[0]->exhaust2 = nodes[3]->exhaust;	nodes[0]->leafEnd2 = nodes[3]->outputEnd;
				nodes[0]->exhaust3 = nodes[4]->exhaust;	nodes[0]->leafEnd3 = nodes[4]->outputEnd;

				nodes[0]->loadFrom0 = loadFrom0;
				nodes[0]->loadFrom1 = loadFrom1;
				nodes[0]->opposite0 = opposite0;
				nodes[0]->opposite1 = opposite1;
			}
			//printf("Refill: %llu\n", refill);
		}
	};

	// for merge-tree that is not power of 4-way (32-way, 128-way etc.)
	template <typename Reg, typename Item, bool external = false>
	class MergeTreeOdd : public MergeTree<Reg, Item, external> {
	public:
		Merge4WayNode<Reg, Item, external>** nodes_right = nullptr;
		Item* bufptrs2[MTREE_MAX_LEVEL * MTREE_MAX_WAY], * bufptrsEnd2[MTREE_MAX_LEVEL * MTREE_MAX_WAY];

		MergeTreeOdd(ui _WAY, Item* _buf, ui buf_n, ui l2_buf_n, void (*f)(int, char**, char**) = nullptr) {
			process_buffer = f;
			ui WAY_POW = (ui)(log2(_WAY));
			WAY_POW += ((1 << WAY_POW) != _WAY);
			ui WAY = 1 << WAY_POW;
			this->WAY = WAY;
			this->_WAY = _WAY;
			Merge4WayNode<Reg, Item, external>** nodes_left = this->nodes;
			Item** bufptrs1 = this->bufptrs, ** bufptrsEnd1 = this->bufptrsEnd;
			const ui HALF_WAY = WAY >> 1;
			this->LEVELS = (ui)(log2(HALF_WAY)) + 1;
			this->LEAF_LEVEL = this->LEVELS - 1;
			this->LEVELS_4WAY = this->LEVELS >> 1;
			this->NODES = 0;
			FOR(i, this->LEVELS_4WAY, 1) this->NODES += pow(4, i);
			nodes_left = new Merge4WayNode<Reg, Item, external> *[this->NODES];
			nodes_right = new Merge4WayNode<Reg, Item, external> *[this->NODES];
			FOR(i, this->NODES, 1) {
				nodes_left[i] = new Merge4WayNode<Reg, Item, external>();
				nodes_right[i] = new Merge4WayNode<Reg, Item, external>();
			}

			// init interim buffer start and end pointers
			this->buf = _buf;
			Item* p1 = this->buf;
			FOR_INIT(i, 1, this->LEAF_LEVEL, 1) {
				ui nodes_at_level = 1U << i;
				ui level_offset = i * HALF_WAY;
				Item** _bufptr1 = bufptrs1 + level_offset;
				Item** _bufptrEnd1 = bufptrsEnd1 + level_offset;
				// for large l2 buffers
				if ((i & 1) == 0) {	// level 2, 4 and so on
					FOR(j, nodes_at_level, 1) {
						//printf("[%llX, %llX], [%llX, %llX]\n", p1, p1 + l2_buf_n, p2, p2 + l2_buf_n);
						*_bufptr1 = p1; _bufptr1++;
						*_bufptrEnd1 = p1 + l2_buf_n; _bufptrEnd1++;
						p1 += l2_buf_n + (64 / sizeof(Item));
					}
				}
				else {
					FOR(j, nodes_at_level, 1) {
						//printf("[%llX, %llX], [%llX, %llX]\n", p1, p1 + buf_n, p2, p2 + buf_n);
						*_bufptr1 = p1; _bufptr1++;
						*_bufptrEnd1 = p1 + buf_n; _bufptrEnd1++;
						p1 += buf_n + (64 / sizeof(Item));
					}
				}
			}
			bufptrs1[0] = p1; bufptrsEnd1[0] = p1 + l2_buf_n;		// output buffer for the roots
			p1 += l2_buf_n + (64 / sizeof(Item));

			FOR_INIT(i, 1, this->LEAF_LEVEL, 1) {
				ui nodes_at_level = 1U << i;
				ui level_offset = i * HALF_WAY;
				Item** _bufptr2 = bufptrs2 + level_offset;
				Item** _bufptrEnd2 = bufptrsEnd2 + level_offset;
				// for large l2 buffers
				if ((i & 1) == 0) {	// level 2, 4 and so on
					FOR(j, nodes_at_level, 1) {
						//printf("[%llX, %llX], [%llX, %llX]\n", p1, p1 + l2_buf_n, p2, p2 + l2_buf_n);
						*_bufptr2 = p1; _bufptr2++;
						*_bufptrEnd2 = p1 + l2_buf_n; _bufptrEnd2++;
						p1 += l2_buf_n + (64 / sizeof(Item));
					}
				}
				else {
					FOR(j, nodes_at_level, 1) {
						//printf("[%llX, %llX], [%llX, %llX]\n", p1, p1 + buf_n, p2, p2 + buf_n);
						*_bufptr2 = p1; _bufptr2++;
						*_bufptrEnd2 = p1 + buf_n; _bufptrEnd2++;
						p1 += buf_n + (64 / sizeof(Item));
					}
				}
			}
			bufptrs2[0] = p1; bufptrsEnd2[0] = p1 + l2_buf_n;
			p1 += l2_buf_n;

			this->nodes = nodes_left;

			ui64 ffs_size_bytes = buf_n * sizeof(Item);
			this->ffs = new Item[ffs_size_bytes];
			memset(this->ffs, 0xFF, ffs_size_bytes);
			memset(this->buf, 0, (p1 - this->buf) * sizeof(Item));

			this->buf_n = buf_n;
			this->l2_buf_n = l2_buf_n;
		}

		~MergeTreeOdd() {
			Merge4WayNode<Reg, Item, external>** nodes_left = this->nodes;
			FOR(i, this->NODES, 1) {
				delete nodes_left[i];
				delete nodes_right[i];
			}
			delete[] nodes_left;
			delete[] nodes_right;
		}

		FORCEINLINE void merge(Item** _X, Item** _endX, Item* C, Item* Cend = nullptr) override {
			ui WAY = this->WAY;
			ui _WAY = this->_WAY;
			ui64 n = 0;
			ui buf_n = this->buf_n;
			ui l2_buf_n = this->l2_buf_n;
			// debug: check if lists sorted
			/*FOR(i, WAY, 1) {
				printf("Checking stream %u w/ %llu items ... ", i, (_endX[i] - _X[i]));
				SortCorrectnessChecker<Item>(_X[i], (_endX[i] - _X[i]));
				printf("done\n");
			}
			return;*/
			constexpr ui ITEMS_PER_REG = sizeof(Reg) / sizeof(Item);
			constexpr ui INC = ITEMS_PER_REG * MTREE_NREG;
			this->prior_unalign_items = 0;
			this->post_unalign_items = 0;
			//printf("Merging: [%llX %llX] to [%llX %llX], Tot: %llu\n", A, A + chunk * WAY, C, C + chunk * WAY, chunk * WAY);
				// create local vars
			Merge4WayNode<Reg, Item, external>** nodes_left = this->nodes;
			Item** bufptrs1 = this->bufptrs, ** bufptrsEnd1 = this->bufptrsEnd;
			Merge4WayNode<Reg, Item, external>** nodes_right = this->nodes_right;
			Item** bufptrs2 = this->bufptrs2, ** bufptrsEnd2 = this->bufptrsEnd2;
			ui LEAF_LEVEL = this->LEAF_LEVEL;

			ui64 n_temp = 0;
			C = (Item*)(origami_utils::align<Reg, MTREE_NREG>((char*)C));

			// initialization begin 
			// 1. init leaf pointers
			const ui HALF_WAY = WAY >> 1;
			ui leaf_level_offset = LEAF_LEVEL * HALF_WAY;
			Item** _bufptr1 = bufptrs1 + leaf_level_offset;
			Item** _bufptr2 = bufptrs2 + leaf_level_offset;
			Item** _bufptrEnd1 = bufptrsEnd1 + leaf_level_offset;
			Item** _bufptrEnd2 = bufptrsEnd2 + leaf_level_offset;
			FOR(i, HALF_WAY, 1) {
				Item* x = _X[i];
				Item* aligned_x = (Item*)origami_utils::align<Reg, MTREE_NREG>((char*)x);
				Item* endx = _endX[i];
				n += endx - x;
				bool endx_aligned = origami_utils::aligned<Reg, MTREE_NREG>((char*)endx);
				Item* endx2 = (Item*)origami_utils::align<Reg, MTREE_NREG>((char*)endx) + INC;
				Item* aligned_endx = endx_aligned ? endx : endx2;

				this->prior_unalign_items += x - aligned_x;
				this->post_unalign_items += aligned_endx - endx;

				*_bufptr1 = aligned_x;
				*_bufptrEnd1 = aligned_endx;
				n_temp += (endx - x);
				_bufptr1++; _bufptrEnd1++;
			}
			FOR_INIT(i, HALF_WAY, _WAY, 1) {
				/*Item* x = _X[i];
				Item* endx = (Item*)(origami_utils::align<Reg>(_endX[i]));
				bool origami_utils::aligned = origami_utils::aligned<Reg>(x);
				Item* y = (Item*)origami_utils::align<Reg>(x) + INC;
				x = origami_utils::aligned ? x : y;
				*_bufptr2 = x;
				*_bufptrEnd2 = endx;
				n_temp += (endx - x);
				_bufptr2++; _bufptrEnd2++;*/

				Item* x = _X[i];
				Item* aligned_x = (Item*)origami_utils::align<Reg, MTREE_NREG>((char*)x);
				Item* endx = _endX[i];
				n += endx - x;
				bool endx_aligned = origami_utils::aligned<Reg, MTREE_NREG>((char*)endx);
				Item* endx2 = (Item*)origami_utils::align<Reg, MTREE_NREG>((char*)endx) + INC;
				Item* aligned_endx = endx_aligned ? endx : endx2;

				//printf("Prior unalign: %lu, Post unalign: %lu\n", x - aligned_x, aligned_endx - endx);
				this->prior_unalign_items += x - aligned_x;
				this->post_unalign_items += aligned_endx - endx;

				*_bufptr2 = aligned_x;
				*_bufptrEnd2 = aligned_endx;
				n_temp += (endx - x);
				_bufptr2++; _bufptrEnd2++;
			}
			Item* t = this->ffs;
			FOR_INIT(i, _WAY, WAY, 1) {
				*_bufptr2 = t;
				*_bufptrEnd2 = t + buf_n;
				_bufptr2++; _bufptrEnd2++;
				//t += ITEMS_PER_REG;
			}


			// 2. init root ptr as output buffer
			Item* output = C, * outputEnd;
			if constexpr (external) outputEnd = Cend;
			else outputEnd = C + n;

			ui idx_offset = 0; // NODES - 1;
			FOR(curr_level, LEAF_LEVEL, 2) {
				ui nodes_at_level = 1 << curr_level;
				FOR(i, nodes_at_level, 1) {
					ui node_idx = curr_level * HALF_WAY + i;
					ui lidx = (curr_level + 1) * HALF_WAY + (i << 1); ui lidx2 = (curr_level + 2) * HALF_WAY + (i << 2);
					nodes_left[idx_offset]->initialize(bufptrs1[lidx2], bufptrs1[lidx2 + 1], bufptrs1[lidx2 + 2], bufptrs1[lidx2 + 3],
						bufptrsEnd1[lidx2], bufptrsEnd1[lidx2 + 1], bufptrsEnd1[lidx2 + 2], bufptrsEnd1[lidx2 + 3],
						bufptrs1[lidx], bufptrs1[lidx + 1], bufptrsEnd1[lidx], bufptrsEnd1[lidx + 1],
						bufptrs1[node_idx], bufptrsEnd1[node_idx]);
					nodes_right[idx_offset]->initialize(bufptrs2[lidx2], bufptrs2[lidx2 + 1], bufptrs2[lidx2 + 2], bufptrs2[lidx2 + 3],
						bufptrsEnd2[lidx2], bufptrsEnd2[lidx2 + 1], bufptrsEnd2[lidx2 + 2], bufptrsEnd2[lidx2 + 3],
						bufptrs2[lidx], bufptrs2[lidx + 1], bufptrsEnd2[lidx], bufptrsEnd2[lidx + 1],
						bufptrs2[node_idx], bufptrsEnd2[node_idx]);
					++idx_offset;
				}
			}

			// Initialization
			if (this->LEVELS_4WAY == 1) {
				nodes_left[0]->merge_leaf_to_internal_init(0);
				nodes_left[0]->merge_leaf_to_internal(0);
				nodes_right[0]->merge_leaf_to_internal_init(4);
				nodes_right[0]->merge_leaf_to_internal(4);
			}
			else if (this->LEVELS_4WAY == 2) {
				FOR_INIT(i, 1, 5, 1) {
					nodes_left[i]->merge_leaf_to_internal_init((i - 1) << 2);
					nodes_left[i]->merge_leaf_to_internal((i - 1) << 2);
					nodes_right[i]->merge_leaf_to_internal_init(HALF_WAY + ((i - 1) << 2));
					nodes_right[i]->merge_leaf_to_internal(HALF_WAY + ((i - 1) << 2));
					//printf("Internal node contains: %llu; Exhaust: %lu\n", nodes[i]->outputEnd - nodes[i]->output, nodes[i]->exhaust);
				}
				// to handle the case when initilization empties out these nodes ** NOTE: need to add this to the following else condition
				nodes_left[0]->exhaust0 = nodes_left[1]->exhaust;	nodes_left[0]->leafEnd0 = nodes_left[1]->outputEnd;
				nodes_left[0]->exhaust1 = nodes_left[2]->exhaust;	nodes_left[0]->leafEnd1 = nodes_left[2]->outputEnd;
				nodes_left[0]->exhaust2 = nodes_left[3]->exhaust;	nodes_left[0]->leafEnd2 = nodes_left[3]->outputEnd;
				nodes_left[0]->exhaust3 = nodes_left[4]->exhaust;	nodes_left[0]->leafEnd3 = nodes_left[4]->outputEnd;

				nodes_right[0]->exhaust0 = nodes_right[1]->exhaust;	nodes_right[0]->leafEnd0 = nodes_right[1]->outputEnd;
				nodes_right[0]->exhaust1 = nodes_right[2]->exhaust;	nodes_right[0]->leafEnd1 = nodes_right[2]->outputEnd;
				nodes_right[0]->exhaust2 = nodes_right[3]->exhaust;	nodes_right[0]->leafEnd2 = nodes_right[3]->outputEnd;
				nodes_right[0]->exhaust3 = nodes_right[4]->exhaust;	nodes_right[0]->leafEnd3 = nodes_right[4]->outputEnd;

				nodes_left[0]->merge_internal_to_internal_init();
				nodes_left[0]->merge_internal_to_internal();
				nodes_right[0]->merge_internal_to_internal_init();
				nodes_right[0]->merge_internal_to_internal();
			}
			else {
				ui nodes_last_level = 1LU << ((this->LEVELS_4WAY - 1) << 1);
				ui idx = this->NODES - 1;
				ui leaf_base = this->NODES - (HALF_WAY >> 2);
				FOR(i, nodes_last_level, 1) {
					ui buf_idx = (idx - leaf_base) << 2;
					nodes_left[idx]->merge_leaf_to_internal_init(buf_idx);
					nodes_left[idx]->merge_leaf_to_internal(buf_idx);
					nodes_right[idx]->merge_leaf_to_internal_init(HALF_WAY + buf_idx);
					nodes_right[idx]->merge_leaf_to_internal(HALF_WAY + buf_idx);

					if (idx > 0) {		// we have more than one node i.e. this node has some parent
						ui pidx = (idx - 1) >> 2;
						ui exhaust_left = nodes_left[idx]->exhaust;
						ui exhaust_right = nodes_right[idx]->exhaust;
						Item* _outputEnd_left = nodes_left[idx]->outputEnd;
						Item* _outputEnd_right = nodes_right[idx]->outputEnd;
						switch (idx & 3) {
						case 1:
							nodes_left[pidx]->exhaust0 = exhaust_left;
							nodes_right[pidx]->exhaust0 = exhaust_right;
							nodes_left[pidx]->leafEnd0 = _outputEnd_left;
							nodes_right[pidx]->leafEnd0 = _outputEnd_right;
							break;
						case 2:
							nodes_left[pidx]->exhaust1 = exhaust_left;
							nodes_right[pidx]->exhaust1 = exhaust_right;
							nodes_left[pidx]->leafEnd1 = _outputEnd_left;
							nodes_right[pidx]->leafEnd1 = _outputEnd_right;
							break;
						case 3:
							nodes_left[pidx]->exhaust2 = exhaust_left;
							nodes_right[pidx]->exhaust2 = exhaust_right;
							nodes_left[pidx]->leafEnd2 = _outputEnd_left;
							nodes_right[pidx]->leafEnd2 = _outputEnd_right;
							break;
						case 0:	// last node
							nodes_left[pidx]->exhaust3 = exhaust_left;
							nodes_right[pidx]->exhaust3 = exhaust_right;
							nodes_left[pidx]->leafEnd3 = _outputEnd_left;
							nodes_right[pidx]->leafEnd3 = _outputEnd_right;
							break;
						}
					}
					--idx;
				}
				for (int i = this->LEVELS_4WAY - 2; i >= 0; --i) {
					ui nodes_level = 1LU << (i << 1);
					FOR(j, nodes_level, 1) {
						nodes_left[idx]->merge_internal_to_internal_init();
						nodes_left[idx]->merge_internal_to_internal();
						nodes_right[idx]->merge_internal_to_internal_init();
						nodes_right[idx]->merge_internal_to_internal();
						if (idx > 0) {
							ui pidx = (idx - 1) >> 2;
							ui exhaust_left = nodes_left[idx]->exhaust;
							ui exhaust_right = nodes_right[idx]->exhaust;
							Item* _outputEnd_left = nodes_left[idx]->outputEnd;
							Item* _outputEnd_right = nodes_right[idx]->outputEnd;
							switch (idx & 3) {
							case 1:
								nodes_left[pidx]->exhaust0 = exhaust_left;
								nodes_right[pidx]->exhaust0 = exhaust_right;
								nodes_left[pidx]->leafEnd0 = _outputEnd_left;
								nodes_right[pidx]->leafEnd0 = _outputEnd_right;
								break;
							case 2:
								nodes_left[pidx]->exhaust1 = exhaust_left;
								nodes_right[pidx]->exhaust1 = exhaust_right;
								nodes_left[pidx]->leafEnd1 = _outputEnd_left;
								nodes_right[pidx]->leafEnd1 = _outputEnd_right;
								break;
							case 3:
								nodes_left[pidx]->exhaust2 = exhaust_left;
								nodes_right[pidx]->exhaust2 = exhaust_right;
								nodes_left[pidx]->leafEnd2 = _outputEnd_left;
								nodes_right[pidx]->leafEnd2 = _outputEnd_right;
								break;
							case 0:	// last node
								nodes_left[pidx]->exhaust3 = exhaust_left;
								nodes_right[pidx]->exhaust3 = exhaust_right;
								nodes_left[pidx]->leafEnd3 = _outputEnd_left;
								nodes_right[pidx]->leafEnd3 = _outputEnd_right;
								break;
							}
						}
						--idx;
					}
				}
			}

			Merge4WayNode<Reg, Item, external>* root_left = nodes_left[0];
			Merge4WayNode<Reg, Item, external>* root_right = nodes_right[0];
			Item* loadFrom = root_left->output;
			Item* opposite = root_right->output;
			Item* endA = root_left->outputEnd;
			Item* endB = root_right->outputEnd;

			Reg a1;
			origami_utils::load<Reg>(a1, (Reg*)opposite); opposite += INC;

			// skip prior_unalign_items keys
			while (1) {
				merge_root_unaligned_skip_prior(&loadFrom, &opposite, a1, endA, endB, root_left->exhaust, root_right->exhaust, &this->prior_unalign_items);
				if (this->prior_unalign_items == 0) break;

				if (loadFrom == endA && root_left->exhaust < 2) {
					this->RefillNode(nodes_left, 0, 0, this->LEVELS_4WAY, this->NODES - (HALF_WAY >> 2), 0);
					loadFrom = root_left->output;
					endA = root_left->outputEnd;
				}
				else if (loadFrom == endB && root_right->exhaust < 2) {
					this->RefillNode(nodes_right, 0, 0, this->LEVELS_4WAY, this->NODES - (HALF_WAY >> 2), HALF_WAY);
					loadFrom = root_right->output;
					endB = root_right->outputEnd;
				}
				bool first = *loadFrom < *opposite;
				Item* tmp = first ? loadFrom : opposite;
				opposite = first ? opposite : loadFrom;
				loadFrom = tmp;
			}

			while (1) {
				merge_root_unaligned<Reg, Item, external>(&loadFrom, &opposite, a1, &output, endA, endB, root_left->exhaust, root_right->exhaust, &outputEnd);

				if (output >= outputEnd) break;
				if (loadFrom == endA && root_left->exhaust < 2) {
					this->RefillNode(nodes_left, 0, 0, this->LEVELS_4WAY, this->NODES - (HALF_WAY >> 2), 0);
					loadFrom = root_left->output;
					endA = root_left->outputEnd;
				}
				else if (loadFrom == endB && root_right->exhaust < 2) {
					this->RefillNode(nodes_right, 0, 0, this->LEVELS_4WAY, this->NODES - (HALF_WAY >> 2), HALF_WAY);
					loadFrom = root_right->output;
					endB = root_right->outputEnd;
				}
				bool first = *loadFrom < *opposite;
				Item* tmp = first ? loadFrom : opposite;
				opposite = first ? opposite : loadFrom;
				loadFrom = tmp;
			}
		}
	};
#undef MTREE_DBG_PRINT
}

