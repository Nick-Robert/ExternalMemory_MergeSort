#include "MinHeap.h"

MinHeap::MinHeap(MinHeapNode a[], int size) : heap_size{ size }, harr{ a }
{
	int i = (heap_size - 1) / 2;
	while (i >= 0)
	{
		MinHeapify(i);
		i--;
	}
}


void MinHeap::MinHeapify(int i)
{
	int l = left(i);
	int r = right(i);
	int smallest = i;

	if (l < heap_size && harr[l].val < harr[i].val) {
		smallest = l;
	}

	if (r < heap_size && harr[r].val < harr[smallest].val) {
		smallest = r;
	}

	if (smallest != i) {
		swap(&harr[i], &harr[smallest]);
		MinHeapify(smallest);
	}
}


void swap(MinHeapNode* x, MinHeapNode* y)
{
	MinHeapNode temp = *x;
	*x = *y;
	*y = temp;
}
