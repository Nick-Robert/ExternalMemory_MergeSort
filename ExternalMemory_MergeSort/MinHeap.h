#ifndef MINHEAP_H
#define MINHEAP_H
#include <utility>

struct MinHeapNode
{
	unsigned int val;

	// says which chunk the index comes from
	int chunk_index;
	// where in the chunk is the current value from
	int val_index;
};

class MinHeap
{
	MinHeapNode* harr;
	int heap_size;

public:
	MinHeap(MinHeapNode a[], int size);
	
	void MinHeapify(int);

	int left(int i) { return (2 * i + 1); }

	int right(int i) { return (2 * i + 2); }

	MinHeapNode getMin() { return harr[0]; }

	void replaceMin(MinHeapNode x)
	{
		harr[0] = x;
		MinHeapify(0);
	}
};

// utility functions
void swap(MinHeapNode* x, MinHeapNode* y)
{
	MinHeapNode temp = *x;
	*x = *y;
	*y = temp;
}


#endif

