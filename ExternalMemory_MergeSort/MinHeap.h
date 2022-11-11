#ifndef MINHEAP_H
#define MINHEAP_H

// originally unsigned int
typedef unsigned long long KEY_TYPE;


struct MinHeapNode
{
	KEY_TYPE val;
	unsigned int chunk_index;
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
extern void swap(MinHeapNode* x, MinHeapNode* y);


#endif

