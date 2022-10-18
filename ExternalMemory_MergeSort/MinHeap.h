#ifndef MINHEAP_H
#define MINHEAP_H


struct MinHeapNode
{
	unsigned int val;

	// says which chunk the index comes from
	unsigned int chunk_index;
	// where in the chunk is the current value from
	unsigned int val_index;
	// the current last valid index of the chunk
	unsigned int last_val_index;
	// how many times this chunk has gathered new values
	unsigned int num_times_pulled;
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

