package queue

import "sync"

type Empty struct{}

// Item is an item that can be added to the priority queue.
type Item interface {
	// Compare returns a bool that can be used to determine
	// ordering in the priority queue.  Assuming the queue
	// is in ascending order, this should return > logic.
	// Return 1 to indicate this object is greater than the
	//  other logic, 0 to indicate equality, and -1 to indicate
	// less than others.
	Compare(other Item) int
	GetUniqueID() string
}

type priorityItems []Item

func (items *priorityItems) swap(i, j int) {
	(*items)[i], (*items)[j] = (*items)[j], (*items)[i]
}

func (items *priorityItems) pop() Item {
	size := len(*items)

	// Move last leaf to root, and 'pop' the last item.
	items.swap(size-1, 0)
	item := (*items)[size-1] // Item to return.
	(*items)[size-1], *items = nil, (*items)[:size-1]

	// 'Bubble down' to restore heap property.
	index := 0
	childL, childR := 2*index+1, 2*index+2
	for len(*items) > childL {
		child := childL
		if len(*items) > childR && (*items)[childR].Compare((*items)[childL]) < 0 {
			child = childR
		}

		if (*items)[child].Compare((*items)[index]) < 0 {
			items.swap(index, child)

			index = child
			childL, childR = 2*index+1, 2*index+2
		} else {
			break
		}
	}

	return item
}

func (items *priorityItems) get(number int) []Item {
	returnItems := make([]Item, 0, number)
	for i := 0; i < number; i++ {
		if len(*items) == 0 {
			break
		}

		returnItems = append(returnItems, items.pop())
	}

	return returnItems
}

func (items *priorityItems) push(item Item) {
	// Stick the item as the end of the last level.
	*items = append(*items, item)

	// 'Bubble up' to restore heap property.
	index := len(*items) - 1
	parent := int((index - 1) / 2)
	for parent >= 0 && (*items)[parent].Compare(item) > 0 {
		items.swap(index, parent)

		index = parent
		parent = int((index - 1) / 2)
	}
}

// PriorityQueue is similar to queue except that it takes
// items that implement the Item interface and adds them
// to the queue in priority order.
type PriorityQueue struct {
	waiters         waiters
	items           priorityItems
	itemMap         map[string]Empty
	lock            sync.Mutex
	disposeLock     sync.Mutex
	disposed        bool
	allowDuplicates bool
}

// Put adds items to the queue.
func (pq *PriorityQueue) Put(items ...Item) error {
	if len(items) == 0 {
		return nil
	}

	pq.lock.Lock()
	defer pq.lock.Unlock()

	if pq.disposed {
		return ErrDisposed
	}

	for _, item := range items {
		if pq.allowDuplicates {
			pq.items.push(item)
		} else if _, ok := pq.itemMap[item.GetUniqueID()]; !ok {
			pq.itemMap[item.GetUniqueID()] = Empty{}
			pq.items.push(item)
		}
	}

	for {
		sema := pq.waiters.get()
		if sema == nil {
			break
		}

		sema.response.Add(1)
		sema.ready <- true
		sema.response.Wait()
		if len(pq.items) == 0 {
			break
		}
	}

	return nil
}

// Get retrieves items from the queue.  If the queue is empty,
// this call blocks until the next item is added to the queue.  This
// will attempt to retrieve number of items.
func (pq *PriorityQueue) Get(number int) ([]Item, error) {
	if number < 1 {
		return nil, nil
	}

	pq.lock.Lock()

	if pq.disposed {
		pq.lock.Unlock()
		return nil, ErrDisposed
	}

	var items []Item

	// Remove references to popped items.
	deleteItems := func(items []Item) {
		for _, item := range items {
			delete(pq.itemMap, item.GetUniqueID())
		}
	}

	if len(pq.items) == 0 {
		sema := newSema()
		pq.waiters.put(sema)
		pq.lock.Unlock()

		<-sema.ready

		if pq.Disposed() {
			return nil, ErrDisposed
		}

		items = pq.items.get(number)
		if !pq.allowDuplicates {
			deleteItems(items)
		}
		sema.response.Done()
		return items, nil
	}

	items = pq.items.get(number)
	deleteItems(items)
	pq.lock.Unlock()
	return items, nil
}

// Peek will look at the next item without removing it from the queue.
func (pq *PriorityQueue) Peek() Item {
	pq.lock.Lock()
	defer pq.lock.Unlock()
	if len(pq.items) > 0 {
		return pq.items[0]
	}
	return nil
}

// Empty returns a bool indicating if there are any items left
// in the queue.
func (pq *PriorityQueue) Empty() bool {
	pq.lock.Lock()
	defer pq.lock.Unlock()

	return len(pq.items) == 0
}

// Len returns a number indicating how many items are in the queue.
func (pq *PriorityQueue) Len() int {
	pq.lock.Lock()
	defer pq.lock.Unlock()

	return len(pq.items)
}

// Disposed returns a bool indicating if this queue has been disposed.
func (pq *PriorityQueue) Disposed() bool {
	pq.disposeLock.Lock()
	defer pq.disposeLock.Unlock()

	return pq.disposed
}

// Dispose will prevent any further reads/writes to this queue
// and frees available resources.
func (pq *PriorityQueue) Dispose() {
	pq.lock.Lock()
	defer pq.lock.Unlock()

	pq.disposeLock.Lock()
	defer pq.disposeLock.Unlock()

	pq.disposed = true
	for _, waiter := range pq.waiters {
		waiter.response.Add(1)
		waiter.ready <- true
	}

	pq.items = nil
	pq.waiters = nil
}

// NewPriorityQueue is the constructor for a priority queue.
func NewPriorityQueue(hint int, allowDuplicates bool) *PriorityQueue {
	return &PriorityQueue{
		items:           make(priorityItems, 0, hint),
		itemMap:         make(map[string]Empty, hint),
		allowDuplicates: allowDuplicates,
	}
}
