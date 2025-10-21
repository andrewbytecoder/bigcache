package queue

import (
	"encoding/binary"
	"errors"
	"fmt"
	"time"
)

const (
	// Number of bytes to encode 0 in uvarint format
	minimumHeaderSize = 17 // 1 byte blobsize + timestampSizeInBytes + hashSizeInBytes
	// Bytes before left margin are not used. Zero index means element does not exist in queue, useful while reading slice from index
	leftMarginIndex = 1
)

var (
	errEmptyQueue       = errors.New("queue is empty")
	errInvalidIndex     = errors.New("index must be greater than zero, Invalid index")
	errIndexOutOfBounds = errors.New("index out of bounds")
	errFullQueue        = errors.New("queue is full, Maximum size limit reached")
)

// BytesQueue is a non-thread safe queue type of fifo based on bytes array.
// For every push operation index of entry is returned. It can be used to read the entry later
type BytesQueue struct {
	full         bool
	array        []byte
	capacity     int
	maxCapacity  int
	head         int
	tail         int
	count        int
	rightMargin  int
	headerBuffer []byte
	verbose      bool
}

// getNeededSize returns the number of bytes an entry of length need in the queue
func getNeededSize(length int) int {
	// 实际需要的字节数为 header + length
	var header int
	switch {
	// 1 byte for header, length <= 126
	case length < 127: // 1 << 7 - 1
		header = 1
	// 2 byte for header, length <= 16381, 1 << 14 - 1 = 16383
	case length < 16382:
		header = 2
	// 3 byte for header, length <= 2097148, 1 << 21 - 1 = 2097151
	case length < 2097149:
		header = 3
	// 4 byte for header, length <= 268435451, 1 << 28 - 1 = 268435455
	case length < 268435452:
		header = 4
	default:
		header = 5
	}
	return header + length
}

// NewBytesQueue initializes new bytes queue.
// capacity is the used in bytes array allocated for queue.
// When verbose flag is set then information about memory allocation are printed to console
func NewBytesQueue(capacity int, maxCapacity int, verbose bool) *BytesQueue {
	return &BytesQueue{
		array:        make([]byte, capacity),              // 初始化一个字节数组
		capacity:     capacity,                            // 容量
		maxCapacity:  maxCapacity,                         // 最大容量
		headerBuffer: make([]byte, binary.MaxVarintLen32), // header buffer
		tail:         leftMarginIndex,                     // tail index
		head:         leftMarginIndex,                     // head index
		rightMargin:  leftMarginIndex,                     // right margin index
		verbose:      verbose,                             // verbose
	}
}

// Reset removes all entries from queue
func (q *BytesQueue) Reset() {
	// Just reset indexes
	q.tail = leftMarginIndex
	q.head = leftMarginIndex
	q.rightMargin = leftMarginIndex
	q.count = 0
	q.full = false
}

// Push copies entry at the end of queue and moves tail pointer. allocates more space if needed.
// Returns index for pushed data or error if maximum size queue limit is reached
func (q *BytesQueue) Push(entry []byte) (int, error) {
	neededSize := getNeededSize(len(entry))

	if !q.canInsertAfterTail(neededSize) {
		if q.canInsertBeforeHead(neededSize) {
			// 后端插入不了，但是 前端能插入，直接将tail 移动到leftMarginIndex，也就是移动到队列的最开始
			q.tail = leftMarginIndex
		} else if q.capacity+neededSize >= q.maxCapacity && q.maxCapacity > 0 {
			return -1, errFullQueue
		} else {
			// 扩容
			q.allocateAdditionalMemory(neededSize)
		}
	}

	// 上次的tail就是这次的index指向位置
	index := q.tail
	q.push(entry, neededSize)

	return index, nil
}

// allocateAdditionalMemory 为BytesQueue 分配额外的内存，使其容量至少能容纳minimum字节。
// 该方法在当前容量不足时被调用 （例如Push 空间不够）
// 扩容策略： 至少翻倍，但是不超过maxCapacity （如果设置）
func (q *BytesQueue) allocateAdditionalMemory(minimum int) {
	// 扩容开始时间
	start := time.Now()

	// 1. 确保新容量至少比 minimum 大
	if q.capacity < minimum {
		q.capacity += minimum
	}

	// 2. 将容量翻倍，避免频繁的扩容
	q.capacity *= 2

	// 3. 确保新容量不超过maxCapacity
	if q.maxCapacity > 0 && q.capacity > q.maxCapacity {
		q.capacity = q.maxCapacity
	}

	// 4. 保存旧数组指针，用于后续数据迁移
	oldArray := q.array

	// 5. 创建新的数组，大小为旧数组的2倍
	q.array = make([]byte, q.capacity)

	// 6. 判断是否需要迁移旧数据
	// leftMarginIndex 是一个常量（通常为 0, 这里为1），q.rightMargin 表示已使用数据的右边界
	if leftMarginIndex != q.rightMargin {
		// 6.1 将旧数组中 [0, q.rightMargin) 区间内的数据复制到新数组中
		copy(q.array, oldArray[:q.rightMargin])

		// 6.2 处理环形队列已回绕的情况：即 tail <= head 的情况
		if q.tail <= q.head {
			if q.tail != q.head {
				// 创建空闲区，并使用空slice填充
				q.push(make([]byte, q.head-q.tail), q.head-q.tail)
			}
			// 6.3 重置 head 和 tail 指针
			// - head 移动到新数组的左侧
			// - tail 移动到新数组的右侧
			q.head = leftMarginIndex
			q.tail = q.rightMargin
		}
		// else: 如果tail > head 数据已经连续不需要进行处理
	}
	// 7. 表级队列容量不满
	q.full = false
	// 8. 若使用verbose模式，打印扩容耗时和新容量信息
	if q.verbose {
		fmt.Printf("Expanding queue to %d bytes in %f\n", q.capacity, time.Since(start).Seconds())
	}
}
func (q *BytesQueue) push(data []byte, len int) {
	headerEntrySize := binary.PutUvarint(q.headerBuffer, uint64(len))
	// 将headerEntrySize字节写入到 array中
	q.copy(q.headerBuffer, headerEntrySize)
	// 将data 写入到 array中
	q.copy(data, len-headerEntrySize)

	if q.tail > q.head {
		q.rightMargin = q.tail
	}
	if q.tail == q.head {
		q.full = true
	}

	q.count++
}

func (q *BytesQueue) copy(data []byte, len int) {
	q.tail += copy(q.array[q.tail:], data[:len])
}

// Pop removes the first entry from the queue (FIFO order) and returns its data.
// It also updates the head pointer and decrements the count of elements.
// If the head reaches the right margin after popping, it resets the head and potentially
// the tail to the left margin to maintain ring buffer behavior.
// This method sets the 'full' flag to false since an element has been removed.
func (q *BytesQueue) Pop() ([]byte, error) {
	// Read the data at the current head position without removing it yet
	data, blockSize, err := q.peek(q.head)
	if err != nil {
		return nil, err
	}

	// Move the head forward by the size of the block that was just read
	q.head += blockSize
	q.count--

	// If head reaches right margin, reset pointers to maintain ring structure
	// 如果数据弹空了，则将head 移动到leftMarginIndex
	if q.head == q.rightMargin {
		q.head = leftMarginIndex
		if q.tail == q.rightMargin {
			q.tail = leftMarginIndex
		}
		q.rightMargin = q.tail
	}

	// Since we've popped an item, the queue cannot be full anymore
	q.full = false

	return data, nil
}

// Peek reads the oldest entry from list  without moving head pointer
func (q *BytesQueue) Peek() ([]byte, error) {
	data, _, err := q.peek(q.head)
	return data, err
}

// Get reads entry from index
func (q *BytesQueue) Get(index int) ([]byte, error) {
	data, _, err := q.peek(index)
	return data, err
}

// CheckGet checks if an entry can be read from index
func (q *BytesQueue) CheckGet(index int) error {
	return q.peekCheckErr(index)
}

// Capacity returns the numbers of allocated bytes for queue
func (q *BytesQueue) Capacity() int {
	return q.capacity
}

// Len returns the number of elements in the queue
func (q *BytesQueue) Len() int {
	return q.count
}

// peekCheckErr is identical to peek, but dost not actually pops the entry
func (q *BytesQueue) peekCheckErr(index int) error {
	if q.count == 0 {
		return errEmptyQueue
	}
	if index <= 0 {
		return errInvalidIndex
	}
	if index >= len(q.array) {
		return errIndexOutOfBounds
	}
	return nil
}

// Peek returns the data from index and the number of bytes to encode the length of the data in uvarint format
func (q *BytesQueue) peek(index int) ([]byte, int, error) {
	err := q.peekCheckErr(index)
	if err != nil {
		return nil, 0, err
	}

	// [size][entry]， n 代表size的长度
	blockSize, n := binary.Uvarint(q.array[index:])
	return q.array[index+n : index+int(blockSize)], int(blockSize), nil

}

// canInsertAfterTail returns true if it's possible to insert an entry of size of need after the tail of the queue
func (q *BytesQueue) canInsertAfterTail(need int) bool {
	if q.full {
		return false
	}
	if q.tail >= q.head {
		return q.capacity-q.tail >= need
	}
	// 必须保留至少 minimumHeaderSize 字节，用于在将来队列“刚好填满”时，能写入一个“空条目”或“结束标记”，以区分“空”和“满”状态
	return q.head-q.tail == need || q.head-q.tail >= need+minimumHeaderSize
}

// canInsertBeforeHead returns true if it's possible to insert an entry of size of need before the head of the queue
func (q *BytesQueue) canInsertBeforeHead(need int) bool {
	if q.full {
		return false
	}
	// 空闲区域在 [1, head) 和 [tail+1, capacity)
	if q.tail >= q.head {
		return q.head-leftMarginIndex == need || q.head-leftMarginIndex >= need+minimumHeaderSize
	}

	return q.head-q.tail == need || q.head-q.tail >= need+minimumHeaderSize
}
