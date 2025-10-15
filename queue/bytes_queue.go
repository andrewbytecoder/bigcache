package queue

import (
	"encoding/binary"
	"log"
	"time"
)

const (
	// Number of bytes to encode 0 in uvarint format
	minimumHeaderSize = 17 // 1 byte blobsize + timestampSizeInBytes + hashSizeInBytes
	// Bytes before left margin are not used. Zero index means element does not exist in queue, useful while reading slice from index
	leftMarginIndex = 1
)

var (
	errEmptyQueue       = &queueError{"Empty queue"}
	errInvalidIndex     = &queueError{"Index must be greater than zero. Invalid index."}
	errIndexOutOfBounds = &queueError{"Index out of range"}
	errFullQueue        = &queueError{"Full queue. Maximum size limit reached."}
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

type queueError struct {
	message string
}

// getNeededSize returns the number of bytes an entry of length need in the queue
func getNeededSize(length int) int {
	var header int
	switch {
	case length < 127: // 1<<7-1
		header = 1
	case length < 16382: // 1<<14-2
		header = 2
	case length < 2097149: // 1<<21 -3
		header = 3
	case length < 268435452: // 1<<28 -4
		header = 4
	default:
		header = 5
	}

	return length + header
}

// NewBytesQueue initialize new bytes queue.
// capacity is used in bytes array allocation
// When verbose flag is set then information about memory allocation are printed
func NewBytesQueue(capacity int, maxCapacity int, verbose bool) *BytesQueue {
	return &BytesQueue{
		array:        make([]byte, capacity),
		capacity:     capacity,
		maxCapacity:  maxCapacity,
		headerBuffer: make([]byte, binary.MaxVarintLen32),
		tail:         leftMarginIndex,
		head:         leftMarginIndex,
		rightMargin:  leftMarginIndex,
		verbose:      verbose,
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

// Push copies entry at the end of queue and moves tail pointer. Allocates more space if needed.
// Returns index for pushed data or error if maximum size queue limit is reached.
func (q *BytesQueue) Push(data []byte) (int, error) {
	neededSize := getNeededSize(len(data))

	if !q.canInsertAfterTail(neededSize) {
		if q.canInsertBeforeHead(neededSize) {
			q.tail = leftMarginIndex
		} else if q.capacity+neededSize >= q.maxCapacity && q.maxCapacity > 0 {
			return -1, errFullQueue
		} else {
			q.allocateAdditionalMemory(neededSize)
		}
	}

	index := q.tail

	q.push(data, neededSize)

	return index, nil
}

// allocateAdditionalMemory 为 BytesQueue 分配额外内存，使其容量至少能容纳 minimum 字节。
// 该方法在当前容量不足时被调用（例如 Push 操作空间不够）。
// 扩容策略：至少翻倍，但不超过 maxCapacity（若设置）。
func (q *BytesQueue) allocateAdditionalMemory(minimum int) {
	start := time.Now() // 记录扩容开始时间（用于性能日志）

	// Step 1: 确保新容量至少比 minimum 大
	if q.capacity < minimum {
		q.capacity += minimum
	}

	// Step 2: 将容量翻倍（即使已满足 minimum，也尝试翻倍以减少频繁扩容）
	q.capacity = q.capacity * 2

	// Step 3: 如果设置了最大容量限制（maxCapacity > 0），则不能超过它
	if q.capacity > q.maxCapacity && q.maxCapacity > 0 {
		q.capacity = q.maxCapacity
	}

	// Step 4: 保存旧数组指针，用于后续数据迁移
	oldArray := q.array

	// Step 5: 分配新的大容量字节数组
	q.array = make([]byte, q.capacity)

	// Step 6: 判断是否需要迁移旧数据
	// leftMarginIndex 是一个常量（通常为 0），q.rightMargin 表示已使用数据的右边界
	if leftMarginIndex != q.rightMargin {
		// 6.1 将旧数组中 [0, q.rightMargin) 的数据拷贝到新数组开头
		copy(q.array, oldArray[:q.rightMargin])

		// 6.2 处理“环形队列已绕回”的情况：即 tail <= head（数据跨越了数组末尾）
		if q.tail <= q.head {
			if q.tail != q.head {
				// 说明中间有一段“空洞”（已被弹出的数据），但 head 到数组末尾还有有效数据？
				// 实际上，这里逻辑存疑或为特殊处理 —— 更可能是将“尾部空闲段”用 dummy 数据填充？
				// 注：make([]byte, q.head-q.tail) 创建零值切片，然后 push 它（可能用于对齐？）
				// 但此操作在扩容后似乎多余，可能是历史遗留或调试代码。
				q.push(make([]byte, q.head-q.tail), q.head-q.tail)
			}

			// 6.3 重置 head 和 tail 指针：
			// - head 移到起始位置（leftMarginIndex = 0）
			// - tail 指向原数据末尾（即新数据的末尾）
			q.head = leftMarginIndex
			q.tail = q.rightMargin
		}
		// else: 如果 tail > head（正常线性状态），则 head/tail 不变，数据已连续拷贝到开头
	}

	// Step 7: 标记队列不再“满”（因为刚扩容）
	q.full = false

	// Step 8: 若启用 verbose 模式，打印扩容耗时和新容量
	if q.verbose {
		log.Printf("Allocated new queue in %s; Capacity: %d \n", time.Since(start), q.capacity)
	}
}

func (q *BytesQueue) push(data []byte, len int) {
	headerEntrySize := binary.PutUvarint(q.headerBuffer, uint64(len))
	q.copy(q.headerBuffer, headerEntrySize)

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

// Pop reads the oldest entry from queue and moves head pointer to the next one
func (q *BytesQueue) Pop() ([]byte, error) {
	data, blockSize, err := q.peek(q.head)
	if err != nil {
		return nil, err
	}

	q.head += blockSize
	q.count--

	if q.head == q.rightMargin {
		q.head = leftMarginIndex
		if q.tail == q.rightMargin {
			q.tail = leftMarginIndex
		}
		q.rightMargin = q.tail
	}

	q.full = false

	return data, nil
}

// Peek reads the oldest entry from list without moving head pointer
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

// Capacity returns number of allocated bytes for queue
func (q *BytesQueue) Capacity() int {
	return q.capacity
}

// Len returns number of entries kept in queue
func (q *BytesQueue) Len() int {
	return q.count
}

// Error returns error message
func (e *queueError) Error() string {
	return e.message
}

// peekCheckErr is identical to peek, but does not actually return any data
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

// peek returns the data from index and the number of bytes to encode the length of the data in uvarint format
func (q *BytesQueue) peek(index int) ([]byte, int, error) {
	err := q.peekCheckErr(index)
	if err != nil {
		return nil, 0, err
	}

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
	// 1. there is exactly need bytes between head and tail, so we do not need
	// to reserve extra space for a potential empty entry when realloc this queue
	// 2. still have unused space between tail and head, then we must reserve
	// at least headerEntrySize bytes so we can put an empty entry
	return q.head-q.tail == need || q.head-q.tail >= need+minimumHeaderSize
}

// canInsertBeforeHead returns true if it's possible to insert an entry of size of need before the head of the queue
func (q *BytesQueue) canInsertBeforeHead(need int) bool {
	if q.full {
		return false
	}
	if q.tail >= q.head {
		return q.head-leftMarginIndex == need || q.head-leftMarginIndex >= need+minimumHeaderSize
	}
	return q.head-q.tail == need || q.head-q.tail >= need+minimumHeaderSize
}
