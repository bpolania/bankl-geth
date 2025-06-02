package core

import (
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
)

// AddressSet holds a snapshot of addresses at a specific time
type AddressSet struct {
	addresses map[common.Address]bool
	timestamp time.Time
	count     int
}

// DynamicAddressFilter provides lock-free address filtering with real-time updates
type DynamicAddressFilter struct {
	// Atomic pointer to current address set - zero-lock reads
	currentSet unsafe.Pointer // *AddressSet

	signer types.Signer

	// Update management
	updateChan chan []string
	stopChan   chan struct{}

	// Metrics (atomic counters)
	totalChecked uint64
	totalMatched uint64
	updateCount  uint64
}

// NewDynamicAddressFilter creates a new dynamic address filter
func NewDynamicAddressFilter(chainConfig *params.ChainConfig) *DynamicAddressFilter {
	// Start with empty set
	initialSet := &AddressSet{
		addresses: make(map[common.Address]bool),
		timestamp: time.Now(),
		count:     0,
	}

	filter := &DynamicAddressFilter{
		signer:     types.LatestSigner(chainConfig),
		updateChan: make(chan []string, 100), // Buffer for rapid updates
		stopChan:   make(chan struct{}),
	}

	// Store initial set atomically
	atomic.StorePointer(&filter.currentSet, unsafe.Pointer(initialSet))

	// Start update processor
	go filter.updateProcessor()

	log.Info("Dynamic address filter initialized")
	return filter
}

// ShouldInclude checks if a transaction involves any monitored addresses
// This is the hot path - must be extremely fast with zero locks
func (df *DynamicAddressFilter) ShouldInclude(tx *types.Transaction) bool {
	// Atomic load - no locks, no waiting!
	setPtr := atomic.LoadPointer(&df.currentSet)
	if setPtr == nil {
		return false // No addresses configured
	}

	addressSet := (*AddressSet)(setPtr)
	atomic.AddUint64(&df.totalChecked, 1)

	// Check recipient first (fast - already available)
	if tx.To() != nil && addressSet.addresses[*tx.To()] {
		atomic.AddUint64(&df.totalMatched, 1)
		return true
	}

	// Check sender (requires signature recovery - more expensive)
	if sender, err := types.Sender(df.signer, tx); err == nil {
		if addressSet.addresses[sender] {
			atomic.AddUint64(&df.totalMatched, 1)
			return true
		}
	}

	return false
}

// UpdateAddresses updates the address set in a non-blocking way
func (df *DynamicAddressFilter) UpdateAddresses(addresses []string) {
	select {
	case df.updateChan <- addresses:
		log.Debug("Address update queued", "count", len(addresses))
	default:
		log.Warn("Address update channel full, dropping update", "count", len(addresses))
	}
}

// updateProcessor handles address updates in the background
func (df *DynamicAddressFilter) updateProcessor() {
	updateTicker := time.NewTicker(100 * time.Millisecond) // Batch updates every 100ms
	defer updateTicker.Stop()

	var pendingAddresses []string

	for {
		select {
		case newAddresses := <-df.updateChan:
			// Always keep the latest update (overwrites pending)
			pendingAddresses = newAddresses

		case <-updateTicker.C:
			if pendingAddresses != nil {
				df.applyAddressUpdate(pendingAddresses)
				pendingAddresses = nil
			}

		case <-df.stopChan:
			log.Info("Address filter update processor stopped")
			return
		}
	}
}

// applyAddressUpdate creates a new address set and swaps it atomically
func (df *DynamicAddressFilter) applyAddressUpdate(addresses []string) {
	// Build new address set
	newSet := &AddressSet{
		addresses: make(map[common.Address]bool, len(addresses)),
		timestamp: time.Now(),
		count:     0,
	}

	// Populate new address set
	for _, addrStr := range addresses {
		if len(addrStr) >= 42 { // Valid Ethereum address length
			addr := common.HexToAddress(addrStr)
			newSet.addresses[addr] = true
			newSet.count++
		}
	}

	// Atomic swap - instant switch, no locks needed!
	oldPtr := atomic.SwapPointer(&df.currentSet, unsafe.Pointer(newSet))
	atomic.AddUint64(&df.updateCount, 1)

	log.Info("Address set updated",
		"addresses", newSet.count,
		"update_number", atomic.LoadUint64(&df.updateCount))

	// Let GC handle the old set
	_ = oldPtr
}

// GetMetrics returns current filter metrics
func (df *DynamicAddressFilter) GetMetrics() (checked, matched, updates uint64, addressCount int) {
	setPtr := atomic.LoadPointer(&df.currentSet)
	count := 0
	if setPtr != nil {
		addressSet := (*AddressSet)(setPtr)
		count = addressSet.count
	}

	return atomic.LoadUint64(&df.totalChecked),
		atomic.LoadUint64(&df.totalMatched),
		atomic.LoadUint64(&df.updateCount),
		count
}

// IsEnabled returns true if the filter has addresses configured
func (df *DynamicAddressFilter) IsEnabled() bool {
	setPtr := atomic.LoadPointer(&df.currentSet)
	if setPtr == nil {
		return false
	}
	addressSet := (*AddressSet)(setPtr)
	return addressSet.count > 0
}

// Stop shuts down the address filter
func (df *DynamicAddressFilter) Stop() {
	close(df.stopChan)
	log.Info("Dynamic address filter stopped")
}
