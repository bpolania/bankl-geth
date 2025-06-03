package core

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
	"github.com/fsnotify/fsnotify"
)

// AddressSet holds a snapshot of addresses at a specific time
type AddressSet struct {
	addresses map[common.Address]bool
	timestamp time.Time
	count     int
	fileHash  string // Hash of the file content when loaded
}

// DynamicAddressFilter provides lock-free address filtering with file-based updates
type DynamicAddressFilter struct {
	// Atomic pointer to current address set - zero-lock reads
	currentSet unsafe.Pointer // *AddressSet

	signer types.Signer

	// File watching
	addressFile string
	fileWatcher *fsnotify.Watcher

	// Update management
	updateChan chan string // File path to reload
	stopChan   chan struct{}

	// Metrics (atomic counters)
	totalChecked uint64
	totalMatched uint64
	updateCount  uint64
	fileReloads  uint64
}

// NewDynamicAddressFilter creates a new dynamic address filter with file watching
func NewDynamicAddressFilter(chainConfig *params.ChainConfig, addressFile string) *DynamicAddressFilter {
	// Start with empty set
	initialSet := &AddressSet{
		addresses: make(map[common.Address]bool),
		timestamp: time.Now(),
		count:     0,
		fileHash:  "",
	}

	filter := &DynamicAddressFilter{
		signer:      types.LatestSigner(chainConfig),
		addressFile: addressFile,
		updateChan:  make(chan string, 10), // Buffer for file updates
		stopChan:    make(chan struct{}),
	}

	// Store initial set atomically
	atomic.StorePointer(&filter.currentSet, unsafe.Pointer(initialSet))

	// Load initial addresses from file if provided
	if addressFile != "" {
		if err := filter.loadAddressesFromFile(addressFile); err != nil {
			log.Error("Failed to load initial addresses", "file", addressFile, "err", err)
		} else {
			log.Info("Loaded initial addresses from file", "file", addressFile)
		}

		// Set up file watching
		if err := filter.setupFileWatcher(); err != nil {
			log.Error("Failed to setup file watcher", "file", addressFile, "err", err)
		}
	}

	// Start update processor
	go filter.updateProcessor()

	log.Info("Dynamic address filter initialized", "file", addressFile)
	return filter
}

// setupFileWatcher creates a file system watcher for the address file
func (df *DynamicAddressFilter) setupFileWatcher() error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}

	df.fileWatcher = watcher

	// Watch the directory containing the file (watching files directly can be unreliable)
	dir := filepath.Dir(df.addressFile)
	if err := watcher.Add(dir); err != nil {
		watcher.Close()
		return err
	}

	// Start the file watching goroutine
	go df.fileWatcherLoop()

	log.Info("File watcher setup complete", "watching", dir)
	return nil
}

// fileWatcherLoop monitors file system events
func (df *DynamicAddressFilter) fileWatcherLoop() {
	defer df.fileWatcher.Close()

	for {
		select {
		case event, ok := <-df.fileWatcher.Events:
			if !ok {
				return
			}

			// Check if the event is for our address file
			if event.Name == df.addressFile {
				if event.Op&fsnotify.Write == fsnotify.Write || event.Op&fsnotify.Create == fsnotify.Create {
					log.Debug("Address file modified", "file", df.addressFile, "op", event.Op)

					// Debounce rapid file changes
					time.Sleep(100 * time.Millisecond)

					select {
					case df.updateChan <- df.addressFile:
						// File update queued
					default:
						log.Warn("File update channel full, skipping reload")
					}
				}
			}

		case err, ok := <-df.fileWatcher.Errors:
			if !ok {
				return
			}
			log.Error("File watcher error", "err", err)

		case <-df.stopChan:
			return
		}
	}
}

// loadAddressesFromFile reads addresses from a file
func (df *DynamicAddressFilter) loadAddressesFromFile(filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	var addresses []string
	scanner := bufio.NewScanner(file)
	lineNum := 0

	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())

		// Skip empty lines and comments
		if len(line) == 0 || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "//") {
			continue
		}

		// Validate address format
		if len(line) < 42 {
			log.Warn("Invalid address format", "file", filePath, "line", lineNum, "address", line)
			continue
		}

		// Clean up address (remove 0x prefix if present, add it back)
		addr := strings.ToLower(strings.TrimPrefix(line, "0x"))
		if len(addr) != 40 {
			log.Warn("Invalid address length", "file", filePath, "line", lineNum, "address", line)
			continue
		}

		addresses = append(addresses, "0x"+addr)
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	// Apply the loaded addresses
	df.updateAddresses(addresses)
	atomic.AddUint64(&df.fileReloads, 1)

	log.Info("Addresses loaded from file",
		"file", filePath,
		"count", len(addresses),
		"reload_number", atomic.LoadUint64(&df.fileReloads))

	return nil
}

// ShouldInclude checks if a transaction involves any monitored addresses
// This is the hot path - must be extremely fast with zero locks
func (df *DynamicAddressFilter) ShouldInclude(tx *types.Transaction) bool {

	setPtr := atomic.LoadPointer(&df.currentSet)
	if setPtr == nil {
		fmt.Printf("No address set loaded\n")
		return false
	}

	addressSet := (*AddressSet)(setPtr)
	if addressSet.count == 0 {
		fmt.Printf("Empty address set\n")
		return false
	}

	// Try to increment and immediately read back
	oldChecked := atomic.LoadUint64(&df.totalChecked)
	newChecked := atomic.AddUint64(&df.totalChecked, 1)
	readBack := atomic.LoadUint64(&df.totalChecked)

	fmt.Printf("Atomic operation: old=%d, new=%d, readback=%d\n", oldChecked, newChecked, readBack)

	// Check recipient
	if tx.To() != nil && addressSet.addresses[*tx.To()] {
		oldMatched := atomic.LoadUint64(&df.totalMatched)
		newMatched := atomic.AddUint64(&df.totalMatched, 1)
		readBackMatched := atomic.LoadUint64(&df.totalMatched)

		fmt.Printf("MATCH! Atomic operation: old=%d, new=%d, readback=%d\n", oldMatched, newMatched, readBackMatched)
		return true
	}

	// Check sender
	if sender, err := types.Sender(df.signer, tx); err == nil {
		if addressSet.addresses[sender] {
			oldMatched := atomic.LoadUint64(&df.totalMatched)
			newMatched := atomic.AddUint64(&df.totalMatched, 1)
			readBackMatched := atomic.LoadUint64(&df.totalMatched)

			fmt.Printf("SENDER MATCH! Atomic operation: old=%d, new=%d, readback=%d\n", oldMatched, newMatched, readBackMatched)
			return true
		}
	}

	return false
}

// UpdateAddresses updates the address set with a new list (for API calls)
func (df *DynamicAddressFilter) UpdateAddresses(addresses []string) {
	df.updateAddresses(addresses)
}

// ReloadFromFile manually triggers a reload from the file
func (df *DynamicAddressFilter) ReloadFromFile() error {
	if df.addressFile == "" {
		return fmt.Errorf("no address file configured")
	}
	return df.loadAddressesFromFile(df.addressFile)
}

// updateAddresses is the internal method that creates and swaps address sets
func (df *DynamicAddressFilter) updateAddresses(addresses []string) {
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

	log.Debug("Address set updated",
		"addresses", newSet.count,
		"update_number", atomic.LoadUint64(&df.updateCount))

	// Let GC handle the old set
	_ = oldPtr
}

// updateProcessor handles file update notifications
func (df *DynamicAddressFilter) updateProcessor() {
	for {
		select {
		case filePath := <-df.updateChan:
			// Reload addresses from file
			if err := df.loadAddressesFromFile(filePath); err != nil {
				log.Error("Failed to reload addresses from file", "file", filePath, "err", err)
			}

		case <-df.stopChan:
			log.Info("Address filter update processor stopped")
			return
		}
	}
}

// GetMetrics returns current filter metrics
func (df *DynamicAddressFilter) GetMetrics() (checked, matched, updates, fileReloads uint64, addressCount int) {
	setPtr := atomic.LoadPointer(&df.currentSet)
	count := 0
	if setPtr != nil {
		addressSet := (*AddressSet)(setPtr)
		count = addressSet.count
	}

	checked = atomic.LoadUint64(&df.totalChecked)
	matched = atomic.LoadUint64(&df.totalMatched)
	updates = atomic.LoadUint64(&df.updateCount)
	fileReloads = atomic.LoadUint64(&df.fileReloads)

	fmt.Printf("GetMetrics called: checked=%d, matched=%d, updates=%d, fileReloads=%d, addressCount=%d\n",
		checked, matched, updates, fileReloads, count)

	return checked, matched, updates, fileReloads, count
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

// GetAddressFile returns the path to the address file
func (df *DynamicAddressFilter) GetAddressFile() string {
	return df.addressFile
}

// Stop shuts down the address filter
func (df *DynamicAddressFilter) Stop() {
	close(df.stopChan)
	if df.fileWatcher != nil {
		df.fileWatcher.Close()
	}
	log.Info("Dynamic address filter stopped")
}
