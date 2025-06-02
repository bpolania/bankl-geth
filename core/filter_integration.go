package core

import (
	"time"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
)

// FilteredBlockData represents a block with only relevant transactions
type FilteredBlockData struct {
	BlockNumber     uint64
	BlockHash       string
	Timestamp       uint64
	Transactions    []*FilteredTransaction
	OriginalTxCount int
	FilteredTxCount int
}

// FilteredTransaction represents a transaction that passed the address filter
type FilteredTransaction struct {
	Hash     string
	From     string
	To       string
	Value    string
	GasPrice string
	GasUsed  uint64
	Status   uint64
	TxIndex  uint64
}

// BlockFilter handles the filtering of entire blocks
type BlockFilter struct {
	addressFilter *DynamicAddressFilter
	enabled       bool
}

// NewBlockFilter creates a new block filter
func NewBlockFilter(addressFilter *DynamicAddressFilter) *BlockFilter {
	return &BlockFilter{
		addressFilter: addressFilter,
		enabled:       addressFilter != nil && addressFilter.IsEnabled(),
	}
}

// FilterBlock processes a block and returns only transactions involving monitored addresses
func (bf *BlockFilter) FilterBlock(block *types.Block, receipts types.Receipts) *FilteredBlockData {
	if !bf.enabled || !bf.addressFilter.IsEnabled() {
		return nil // No filtering needed
	}

	startTime := time.Now()
	filteredTxs := make([]*FilteredTransaction, 0)

	// Process each transaction in the block
	for i, tx := range block.Transactions() {
		if bf.addressFilter.ShouldInclude(tx) {
			// Convert to filtered transaction format
			var toAddr string
			if tx.To() != nil {
				toAddr = tx.To().Hex()
			}

			var fromAddr string
			if from, err := types.Sender(bf.addressFilter.signer, tx); err == nil {
				fromAddr = from.Hex()
			}

			var gasUsed uint64
			var status uint64
			if i < len(receipts) && receipts[i] != nil {
				gasUsed = receipts[i].GasUsed
				status = receipts[i].Status
			}

			filteredTx := &FilteredTransaction{
				Hash:     tx.Hash().Hex(),
				From:     fromAddr,
				To:       toAddr,
				Value:    tx.Value().String(),
				GasPrice: tx.GasPrice().String(),
				GasUsed:  gasUsed,
				Status:   status,
				TxIndex:  uint64(i),
			}

			filteredTxs = append(filteredTxs, filteredTx)
		}
	}

	processingTime := time.Since(startTime)

	if len(filteredTxs) > 0 {
		log.Debug("Block filtered",
			"number", block.NumberU64(),
			"original_txs", len(block.Transactions()),
			"filtered_txs", len(filteredTxs),
			"processing_time", processingTime)
	}

	return &FilteredBlockData{
		BlockNumber:     block.NumberU64(),
		BlockHash:       block.Hash().Hex(),
		Timestamp:       block.Time(),
		Transactions:    filteredTxs,
		OriginalTxCount: len(block.Transactions()),
		FilteredTxCount: len(filteredTxs),
	}
}

// UpdateAddresses updates the monitored addresses (for API calls)
func (bf *BlockFilter) UpdateAddresses(addresses []string) {
	if bf.addressFilter != nil {
		bf.addressFilter.UpdateAddresses(addresses)
		bf.enabled = bf.addressFilter.IsEnabled()
	}
}

// ReloadFromFile triggers a manual reload from the address file
func (bf *BlockFilter) ReloadFromFile() error {
	if bf.addressFilter != nil {
		err := bf.addressFilter.ReloadFromFile()
		bf.enabled = bf.addressFilter.IsEnabled()
		return err
	}
	return nil
}

// GetFilterStats returns current filtering statistics
func (bf *BlockFilter) GetFilterStats() map[string]interface{} {
	if bf.addressFilter == nil {
		return map[string]interface{}{"enabled": false}
	}

	checked, matched, updates, fileReloads, addressCount := bf.addressFilter.GetMetrics()

	var matchRate float64
	if checked > 0 {
		matchRate = float64(matched) / float64(checked) * 100
	}

	return map[string]interface{}{
		"enabled":              bf.enabled,
		"address_count":        addressCount,
		"address_file":         bf.addressFilter.GetAddressFile(),
		"transactions_checked": checked,
		"transactions_matched": matched,
		"match_rate_percent":   matchRate,
		"api_updates":          updates,
		"file_reloads":         fileReloads,
	}
}
