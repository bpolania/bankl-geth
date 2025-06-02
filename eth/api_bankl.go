package eth

import (
	"context"

	"github.com/ethereum/go-ethereum/core"
)

// FilterAPI provides JSON-RPC methods for address filtering
type FilterAPI struct {
	eth *Ethereum
}

// NewFilterAPI creates a new filter API instance
func NewFilterAPI(eth *Ethereum) *FilterAPI {
	return &FilterAPI{eth: eth}
}

// UpdateAddresses updates the list of monitored addresses
func (api *FilterAPI) UpdateAddresses(addresses []string) error {
	api.eth.blockchain.UpdateFilterAddresses(addresses)
	return nil
}

// GetFilterStats returns current filtering statistics
func (api *FilterAPI) GetFilterStats() map[string]interface{} {
	return api.eth.blockchain.GetFilterStats()
}

// GetFilteredBlock returns filtered transaction data for a specific block
func (api *FilterAPI) GetFilteredBlock(ctx context.Context, blockNumber uint64) *core.FilteredBlockData {
	return api.eth.blockchain.GetFilteredBlockData(blockNumber)
}

// GetFilteredBlocks returns filtered transaction data for a range of blocks
func (api *FilterAPI) GetFilteredBlocks(ctx context.Context, fromBlock, toBlock uint64) []*core.FilteredBlockData {
	var results []*core.FilteredBlockData

	for blockNum := fromBlock; blockNum <= toBlock; blockNum++ {
		if filtered := api.eth.blockchain.GetFilteredBlockData(blockNum); filtered != nil {
			results = append(results, filtered)
		}
	}

	return results
}
