// File: candyflow-consumer/arrow_data.go
package main

import (
	"fmt"
	"sync"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
)

// CandyPrice struct for quick JSON unmarshal
type CandyPrice struct {
	CandyID   string  `json:"candy_id"`
	StoreID   string  `json:"store_id"`
	Price     float64 `json:"price"`
	Timestamp int64   `json:"timestamp"`
}

// ArrowData holds our in-memory Arrow columns and any extra indexes we need.
type ArrowData struct {
	mu sync.Mutex

	// We'll store an Arrow schema & builder for columnar data.
	schema  *arrow.Schema
	builder *array.RecordBuilder

	// For quick lookups, we keep track of cheapest price per candyID.
	cheapestMap map[string]CheapestEntry
}

// CheapestEntry helps track the store + price for the cheapest instance of a candy.
type CheapestEntry struct {
	StoreID string
	Price   float64
}

// NewArrowData initializes the Arrow schema and builder.
func NewArrowData() *ArrowData {
	// Define the schema
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "candy_id", Type: arrow.BinaryTypes.String},
		{Name: "store_id", Type: arrow.BinaryTypes.String},
		{Name: "price", Type: arrow.PrimitiveTypes.Float64},
		{Name: "timestamp", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	// Create a RecordBuilder for appending rows
	pool := memory.NewGoAllocator()
	builder := array.NewRecordBuilder(pool, schema)

	return &ArrowData{
		schema:      schema,
		builder:     builder,
		cheapestMap: make(map[string]CheapestEntry),
	}
}

// AddCandyPrice appends a new row of data to Arrow and updates the cheapestMap.
func (ad *ArrowData) AddCandyPrice(cp CandyPrice) {
	ad.mu.Lock()
	defer ad.mu.Unlock()

	// Append to Arrow builder
	ad.builder.Field(0).(*array.StringBuilder).Append(cp.CandyID)
	ad.builder.Field(1).(*array.StringBuilder).Append(cp.StoreID)
	ad.builder.Field(2).(*array.Float64Builder).Append(cp.Price)
	ad.builder.Field(3).(*array.Int64Builder).Append(cp.Timestamp)

	// Update cheapest map
	entry, exists := ad.cheapestMap[cp.CandyID]
	if !exists || cp.Price < entry.Price {
		ad.cheapestMap[cp.CandyID] = CheapestEntry{
			StoreID: cp.StoreID,
			Price:   cp.Price,
		}
	}
}

// GetCheapest returns the cheapest store + price for a given candyID.
func (ad *ArrowData) GetCheapest(candyID string) (CheapestEntry, bool) {
	ad.mu.Lock()
	defer ad.mu.Unlock()
	entry, ok := ad.cheapestMap[candyID]
	return entry, ok
}

// GetRecordCount returns how many rows are in the builder so far.
func (ad *ArrowData) GetRecordCount() int {
	ad.mu.Lock()
	defer ad.mu.Unlock()
	return ad.builder.Field(0).Len()
}

// BuildRecordBatch can be used to finalize and retrieve a record for advanced use.
func (ad *ArrowData) BuildRecordBatch() array.Record {
	ad.mu.Lock()
	defer ad.mu.Unlock()
	rec := ad.builder.NewRecord()
	return rec
}

// DebugPrint prints some debug info from the Arrow table (not strictly needed).
func (ad *ArrowData) DebugPrint() {
	ad.mu.Lock()
	defer ad.mu.Unlock()

	rec := ad.builder.NewRecord()
	defer rec.Release()

	fmt.Println("\n[DebugPrint] Current Arrow Data (RecordBatch):")

	// We'll iterate manually over rows/columns
	nRows := int(rec.NumRows())
	nCols := int(rec.NumCols())
	for row := 0; row < nRows; row++ {
		fmt.Printf("Row %d:\n", row)
		for col := 0; col < nCols; col++ {
			colName := rec.ColumnName(col)
			colData := rec.Column(col)

			// Attempt to read the row's value based on known field types
			switch c := colData.(type) {
			case *array.String:
				val := c.Value(row)
				fmt.Printf("  %s = %s\n", colName, val)
			case *array.Float64:
				val := c.Value(row)
				fmt.Printf("  %s = %.2f\n", colName, val)
			case *array.Int64:
				val := c.Value(row)
				fmt.Printf("  %s = %d\n", colName, val)
			default:
				fmt.Printf("  %s = (unhandled column type)\n", colName)
			}
		}
	}
	fmt.Printf("Number of rows: %d\n", nRows)
}
