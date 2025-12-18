// Package inventory provides readers for AWS S3 Inventory CSV files.
package inventory

// Record represents a single object from the inventory.
type Record struct {
	Key  string
	Size uint64
}

// Reader is the interface for reading inventory records.
type Reader interface {
	// Read reads the next record. Returns io.EOF when done.
	Read() (Record, error)
	// Close releases resources.
	Close() error
}
