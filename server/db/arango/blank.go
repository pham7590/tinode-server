//go:build !arango
// +build !arango

// This file is needed for conditional compilation. It's used when
// the build tag 'arango' is not defined. Otherwise the adapter.go
// is compiled.

package arango
