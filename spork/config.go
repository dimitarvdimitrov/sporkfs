package spork

type Config struct {
	InventoryLocation, DataLocation, CacheLocation string
	Peers                                          []string
	ThisPeer                                       string
}
