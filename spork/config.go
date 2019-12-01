package spork

type Config struct {
	InventoryLocation, DataLocation string
	Peers                           []string
	ThisPeer                        string
}
