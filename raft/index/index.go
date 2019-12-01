package index

const (
	redundancy = 2
)

var (
	minusOne = -1
	maxId    = uint64(minusOne)
)

func FindPeers(peerList []string, fileId uint64) []string {
	numPeers := uint64(len(peerList))

	idsPerPeer := maxId / numPeers
	peersWithFile := make([]string, 0, redundancy)

	for i := uint64(0); i < redundancy; i++ {
		peerIdx := (i + fileId/idsPerPeer) % numPeers
		peersWithFile = append(peersWithFile, peerList[peerIdx])
	}

	return peersWithFile
}
