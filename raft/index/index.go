package index

var (
	minusOne = -1
	maxId    = uint64(minusOne)
)

func FindPeer(peerList []string, fileId uint64) string {
	numPeers := uint64(len(peerList))
	perBucket := maxId / numPeers
	peerIdx := (fileId / perBucket) % numPeers
	return peerList[peerIdx]
}
