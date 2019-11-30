package index

var (
	minusOne = -1
	maxId    = uint64(minusOne)
)

func GetLocation(peerList []string, id uint64) int {
	numPeers := uint64(len(peerList))
	perBucket := maxId / numPeers

	return int((id / perBucket) % numPeers)
}
