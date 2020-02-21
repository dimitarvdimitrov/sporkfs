package storage

type Source interface {
	GetState() []byte
}

type snapshoter struct {
	inventory, fileIndex, confState Source
}
