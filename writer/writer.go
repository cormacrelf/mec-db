package writer

import (
	"github.com/jmhodges/levigo"
	"github.com/cormacrelf/mec-db/peers"
	// "github.com/cormacrelf/mec-db/vclock"
)

// handles
// - receiving node as coordinator on write
// - writing simple PUT from coordinator node
// - responding to PUT requests
// - fire PUT to W nodes
// - confirm success by examining replies
// - actual leveldb writes
// - VClocks at every stage

type Writer struct {
	db *levigo.DB
	pl *peers.PeerList
}

func Create(db *levigo.DB, pl *peers.PeerList) *Writer {
	return &Writer{
		db: db,
		pl: pl,
	}
}

