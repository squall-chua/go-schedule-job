package storetest_test

import (
	"testing"

	gs "github.com/squallchua/goschedule"
	"github.com/squallchua/goschedule/memstore"
	"github.com/squallchua/goschedule/storetest"
)

func TestMemStore_Conformance(t *testing.T) {
	storetest.Run(t, func(_ *testing.T) (gs.Store, func()) {
		return memstore.New(), func() {}
	})
}
