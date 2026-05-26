package storetest_test

import (
	"testing"

	gs "github.com/squall-chua/go-schedule-job"
	"github.com/squall-chua/go-schedule-job/memstore"
	"github.com/squall-chua/go-schedule-job/storetest"
)

func TestMemStore_Conformance(t *testing.T) {
	storetest.Run(t, func(_ *testing.T) (gs.Store, func()) {
		return memstore.New(), func() {}
	})
}
