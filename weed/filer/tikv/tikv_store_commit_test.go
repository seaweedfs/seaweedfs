//go:build tikv

package tikv

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	tikverr "github.com/tikv/client-go/v2/error"

	"github.com/seaweedfs/seaweedfs/weed/filer"
)

// A commit TiKV refuses because another writer committed one of the
// transaction's keys after it began is a transaction conflict, however the
// client wrapped it; the message keeps TiKV's details.
func TestCommitErrorMarksWriteConflicts(t *testing.T) {
	conflict := tikverr.NewErrWriteConflictWithArgs(470569654678716421, 470569654678716448, 0, []byte("k"), 0)
	for _, err := range []error{conflict, fmt.Errorf("commit: %w", conflict)} {
		got := commitError(err)
		if !errors.Is(got, filer.ErrTransactionConflict) {
			t.Errorf("commitError(%v) = %v, want a transaction conflict", err, got)
		}
		if !strings.Contains(got.Error(), "write conflict") || !strings.Contains(got.Error(), "470569654678716448") {
			t.Errorf("commitError(%v) = %q, want TiKV's details kept", err, got)
		}
	}

	other := errors.New("region is unavailable")
	if got := commitError(other); got != other {
		t.Errorf("commitError(%v) = %v, want it unchanged", other, got)
	}
	if got := commitError(nil); got != nil {
		t.Errorf("commitError(nil) = %v, want nil", got)
	}
}
