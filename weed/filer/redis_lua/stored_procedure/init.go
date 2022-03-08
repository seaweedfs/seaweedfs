package stored_procedure

import (
	_ "embed"
	"github.com/go-redis/redis/v8"
)

func init() {
	InsertEntryScript = redis.NewScript(insertEntry)
	DeleteEntryScript = redis.NewScript(deleteEntry)
	DeleteFolderChildrenScript = redis.NewScript(deleteFolderChildren)
}

//go:embed insert_entry.lua
var insertEntry string
var InsertEntryScript *redis.Script

//go:embed delete_entry.lua
var deleteEntry string
var DeleteEntryScript *redis.Script

//go:embed delete_folder_children.lua
var deleteFolderChildren string
var DeleteFolderChildrenScript *redis.Script
