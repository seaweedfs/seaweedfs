package shell

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func init() {
	Commands = append(Commands, &commandFsMetaToken{})
}

type commandFsMetaToken struct {
}

func (c *commandFsMetaToken) Name() string {
	return "fs.meta.token"
}

func (c *commandFsMetaToken) Help() string {
	return `mint a read-only token for metadata subscriptions under path prefixes

	fs.meta.token -prefix=/buckets/uploads -expire=720h
	fs.meta.token -prefix=/buckets/a,/buckets/b

	The token is signed with jwt.filer_signing.subscribe.key from security.toml.
	Send it as "authorization: Bearer <token>" gRPC metadata on SubscribeMetadata
	or SubscribeLocalMetadata; the filer then sends only events whose paths are
	all under one of the prefixes.

`
}

func (c *commandFsMetaToken) HasTag(CommandTag) bool {
	return false
}

func (c *commandFsMetaToken) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {
	tokenCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	prefixes := tokenCommand.String("prefix", "", "comma-separated absolute path prefixes")
	expire := tokenCommand.Duration("expire", 24*time.Hour, "token lifetime")
	if err = tokenCommand.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}

	key := util.GetViper().GetString("jwt.filer_signing.subscribe.key")
	if key == "" {
		return fmt.Errorf("jwt.filer_signing.subscribe.key is not set")
	}
	expireSeconds := int(expire.Seconds())
	if expireSeconds <= 0 {
		return fmt.Errorf("-expire must be at least one second")
	}
	var prefixList []string
	for _, p := range strings.Split(*prefixes, ",") {
		if p = strings.TrimSpace(p); p != "" {
			prefixList = append(prefixList, p)
		}
	}
	token := security.GenJwtForMetadataSubscription(security.SigningKey(key), expireSeconds, prefixList)
	if token == "" {
		return fmt.Errorf("-prefix needs one or more absolute paths")
	}
	fmt.Fprintln(writer, token)
	return nil
}
