package kv

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	tr "github.com/seaweedfs/seaweedfs/weed/storage/blockvol/testrunner"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/testrunner/actions"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/testrunner/infra"
)

// kvAssign calls GET /dir/assign on the master to get a file ID.
// Params: master_url (or env var), count (default 1).
// Sets save_as=fid, save_as_url, save_as_public_url.
func kvAssign(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	node, err := actions.GetNode(actx, act.Node)
	if err != nil {
		return nil, fmt.Errorf("kv_assign: %w", err)
	}
	masterURL := act.Params["master_url"]
	if masterURL == "" {
		masterURL = actx.Vars["master_url"]
	}
	if masterURL == "" {
		return nil, fmt.Errorf("kv_assign: master_url param or var required")
	}
	count := act.Params["count"]
	if count == "" {
		count = "1"
	}

	cmd := fmt.Sprintf("curl -s '%s/dir/assign?count=%s' 2>/dev/null", masterURL, count)
	stdout, _, code, err := node.Run(ctx, cmd)
	if err != nil || code != 0 {
		return nil, fmt.Errorf("kv_assign: curl failed: code=%d err=%v", code, err)
	}

	var resp struct {
		Fid       string `json:"fid"`
		URL       string `json:"url"`
		PublicURL string `json:"publicUrl"`
		Count     int    `json:"count"`
		Error     string `json:"error"`
	}
	if err := json.Unmarshal([]byte(stdout), &resp); err != nil {
		return nil, fmt.Errorf("kv_assign: parse response: %w (body: %s)", err, stdout)
	}
	if resp.Error != "" {
		return nil, fmt.Errorf("kv_assign: %s", resp.Error)
	}
	if resp.Fid == "" {
		return nil, fmt.Errorf("kv_assign: empty fid in response: %s", stdout)
	}

	actx.Log("  assigned fid=%s url=%s", resp.Fid, resp.URL)
	if act.SaveAs != "" {
		actx.Vars[act.SaveAs+"_fid"] = resp.Fid
		actx.Vars[act.SaveAs+"_url"] = resp.URL
		actx.Vars[act.SaveAs+"_public_url"] = resp.PublicURL
	}
	return map[string]string{"value": resp.Fid}, nil
}

// kvUpload uploads a file to a volume server using the assigned fid.
// Params: url (volume server), fid, file (path) OR data (inline string) OR size (generate random).
// Sets save_as=md5.
func kvUpload(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	node, err := actions.GetNode(actx, act.Node)
	if err != nil {
		return nil, fmt.Errorf("kv_upload: %w", err)
	}
	url := act.Params["url"]
	fid := act.Params["fid"]
	if url == "" || fid == "" {
		return nil, fmt.Errorf("kv_upload: url and fid params required")
	}

	var cmd string
	if file := act.Params["file"]; file != "" {
		// Upload existing file.
		cmd = fmt.Sprintf("md5sum %s | awk '{print $1}' && curl -s -F file=@%s 'http://%s/%s' 2>/dev/null",
			file, file, url, fid)
	} else if size := act.Params["size"]; size != "" {
		// Generate random data of given size, upload it.
		cmd = fmt.Sprintf("TF=/tmp/sw-kv-upload-$$-$RANDOM.dat && dd if=/dev/urandom bs=%s count=1 2>/dev/null | tee $TF | md5sum | awk '{print $1}' && curl -s -F file=@$TF 'http://%s/%s' 2>/dev/null && rm -f $TF",
			size, url, fid)
	} else if data := act.Params["data"]; data != "" {
		// Upload inline string data.
		cmd = fmt.Sprintf("TF=/tmp/sw-kv-upload-$$-$RANDOM.dat && echo -n '%s' | tee $TF | md5sum | awk '{print $1}' && curl -s -F file=@$TF 'http://%s/%s' 2>/dev/null && rm -f $TF",
			data, url, fid)
	} else {
		return nil, fmt.Errorf("kv_upload: file, data, or size param required")
	}

	stdout, _, code, err := node.Run(ctx, cmd)
	if err != nil || code != 0 {
		return nil, fmt.Errorf("kv_upload: code=%d err=%v", code, err)
	}

	lines := strings.Split(strings.TrimSpace(stdout), "\n")
	md5 := ""
	if len(lines) > 0 {
		md5 = strings.TrimSpace(lines[0])
	}

	actx.Log("  uploaded fid=%s md5=%s", fid, md5)
	return map[string]string{"value": md5}, nil
}

// kvDownload downloads a file by fid and returns its md5.
// Params: url (volume server), fid.
// Sets save_as=md5.
func kvDownload(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	node, err := actions.GetNode(actx, act.Node)
	if err != nil {
		return nil, fmt.Errorf("kv_download: %w", err)
	}
	url := act.Params["url"]
	fid := act.Params["fid"]
	if url == "" || fid == "" {
		return nil, fmt.Errorf("kv_download: url and fid params required")
	}

	cmd := fmt.Sprintf("curl -s 'http://%s/%s' 2>/dev/null | md5sum | awk '{print $1}'", url, fid)
	stdout, _, code, err := node.Run(ctx, cmd)
	if err != nil || code != 0 {
		return nil, fmt.Errorf("kv_download: code=%d err=%v", code, err)
	}

	md5 := strings.TrimSpace(stdout)
	actx.Log("  downloaded fid=%s md5=%s", fid, md5)
	return map[string]string{"value": md5}, nil
}

// kvVerify is a convenience action: assign + upload + download + assert md5 match.
// Params: master_url, size (default "1K"), node.
func kvVerify(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	node, err := actions.GetNode(actx, act.Node)
	if err != nil {
		return nil, fmt.Errorf("kv_verify: %w", err)
	}
	masterURL := act.Params["master_url"]
	if masterURL == "" {
		masterURL = actx.Vars["master_url"]
	}
	if masterURL == "" {
		return nil, fmt.Errorf("kv_verify: master_url required")
	}
	size := act.Params["size"]
	if size == "" {
		size = "1K"
	}

	// All-in-one: assign, upload random data, download, verify md5.
	cmd := fmt.Sprintf(`
ASSIGN=$(curl -s '%s/dir/assign' 2>/dev/null)
FID=$(echo "$ASSIGN" | python3 -c "import sys,json; print(json.load(sys.stdin)['fid'])" 2>/dev/null || echo "$ASSIGN" | grep -o '"fid":"[^"]*"' | cut -d'"' -f4)
URL=$(echo "$ASSIGN" | python3 -c "import sys,json; print(json.load(sys.stdin)['url'])" 2>/dev/null || echo "$ASSIGN" | grep -o '"url":"[^"]*"' | cut -d'"' -f4)
if [ -z "$FID" ] || [ -z "$URL" ]; then echo "FAIL: assign failed: $ASSIGN"; exit 1; fi
dd if=/dev/urandom bs=%s count=1 2>/dev/null > /tmp/sw-kv-verify-$$.dat
UPLOAD_MD5=$(md5sum /tmp/sw-kv-verify-$$.dat | awk '{print $1}')
curl -s -F file=@/tmp/sw-kv-verify-$$.dat "http://$URL/$FID" >/dev/null 2>&1
DOWNLOAD_MD5=$(curl -s "http://$URL/$FID" 2>/dev/null | md5sum | awk '{print $1}')
rm -f /tmp/sw-kv-verify-$$.dat
if [ "$UPLOAD_MD5" = "$DOWNLOAD_MD5" ]; then
  echo "OK fid=$FID upload_md5=$UPLOAD_MD5 download_md5=$DOWNLOAD_MD5"
else
  echo "FAIL fid=$FID upload_md5=$UPLOAD_MD5 download_md5=$DOWNLOAD_MD5"
  exit 1
fi
`, masterURL, size)

	stdout, stderr, code, err := node.Run(ctx, cmd)
	if err != nil || code != 0 {
		return nil, fmt.Errorf("kv_verify: FAIL: stdout=%s stderr=%s code=%d err=%v", stdout, stderr, code, err)
	}
	actx.Log("  %s", strings.TrimSpace(stdout))
	return map[string]string{"value": strings.TrimSpace(stdout)}, nil
}

// kvDelete deletes a file by fid.
// Params: url, fid.
func kvDelete(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	node, err := actions.GetNode(actx, act.Node)
	if err != nil {
		return nil, fmt.Errorf("kv_delete: %w", err)
	}
	url := act.Params["url"]
	fid := act.Params["fid"]
	if url == "" || fid == "" {
		return nil, fmt.Errorf("kv_delete: url and fid params required")
	}

	cmd := fmt.Sprintf("curl -s -X DELETE 'http://%s/%s' 2>/dev/null", url, fid)
	stdout, _, code, err := node.Run(ctx, cmd)
	if err != nil || code != 0 {
		return nil, fmt.Errorf("kv_delete: code=%d err=%v stdout=%s", code, err, stdout)
	}
	actx.Log("  deleted fid=%s", fid)
	return nil, nil
}

// startWeedFiler starts a weed filer process on the given node.
// Params: port (default 8888), master, dir, node.
func startWeedFiler(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	node, err := actions.GetNode(actx, act.Node)
	if err != nil {
		return nil, fmt.Errorf("start_weed_filer: %w", err)
	}
	port := act.Params["port"]
	if port == "" {
		port = "8888"
	}
	master := act.Params["master"]
	if master == "" {
		return nil, fmt.Errorf("start_weed_filer: master param required")
	}
	dir := act.Params["dir"]
	if dir == "" {
		dir = "/tmp/sw-weed-filer"
	}

	node.RunRoot(ctx, fmt.Sprintf("mkdir -p %s", dir))

	cmd := fmt.Sprintf("sh -c 'nohup %sweed filer -port=%s -master=%s -defaultStoreDir=%s </dev/null >%s/filer.log 2>&1 & echo $!'",
		tr.UploadBasePath, port, master, dir, dir)
	stdout, stderr, code, err := node.RunRoot(ctx, cmd)
	if err != nil || code != 0 {
		return nil, fmt.Errorf("start_weed_filer: code=%d stderr=%s err=%v", code, stderr, err)
	}

	pid := strings.TrimSpace(stdout)
	actx.Log("  weed filer started on port %s (PID %s)", port, pid)

	// Wait for filer to be ready.
	readyCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for {
		select {
		case <-readyCtx.Done():
			return map[string]string{"value": pid}, nil // return PID even if not ready
		case <-time.After(1 * time.Second):
			checkCmd := fmt.Sprintf("curl -s -o /dev/null -w '%%{http_code}' http://localhost:%s/ 2>/dev/null", port)
			out, _, _, _ := node.Run(readyCtx, checkCmd)
			if strings.TrimSpace(out) == "200" {
				actx.Log("  filer ready on port %s", port)
				return map[string]string{"value": pid}, nil
			}
		}
	}
}

// filerPut uploads a file to the filer.
// Params: filer_url, path (filer path), file (local path) OR data (inline).
func filerPut(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	node, err := actions.GetNode(actx, act.Node)
	if err != nil {
		return nil, fmt.Errorf("filer_put: %w", err)
	}
	filerURL := act.Params["filer_url"]
	if filerURL == "" {
		filerURL = actx.Vars["filer_url"]
	}
	path := act.Params["path"]
	if filerURL == "" || path == "" {
		return nil, fmt.Errorf("filer_put: filer_url and path required")
	}

	var cmd string
	if file := act.Params["file"]; file != "" {
		cmd = fmt.Sprintf("curl -s -F file=@%s '%s%s' 2>/dev/null", file, filerURL, path)
	} else if data := act.Params["data"]; data != "" {
		cmd = fmt.Sprintf("TF=/tmp/sw-filer-put-$$-$RANDOM.dat && echo -n '%s' > $TF && curl -s -F file=@$TF '%s%s' 2>/dev/null && rm -f $TF",
			data, filerURL, path)
	} else {
		return nil, fmt.Errorf("filer_put: file or data param required")
	}

	stdout, _, code, err := node.Run(ctx, cmd)
	if err != nil || code != 0 {
		return nil, fmt.Errorf("filer_put: code=%d err=%v stdout=%s", code, err, stdout)
	}
	actx.Log("  filer PUT %s", path)
	return map[string]string{"value": stdout}, nil
}

// filerGet downloads a file from the filer and returns its md5.
// Params: filer_url, path.
func filerGet(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	node, err := actions.GetNode(actx, act.Node)
	if err != nil {
		return nil, fmt.Errorf("filer_get: %w", err)
	}
	filerURL := act.Params["filer_url"]
	if filerURL == "" {
		filerURL = actx.Vars["filer_url"]
	}
	path := act.Params["path"]
	if filerURL == "" || path == "" {
		return nil, fmt.Errorf("filer_get: filer_url and path required")
	}

	cmd := fmt.Sprintf("curl -s '%s%s' 2>/dev/null | md5sum | awk '{print $1}'", filerURL, path)
	stdout, _, code, err := node.Run(ctx, cmd)
	if err != nil || code != 0 {
		return nil, fmt.Errorf("filer_get: code=%d err=%v", code, err)
	}
	md5 := strings.TrimSpace(stdout)
	actx.Log("  filer GET %s md5=%s", path, md5)
	return map[string]string{"value": md5}, nil
}

// filerDelete deletes a file from the filer.
// Params: filer_url, path.
func filerDelete(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	node, err := actions.GetNode(actx, act.Node)
	if err != nil {
		return nil, fmt.Errorf("filer_delete: %w", err)
	}
	filerURL := act.Params["filer_url"]
	if filerURL == "" {
		filerURL = actx.Vars["filer_url"]
	}
	path := act.Params["path"]
	if filerURL == "" || path == "" {
		return nil, fmt.Errorf("filer_delete: filer_url and path required")
	}

	cmd := fmt.Sprintf("curl -s -X DELETE '%s%s' 2>/dev/null", filerURL, path)
	stdout, _, code, err := node.Run(ctx, cmd)
	if err != nil || code != 0 {
		return nil, fmt.Errorf("filer_delete: code=%d err=%v stdout=%s", code, err, stdout)
	}
	actx.Log("  filer DELETE %s", path)
	return nil, nil
}

// Ensure infra import is used (for getNode via actions package).
var _ = (*infra.Node)(nil)
