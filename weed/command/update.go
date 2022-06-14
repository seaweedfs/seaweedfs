package command

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/util"
	"golang.org/x/net/context/ctxhttp"
)

//copied from https://github.com/restic/restic/tree/master/internal/selfupdate

// Release collects data about a single release on GitHub.
type Release struct {
	Name        string    `json:"name"`
	TagName     string    `json:"tag_name"`
	Draft       bool      `json:"draft"`
	PreRelease  bool      `json:"prerelease"`
	PublishedAt time.Time `json:"published_at"`
	Assets      []Asset   `json:"assets"`

	Version string `json:"-"` // set manually in the code
}

// Asset is a file uploaded and attached to a release.
type Asset struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
	URL  string `json:"url"`
}

var (
	updateOpt UpdateOptions
)

type UpdateOptions struct {
	Output *string
}

func init() {
	updateOpt.Output = cmdUpdate.Flag.String("output", "weed", "Save the latest weed as `filename`")
	cmdUpdate.Run = runUpdate
}

var cmdUpdate = &Command{
	UsageLine: "update [-output=weed]",
	Short:     "get latest version from https://github.com/chrislusf/seaweedfs",
	Long:      `get latest version from https://github.com/chrislusf/seaweedfs`,
}

func runUpdate(cmd *Command, args []string) bool {
	weedPath := *updateOpt.Output
	if weedPath == "" {
		file, err := os.Executable()
		if err != nil {
			glog.Fatalf("unable to find executable:%s", err)
			return false
		}

		*updateOpt.Output = file
	}

	fi, err := os.Lstat(*updateOpt.Output)
	if err != nil {
		dirname := filepath.Dir(*updateOpt.Output)
		di, err := os.Lstat(dirname)
		if err != nil {
			glog.Fatalf("unable to find directory:%s", dirname)
			return false
		}
		if !di.Mode().IsDir() {
			glog.Fatalf("output parent path %v is not a directory, use --output to specify a different file path", dirname)
			return false
		}
	} else {
		if !fi.Mode().IsRegular() {
			glog.Fatalf("output path %v is not a normal file, use --output to specify a different file path", updateOpt.Output)
			return false
		}
	}

	glog.V(0).Infof("writing weed to %v\n", *updateOpt.Output)

	v, err := downloadLatestStableRelease(context.Background(), *updateOpt.Output)
	if err != nil {
		glog.Fatalf("unable to update weed: %v", err)
		return false
	}

	glog.V(0).Infof("successfully updated weed to version %v\n", v)

	return true
}

func downloadLatestStableRelease(ctx context.Context, target string) (version string, err error) {
	currentVersion := util.VERSION_NUMBER
	largeDiskSuffix := ""
	if util.VolumeSizeLimitGB == 8000 {
		largeDiskSuffix = "_large_disk"
	}

	rel, err := GitHubLatestRelease(ctx, "chrislusf", "seaweedfs")
	if err != nil {
		return "", err
	}

	if rel.Version == currentVersion {
		glog.V(0).Infof("weed is up to date\n")
		return currentVersion, nil
	}

	glog.V(0).Infof("latest version is %v\n", rel.Version)

	ext := "tar.gz"
	if runtime.GOOS == "windows" {
		ext = "zip"
	}

	suffix := fmt.Sprintf("%s_%s%s.%s", runtime.GOOS, runtime.GOARCH, largeDiskSuffix, ext)
	md5Filename := fmt.Sprintf("%s.md5", suffix)
	_, md5Val, err := getGithubDataFile(ctx, rel.Assets, md5Filename)
	if err != nil {
		return "", err
	}

	downloadFilename, buf, err := getGithubDataFile(ctx, rel.Assets, suffix)
	if err != nil {
		return "", err
	}

	md5Ctx := md5.New()
	md5Ctx.Write(buf)
	binaryMd5 := md5Ctx.Sum(nil)
	if hex.EncodeToString(binaryMd5) != string(md5Val[0:32]) {
		glog.Errorf("md5:'%s' '%s'", hex.EncodeToString(binaryMd5), string(md5Val[0:32]))
		err = errors.New("binary md5sum doesn't match")
		return "", err
	}

	err = extractToFile(buf, downloadFilename, target)
	if err != nil {
		return "", err
	}

	return rel.Version, nil
}

func (r Release) String() string {
	return fmt.Sprintf("%v %v, %d assets",
		r.TagName,
		r.PublishedAt.Local().Format("2006-01-02 15:04:05"),
		len(r.Assets))
}

const githubAPITimeout = 30 * time.Second

// githubError is returned by the GitHub API, e.g. for rate-limiting.
type githubError struct {
	Message string
}

// GitHubLatestRelease uses the GitHub API to get information about the latest
// release of a repository.
func GitHubLatestRelease(ctx context.Context, owner, repo string) (Release, error) {
	ctx, cancel := context.WithTimeout(ctx, githubAPITimeout)
	defer cancel()

	url := fmt.Sprintf("https://api.github.com/repos/%s/%s/releases/latest", owner, repo)
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return Release{}, err
	}

	// pin API version 3
	req.Header.Set("Accept", "application/vnd.github.v3+json")

	res, err := ctxhttp.Do(ctx, http.DefaultClient, req)
	if err != nil {
		return Release{}, err
	}

	if res.StatusCode != http.StatusOK {
		content := res.Header.Get("Content-Type")
		if strings.Contains(content, "application/json") {
			// try to decode error message
			var msg githubError
			jerr := json.NewDecoder(res.Body).Decode(&msg)
			if jerr == nil {
				return Release{}, fmt.Errorf("unexpected status %v (%v) returned, message:\n  %v", res.StatusCode, res.Status, msg.Message)
			}
		}

		_ = res.Body.Close()
		return Release{}, fmt.Errorf("unexpected status %v (%v) returned", res.StatusCode, res.Status)
	}

	buf, err := ioutil.ReadAll(res.Body)
	if err != nil {
		_ = res.Body.Close()
		return Release{}, err
	}

	err = res.Body.Close()
	if err != nil {
		return Release{}, err
	}

	var release Release
	err = json.Unmarshal(buf, &release)
	if err != nil {
		return Release{}, err
	}

	if release.TagName == "" {
		return Release{}, errors.New("tag name for latest release is empty")
	}

	release.Version = release.TagName

	return release, nil
}

func getGithubData(ctx context.Context, url string) ([]byte, error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	// request binary data
	req.Header.Set("Accept", "application/octet-stream")

	res, err := ctxhttp.Do(ctx, http.DefaultClient, req)
	if err != nil {
		return nil, err
	}

	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status %v (%v) returned", res.StatusCode, res.Status)
	}

	buf, err := ioutil.ReadAll(res.Body)
	if err != nil {
		_ = res.Body.Close()
		return nil, err
	}

	err = res.Body.Close()
	if err != nil {
		return nil, err
	}

	return buf, nil
}

func getGithubDataFile(ctx context.Context, assets []Asset, suffix string) (filename string, data []byte, err error) {
	var url string
	for _, a := range assets {
		if strings.HasSuffix(a.Name, suffix) {
			url = a.URL
			filename = a.Name
			break
		}
	}

	if url == "" {
		return "", nil, fmt.Errorf("unable to find file with suffix %v", suffix)
	}

	glog.V(0).Infof("download %v\n", filename)
	data, err = getGithubData(ctx, url)
	if err != nil {
		return "", nil, err
	}

	return filename, data, nil
}

func extractToFile(buf []byte, filename, target string) error {
	var rd io.Reader = bytes.NewReader(buf)

	switch filepath.Ext(filename) {
	case ".gz":
		gr, err := gzip.NewReader(rd)
		if err != nil {
			return err
		}
		defer gr.Close()
		trd := tar.NewReader(gr)
		hdr, terr := trd.Next()
		if terr != nil {
			glog.Errorf("uncompress file(%s) failed:%s", hdr.Name, terr)
			return terr
		}
		rd = trd
	case ".zip":
		zrd, err := zip.NewReader(bytes.NewReader(buf), int64(len(buf)))
		if err != nil {
			return err
		}

		if len(zrd.File) != 1 {
			return errors.New("ZIP archive contains more than one file")
		}

		file, err := zrd.File[0].Open()
		if err != nil {
			return err
		}

		defer func() {
			_ = file.Close()
		}()

		rd = file
	}

	// Write everything to a temp file
	dir := filepath.Dir(target)
	new, err := ioutil.TempFile(dir, "weed")
	if err != nil {
		return err
	}

	n, err := io.Copy(new, rd)
	if err != nil {
		_ = new.Close()
		_ = os.Remove(new.Name())
		return err
	}
	if err = new.Sync(); err != nil {
		return err
	}
	if err = new.Close(); err != nil {
		return err
	}

	mode := os.FileMode(0755)
	// attempt to find the original mode
	if fi, err := os.Lstat(target); err == nil {
		mode = fi.Mode()
	}

	// Remove the original binary.
	if err := removeWeedBinary(dir, target); err != nil {
		return err
	}

	// Rename the temp file to the final location atomically.
	if err := os.Rename(new.Name(), target); err != nil {
		return err
	}

	glog.V(0).Infof("saved %d bytes in %v\n", n, target)
	return os.Chmod(target, mode)
}

// Rename (rather than remove) the running version. The running binary will be locked
// on Windows and cannot be removed while still executing.
func removeWeedBinary(dir, target string) error {
	if runtime.GOOS == "linux" {
		return nil
	}
	backup := filepath.Join(dir, filepath.Base(target)+".bak")
	if _, err := os.Stat(backup); err == nil {
		_ = os.Remove(backup)
	}
	if err := os.Rename(target, backup); err != nil {
		return fmt.Errorf("unable to rename target file: %v", err)
	}
	return nil
}
