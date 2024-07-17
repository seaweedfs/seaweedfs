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
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"golang.org/x/net/context/ctxhttp"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
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

const githubAPITimeout = 30 * time.Second

// githubError is returned by the GitHub API, e.g. for rate-limiting.
type githubError struct {
	Message string
}

// default version is not full version
var isFullVersion = false

var (
	updateOpt UpdateOptions
)

type UpdateOptions struct {
	dir     *string
	name    *string
	Version *string
}

func init() {
	path, _ := os.Executable()
	_, name := filepath.Split(path)
	updateOpt.dir = cmdUpdate.Flag.String("dir", filepath.Dir(path), "directory to save new weed.")
	updateOpt.name = cmdUpdate.Flag.String("name", name, "name of new weed. On windows, name shouldn't be same to the original name.")
	updateOpt.Version = cmdUpdate.Flag.String("version", "0", "specific version of weed you want to download. If not specified, get the latest version.")
	cmdUpdate.Run = runUpdate
}

var cmdUpdate = &Command{
	UsageLine: "update [-dir=/path/to/dir] [-name=name] [-version=x.xx]",
	Short:     "get latest or specific version from https://github.com/seaweedfs/seaweedfs",
	Long:      `get latest or specific version from https://github.com/seaweedfs/seaweedfs`,
}

func runUpdate(cmd *Command, args []string) bool {
	path, _ := os.Executable()
	_, name := filepath.Split(path)

	if *updateOpt.dir != "" {
		if err := util.TestFolderWritable(util.ResolvePath(*updateOpt.dir)); err != nil {
			glog.Fatalf("Check Folder(-dir) Writable %s : %s", *updateOpt.dir, err)
			return false
		}
	} else {
		*updateOpt.dir = filepath.Dir(path)
	}

	if *updateOpt.name == "" {
		*updateOpt.name = name
	}

	target := filepath.Join(*updateOpt.dir, *updateOpt.name)

	if runtime.GOOS == "windows" {
		if target == path {
			glog.Fatalf("On windows, name of the new weed shouldn't be same to the original name.")
			return false
		}
	}

	glog.V(0).Infof("new weed will be saved to %s", target)

	_, err := downloadRelease(context.Background(), target, *updateOpt.Version)
	if err != nil {
		glog.Errorf("unable to download weed: %v", err)
		return false
	}
	return true
}

func downloadRelease(ctx context.Context, target string, ver string) (version string, err error) {
	currentVersion := util.VERSION_NUMBER
	rel, err := GitHubLatestRelease(ctx, ver, "seaweedfs", "seaweedfs")
	if err != nil {
		return "", err
	}

	if rel.Version == currentVersion {
		if ver == "0" {
			glog.V(0).Infof("weed is up to date")
		} else {
			glog.V(0).Infof("no need to download the same version of weed ")
		}
		return currentVersion, nil
	}

	glog.V(0).Infof("download version: %s", rel.Version)

	largeDiskSuffix := ""
	if util.VolumeSizeLimitGB == 8000 {
		largeDiskSuffix = "_large_disk"
	}

	fullSuffix := ""
	if isFullVersion {
		fullSuffix = "_full"
	}

	ext := "tar.gz"
	if runtime.GOOS == "windows" {
		ext = "zip"
	}

	suffix := fmt.Sprintf("%s_%s%s%s.%s", runtime.GOOS, runtime.GOARCH, fullSuffix, largeDiskSuffix, ext)
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
		err = fmt.Errorf("binary md5sum doesn't match")
		return "", err
	}

	err = extractToFile(buf, downloadFilename, target)
	if err != nil {
		return "", err
	} else {
		glog.V(0).Infof("successfully updated weed to version %v\n", rel.Version)
	}

	return rel.Version, nil
}

// GitHubLatestRelease uses the GitHub API to get information about the specific
// release of a repository.
func GitHubLatestRelease(ctx context.Context, ver string, owner, repo string) (Release, error) {
	ctx, cancel := context.WithTimeout(ctx, githubAPITimeout)
	defer cancel()

	url := fmt.Sprintf("https://api.github.com/repos/%s/%s/releases", owner, repo)
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
	defer util_http.CloseResponse(res)

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

		return Release{}, fmt.Errorf("unexpected status %v (%v) returned", res.StatusCode, res.Status)
	}

	buf, err := io.ReadAll(res.Body)
	if err != nil {
		return Release{}, err
	}

	var release Release
	var releaseList []Release
	err = json.Unmarshal(buf, &releaseList)
	if err != nil {
		return Release{}, err
	}
	if ver == "0" {
		release = releaseList[0]
		glog.V(0).Infof("latest version is %v\n", release.TagName)
	} else {
		for _, r := range releaseList {
			if r.TagName == ver {
				release = r
				break
			}
		}
	}

	if release.TagName == "" {
		return Release{}, fmt.Errorf("can not find the specific version")
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
	defer util_http.CloseResponse(res)

	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status %v (%v) returned", res.StatusCode, res.Status)
	}

	buf, err := io.ReadAll(res.Body)
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
			return fmt.Errorf("ZIP archive contains more than one file")
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
	new, err := os.CreateTemp(dir, "weed")
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

	// Rename the temp file to the final location atomically.
	if err := os.Rename(new.Name(), target); err != nil {
		return err
	}

	glog.V(0).Infof("saved %d bytes in %v\n", n, target)
	return os.Chmod(target, mode)
}
