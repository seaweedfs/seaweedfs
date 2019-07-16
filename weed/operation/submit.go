package operation

import (
	"bytes"
	"google.golang.org/grpc"
	"io"
	"mime"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/security"
)

type FilePart struct {
	Reader      io.Reader
	FileName    string
	FileSize    int64
	MimeType    string
	ModTime     int64 //in seconds
	Replication string
	Collection  string
	DataCenter  string
	Ttl         string
	Server      string //this comes from assign result
	Fid         string //this comes from assign result, but customizable
}

type SubmitResult struct {
	FileName string `json:"fileName,omitempty"`
	FileUrl  string `json:"fileUrl,omitempty"`
	Fid      string `json:"fid,omitempty"`
	Size     uint32 `json:"size,omitempty"`
	Error    string `json:"error,omitempty"`
}

func SubmitFiles(master string, grpcDialOption grpc.DialOption, files []FilePart,
	replication string, collection string, dataCenter string, ttl string, maxMB int) ([]SubmitResult, error) {
	results := make([]SubmitResult, len(files))
	for index, file := range files {
		results[index].FileName = file.FileName
	}
	ar := &VolumeAssignRequest{
		Count:       uint64(len(files)),
		Replication: replication,
		Collection:  collection,
		DataCenter:  dataCenter,
		Ttl:         ttl,
	}
	ret, err := Assign(master, grpcDialOption, ar)
	if err != nil {
		for index, _ := range files {
			results[index].Error = err.Error()
		}
		return results, err
	}
	for index, file := range files {
		file.Fid = ret.Fid
		if index > 0 {
			file.Fid = file.Fid + "_" + strconv.Itoa(index)
		}
		file.Server = ret.Url
		file.Replication = replication
		file.Collection = collection
		file.DataCenter = dataCenter
		results[index].Size, err = file.Upload(maxMB, master, ret.Auth, grpcDialOption)
		if err != nil {
			results[index].Error = err.Error()
		}
		results[index].Fid = file.Fid
		results[index].FileUrl = ret.PublicUrl + "/" + file.Fid
	}
	return results, nil
}

func NewFileParts(fullPathFilenames []string) (ret []FilePart, err error) {
	ret = make([]FilePart, len(fullPathFilenames))
	for index, file := range fullPathFilenames {
		if ret[index], err = newFilePart(file); err != nil {
			return
		}
	}
	return
}
func newFilePart(fullPathFilename string) (ret FilePart, err error) {
	fh, openErr := os.Open(fullPathFilename)
	if openErr != nil {
		glog.V(0).Info("Failed to open file: ", fullPathFilename)
		return ret, openErr
	}
	ret.Reader = fh

	fi, fiErr := fh.Stat()
	if fiErr != nil {
		glog.V(0).Info("Failed to stat file:", fullPathFilename)
		return ret, fiErr
	}
	ret.ModTime = fi.ModTime().UTC().Unix()
	ret.FileSize = fi.Size()
	ext := strings.ToLower(path.Ext(fullPathFilename))
	ret.FileName = fi.Name()
	if ext != "" {
		ret.MimeType = mime.TypeByExtension(ext)
	}

	return ret, nil
}

func (fi FilePart) Upload(maxMB int, master string, jwt security.EncodedJwt, grpcDialOption grpc.DialOption) (retSize uint32, err error) {
	fileUrl := "http://" + fi.Server + "/" + fi.Fid
	if fi.ModTime != 0 {
		fileUrl += "?ts=" + strconv.Itoa(int(fi.ModTime))
	}
	if closer, ok := fi.Reader.(io.Closer); ok {
		defer closer.Close()
	}
	baseName := path.Base(fi.FileName)
	if maxMB > 0 && fi.FileSize > int64(maxMB*1024*1024) {
		chunkSize := int64(maxMB * 1024 * 1024)
		chunks := fi.FileSize/chunkSize + 1
		cm := ChunkManifest{
			Name:   baseName,
			Size:   fi.FileSize,
			Mime:   fi.MimeType,
			Chunks: make([]*ChunkInfo, 0, chunks),
		}

		var ret *AssignResult
		var id string
		if fi.DataCenter != "" {
			ar := &VolumeAssignRequest{
				Count:       uint64(chunks),
				Replication: fi.Replication,
				Collection:  fi.Collection,
				Ttl:         fi.Ttl,
			}
			ret, err = Assign(master, grpcDialOption, ar)
			if err != nil {
				return
			}
		}
		for i := int64(0); i < chunks; i++ {
			if fi.DataCenter == "" {
				ar := &VolumeAssignRequest{
					Count:       1,
					Replication: fi.Replication,
					Collection:  fi.Collection,
					Ttl:         fi.Ttl,
				}
				ret, err = Assign(master, grpcDialOption, ar)
				if err != nil {
					// delete all uploaded chunks
					cm.DeleteChunks(master, grpcDialOption)
					return
				}
				id = ret.Fid
			} else {
				id = ret.Fid
				if i > 0 {
					id += "_" + strconv.FormatInt(i, 10)
				}
			}
			fileUrl := "http://" + ret.Url + "/" + id
			count, e := upload_one_chunk(
				baseName+"-"+strconv.FormatInt(i+1, 10),
				io.LimitReader(fi.Reader, chunkSize),
				master, fileUrl,
				ret.Auth)
			if e != nil {
				// delete all uploaded chunks
				cm.DeleteChunks(master, grpcDialOption)
				return 0, e
			}
			cm.Chunks = append(cm.Chunks,
				&ChunkInfo{
					Offset: i * chunkSize,
					Size:   int64(count),
					Fid:    id,
				},
			)
			retSize += count
		}
		err = upload_chunked_file_manifest(fileUrl, &cm, jwt)
		if err != nil {
			// delete all uploaded chunks
			cm.DeleteChunks(master, grpcDialOption)
		}
	} else {
		ret, e := Upload(fileUrl, baseName, fi.Reader, false, fi.MimeType, nil, jwt)
		if e != nil {
			return 0, e
		}
		return ret.Size, e
	}
	return
}

func upload_one_chunk(filename string, reader io.Reader, master,
	fileUrl string, jwt security.EncodedJwt,
) (size uint32, e error) {
	glog.V(4).Info("Uploading part ", filename, " to ", fileUrl, "...")
	uploadResult, uploadError := Upload(fileUrl, filename, reader, false,
		"application/octet-stream", nil, jwt)
	if uploadError != nil {
		return 0, uploadError
	}
	return uploadResult.Size, nil
}

func upload_chunked_file_manifest(fileUrl string, manifest *ChunkManifest, jwt security.EncodedJwt) error {
	buf, e := manifest.Marshal()
	if e != nil {
		return e
	}
	bufReader := bytes.NewReader(buf)
	glog.V(4).Info("Uploading chunks manifest ", manifest.Name, " to ", fileUrl, "...")
	u, _ := url.Parse(fileUrl)
	q := u.Query()
	q.Set("cm", "true")
	u.RawQuery = q.Encode()
	_, e = Upload(u.String(), manifest.Name, bufReader, false, "application/json", nil, jwt)
	return e
}
