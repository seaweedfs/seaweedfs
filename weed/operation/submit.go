package operation

import (
	"context"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"io"
	"math/rand/v2"
	"mime"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/security"
)

type FilePart struct {
	Reader   io.Reader
	FileName string
	FileSize int64
	MimeType string
	ModTime  int64 //in seconds
	Pref     StoragePreference
	Server   string //this comes from assign result
	Fid      string //this comes from assign result, but customizable
	Fsync    bool
}

type SubmitResult struct {
	FileName string `json:"fileName,omitempty"`
	FileUrl  string `json:"url,omitempty"`
	Fid      string `json:"fid,omitempty"`
	Size     uint32 `json:"size,omitempty"`
	Error    string `json:"error,omitempty"`
}

type StoragePreference struct {
	Replication string
	Collection  string
	DataCenter  string
	Ttl         string
	DiskType    string
	MaxMB       int
}

type GetMasterFn func(ctx context.Context) pb.ServerAddress

func SubmitFiles(masterFn GetMasterFn, grpcDialOption grpc.DialOption, files []*FilePart, pref StoragePreference, usePublicUrl bool) ([]SubmitResult, error) {
	results := make([]SubmitResult, len(files))
	for index, file := range files {
		results[index].FileName = file.FileName
	}
	ar := &VolumeAssignRequest{
		Count:       uint64(len(files)),
		Replication: pref.Replication,
		Collection:  pref.Collection,
		DataCenter:  pref.DataCenter,
		Ttl:         pref.Ttl,
		DiskType:    pref.DiskType,
	}
	ret, err := Assign(masterFn, grpcDialOption, ar)
	if err != nil {
		for index := range files {
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
		if usePublicUrl {
			file.Server = ret.PublicUrl
		}
		file.Pref = pref
		results[index].Size, err = file.Upload(pref.MaxMB, masterFn, usePublicUrl, ret.Auth, grpcDialOption)
		if err != nil {
			results[index].Error = err.Error()
		}
		results[index].Fid = file.Fid
		results[index].FileUrl = ret.PublicUrl + "/" + file.Fid
	}
	return results, nil
}

func NewFileParts(fullPathFilenames []string) (ret []*FilePart, err error) {
	ret = make([]*FilePart, len(fullPathFilenames))
	for index, file := range fullPathFilenames {
		if ret[index], err = newFilePart(file); err != nil {
			return
		}
	}
	return
}
func newFilePart(fullPathFilename string) (ret *FilePart, err error) {
	ret = &FilePart{}
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

func (fi *FilePart) Upload(maxMB int, masterFn GetMasterFn, usePublicUrl bool, jwt security.EncodedJwt, grpcDialOption grpc.DialOption) (retSize uint32, err error) {
	fileUrl := "http://" + fi.Server + "/" + fi.Fid
	if fi.ModTime != 0 {
		fileUrl += "?ts=" + strconv.Itoa(int(fi.ModTime))
	}
	if fi.Fsync {
		fileUrl += "?fsync=true"
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
		if fi.Pref.DataCenter != "" {
			ar := &VolumeAssignRequest{
				Count:       uint64(chunks),
				Replication: fi.Pref.Replication,
				Collection:  fi.Pref.Collection,
				Ttl:         fi.Pref.Ttl,
				DiskType:    fi.Pref.DiskType,
			}
			ret, err = Assign(masterFn, grpcDialOption, ar)
			if err != nil {
				return
			}
		}
		for i := int64(0); i < chunks; i++ {
			if fi.Pref.DataCenter == "" {
				ar := &VolumeAssignRequest{
					Count:       1,
					Replication: fi.Pref.Replication,
					Collection:  fi.Pref.Collection,
					Ttl:         fi.Pref.Ttl,
					DiskType:    fi.Pref.DiskType,
				}
				ret, err = Assign(masterFn, grpcDialOption, ar)
				if err != nil {
					// delete all uploaded chunks
					cm.DeleteChunks(masterFn, usePublicUrl, grpcDialOption)
					return
				}
				id = ret.Fid
			} else {
				id = ret.Fid
				if i > 0 {
					id += "_" + strconv.FormatInt(i, 10)
				}
			}
			fileUrl := genFileUrl(ret, id, usePublicUrl)
			count, e := uploadOneChunk(
				baseName+"-"+strconv.FormatInt(i+1, 10),
				io.LimitReader(fi.Reader, chunkSize),
				masterFn, fileUrl,
				ret.Auth)
			if e != nil {
				// delete all uploaded chunks
				cm.DeleteChunks(masterFn, usePublicUrl, grpcDialOption)
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
		err = uploadChunkedFileManifest(fileUrl, &cm, jwt)
		if err != nil {
			// delete all uploaded chunks
			cm.DeleteChunks(masterFn, usePublicUrl, grpcDialOption)
		}
	} else {
		uploadOption := &UploadOption{
			UploadUrl:         fileUrl,
			Filename:          baseName,
			Cipher:            false,
			IsInputCompressed: false,
			MimeType:          fi.MimeType,
			PairMap:           nil,
			Jwt:               jwt,
		}

		uploader, e := NewUploader()
		if e != nil {
			return 0, e
		}

		ret, e, _ := uploader.Upload(fi.Reader, uploadOption)
		if e != nil {
			return 0, e
		}
		return ret.Size, e
	}
	return
}

func genFileUrl(ret *AssignResult, id string, usePublicUrl bool) string {
	fileUrl := "http://" + ret.Url + "/" + id
	if usePublicUrl {
		fileUrl = "http://" + ret.PublicUrl + "/" + id
	}
	for _, replica := range ret.Replicas {
		if rand.IntN(len(ret.Replicas)+1) == 0 {
			fileUrl = "http://" + replica.Url + "/" + id
			if usePublicUrl {
				fileUrl = "http://" + replica.PublicUrl + "/" + id
			}
		}
	}
	return fileUrl
}

func uploadOneChunk(filename string, reader io.Reader, masterFn GetMasterFn,
	fileUrl string, jwt security.EncodedJwt,
) (size uint32, e error) {
	glog.V(4).Info("Uploading part ", filename, " to ", fileUrl, "...")
	uploadOption := &UploadOption{
		UploadUrl:         fileUrl,
		Filename:          filename,
		Cipher:            false,
		IsInputCompressed: false,
		MimeType:          "",
		PairMap:           nil,
		Jwt:               jwt,
	}

	uploader, uploaderError := NewUploader()
	if uploaderError != nil {
		return 0, uploaderError
	}

	uploadResult, uploadError, _ := uploader.Upload(reader, uploadOption)
	if uploadError != nil {
		return 0, uploadError
	}
	return uploadResult.Size, nil
}

func uploadChunkedFileManifest(fileUrl string, manifest *ChunkManifest, jwt security.EncodedJwt) error {
	buf, e := manifest.Marshal()
	if e != nil {
		return e
	}
	glog.V(4).Info("Uploading chunks manifest ", manifest.Name, " to ", fileUrl, "...")
	u, _ := url.Parse(fileUrl)
	q := u.Query()
	q.Set("cm", "true")
	u.RawQuery = q.Encode()
	uploadOption := &UploadOption{
		UploadUrl:         u.String(),
		Filename:          manifest.Name,
		Cipher:            false,
		IsInputCompressed: false,
		MimeType:          "application/json",
		PairMap:           nil,
		Jwt:               jwt,
	}

	uploader, e := NewUploader()
	if e != nil {
		return e
	}

	_, e = uploader.UploadData(buf, uploadOption)
	return e
}
