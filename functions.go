package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
)

var streamIDtoNumChunks map[uint32]uint32
var streamIDtoFilename map[uint32]string

func init() {
	streamIDtoNumChunks = make(map[uint32]uint32) // cache for numChunks by streamID
	streamIDtoFilename = make(map[uint32]string)
}

func hex(n uint32) string {
	return fmt.Sprintf("%08x", n)
}

func loadManifest(manifest string) *Header {
	b, err := ioutil.ReadFile(manifest)
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		log.Println(err.Error())
	} else {
		hdr := new(Header)
		err = json.Unmarshal(b, hdr)
		if err != nil {
			log.Println(err)
		} else {
			return hdr
		}
	}
	return nil
}

func chunksForStream(tmpDir string, streamID uint32) uint32 {
	numChunks, ok := streamIDtoNumChunks[streamID]
	if ok {
		return numChunks
	}
	// cache miss, check for manifest file
	manifest := filepath.Join(tmpDir, hex(streamID), "manifest.json")

	//log.Printf("chunksForStream loading manifest: %s\n", manifest)
	hdr := loadManifest(manifest)
	//log.Printf("chunksForStream loaded manifest: %v\n", hdr)
	if hdr != nil {
		streamIDtoNumChunks[streamID] = hdr.NumChunks
		return hdr.NumChunks
	}
	return 0
}

func fileCount(path string, numChunks uint32) (int, []uint32) {
	files, err := ioutil.ReadDir(path)
	if err != nil {
		return 0, nil
	}
	var i int
	var retval []uint32
	var lastFound int64
	for _, file := range files {
		if !file.IsDir() {
			// record gaps
			x, err := strconv.ParseInt(file.Name(), 16, 64)
			if err == nil {
				if i == 0 || lastFound == x-1 {
					lastFound = x
				} else {
					for j := lastFound + 1; j < x; j++ {
						retval = append(retval, uint32(j))
						//log.Printf("file missing: %08x\n", j)
					}
					lastFound = x
				}
				i++
			}
		}
	}
	// if only the last file is missing, the above loop won't notice
	if lastFound == int64(numChunks)-2 { // -1 for 1-index(numChunks) to 0-index(lastFound) and -1 for the next-to-last item
		retval = append(retval, numChunks-1)
	}
	return i, retval
}

func getStreamDirs(tmpDir string) ([]uint32, error) {
	files, err := ioutil.ReadDir(tmpDir)
	if err != nil {
		return nil, err
	}
	var retval []uint32
	for _, file := range files {
		if file.IsDir() {
			x, err := strconv.ParseInt(file.Name(), 16, 64)
			if err == nil {
				retval = append(retval, uint32(x))
			}
		}
	}
	return retval, nil
}

func mkStreamDir(tmpDir string, streamID uint32) (string, error) {
	// Create Directory for File, if not exist
	tmp := filepath.Join(tmpDir, hex(streamID))
	if _, err := os.Stat(tmp); os.IsNotExist(err) {
		return tmp, os.Mkdir(tmp, 0700)
	}
	return tmp, nil
}

var reentranceFlag int32

// this function uses a mutex which assumes only one thread will be calling it
// returns StreamComplete, UseMissing, and Missing
func isStreamComplete(tmpDir string, streamID uint32) (bool, bool, []uint32) {

	if atomic.CompareAndSwapInt32(&reentranceFlag, 0, 1) {
		defer atomic.StoreInt32(&reentranceFlag, 0)
	} else {
		log.Println("isStreamComplete - not reentered")
		return false, false, nil // this assumes that !done is enough to short-circuit other logic
	}

	numChunks := chunksForStream(tmpDir, streamID)
	if numChunks == 0 {
		log.Println("isStreamComplete: no manifest yet")
		return false, false, nil // no manifest yet
	}
	tmp := filepath.Join(tmpDir, hex(streamID))
	//log.Printf("isStreamComplete: %s %d == %d\n", tmp, fileCount(tmp)-1, int(numChunks))

	n, missing := fileCount(tmp, numChunks)

	if n+len(missing) == int(numChunks) {
		if len(missing) == 0 {
			log.Printf("File complete with %d / %d, %d missing\n", n, numChunks, len(missing))
			return true, false, nil
		} else {
			log.Printf("stream not complete: %d + %d = %d == %d\n", n, len(missing), n+len(missing), int(numChunks))
			return false, true, missing // only ask for resends when the first pass is done
		}
	}
	// not done with first pass, but not ready to ask for resends
	log.Printf("isStreamComplete: %d + %d = %d == %d\n", n, len(missing), n+len(missing), int(numChunks))
	return false, false, nil
}

func completeFile(tmpDir string, streamID uint32, rxDir string) {
	tmp := filepath.Join(tmpDir, hex(streamID))
	manifest := filepath.Join(tmp, "manifest.json")
	hdr := loadManifest(manifest)
	if hdr != nil && len(hdr.Filename) > 0 && len(hdr.Filename) < 256 {
		log.Printf("completed file, bytes: %d\n", hdr.Filesize)
		// Open the output file
		outfile, err := os.Create(filepath.Join(rxDir, hdr.Filename))
		defer outfile.Close()
		if err != nil {
			log.Println(err)
			return
		}
		// Copy out all the chunks
		var i uint64
		for i = 0; i < uint64(hdr.NumChunks); i++ {
			chunk := filepath.Join(tmp, hex(uint32(i)))
			b, err := ioutil.ReadFile(chunk)
			if err != nil {
				log.Println(err) // this will get hit if a file is missing
				return
			}
			outfile.Write(b) // todo: check n and error here for full fs, delete chunks as you go for low space
		}
		_, ok := streamIDtoNumChunks[streamID]
		if ok {
			delete(streamIDtoNumChunks, streamID)
		}
		if err := os.RemoveAll(tmp); err != nil {
			log.Println("removing stream tmp dir failed: " + tmp)
		}
	}
}
