package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
)

var streamIDtoNumChunks map[uint32]uint32

func init() {
	streamIDtoNumChunks = make(map[uint32]uint32) // cache for numChunks by streamID
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
	manifest := filepath.Join(tmpDir, strconv.FormatUint(uint64(streamID), 16),
		"manifest.json")

	//log.Printf("chunksForStream loading manifest: %s\n", manifest)
	hdr := loadManifest(manifest)
	//log.Printf("chunksForStream loaded manifest: %v\n", hdr)
	if hdr != nil {
		streamIDtoNumChunks[streamID] = hdr.NumChunks
		return hdr.NumChunks
	}
	return 0
}

func fileCount(path string) int {
	i := 0
	files, err := ioutil.ReadDir(path)
	if err != nil {
		return 0
	}
	for _, file := range files {
		if !file.IsDir() {
			i++
		}
	}
	return i
}

func getStreamDirs(tmpDir string) ([]uint32, error) {
	files, err := ioutil.ReadDir(tmpDir)
	if err != nil {
		return nil, err
	}
	var retval []uint32
	for _, file := range files {
		if file.IsDir() {
			x, err := strconv.Atoi(file.Name())
			if err == nil {
				retval = append(retval, uint32(x))
			}
		}
	}
	return retval, nil
}

func mkStreamDir(tmpDir string, streamID uint32) (string, error) {
	// Create Directory for File, if not exist
	tmp := filepath.Join(tmpDir, strconv.FormatUint(uint64(streamID), 16))
	if _, err := os.Stat(tmp); os.IsNotExist(err) {
		return tmp, os.Mkdir(tmp, 0700)
	}
	return tmp, nil
}

func isStreamComplete(tmpDir string, streamID uint32) bool {

	numChunks := chunksForStream(tmpDir, streamID)
	if numChunks == 0 {
		return false // no manifest yet
	}

	tmp := filepath.Join(tmpDir, strconv.FormatUint(uint64(streamID), 16))

	//log.Printf("isStreamComplete: %s %d == %d\n", tmp, fileCount(tmp)-1, int(numChunks))

	return fileCount(tmp)-1 == int(numChunks) // -1 for the manifest.json file, which is not a chunk
}

var totalBytes uint64

func completeFile(tmpDir string, streamID uint32, rxDir string) {
	tmp := filepath.Join(tmpDir, strconv.FormatUint(uint64(streamID), 16))
	manifest := filepath.Join(tmp, "manifest.json")
	hdr := loadManifest(manifest)
	if hdr != nil && len(hdr.Filename) > 0 && len(hdr.Filename) < 256 {
		log.Println("completed file, cumulative bytes: " + strconv.FormatUint(totalBytes, 10))
		totalBytes += hdr.Filesize
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
			chunk := filepath.Join(tmp, strconv.FormatUint(i, 16))
			b, err := ioutil.ReadFile(chunk)
			if err != nil {
				log.Println(err) // this will get hit if a file is missing
				return
			}
			outfile.Write(b)
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

/*
func completeFile(tmpDir string, streamID uint32, rxDir string) {
	tmp := filepath.Join(tmpDir, strconv.FormatUint(uint64(streamID), 16))
	manifest := filepath.Join(tmp, "manifest.json")

	//log.Printf("completeFile loading manifest: %s\n", manifest)
	hdr := loadManifest(manifest)
	//log.Printf("completeFile loaded manifest: %v\n", hdr)

	if hdr != nil && len(hdr.Filename) > 0 && len(hdr.Filename) < 256 {
		log.Println("completed file, cumulative bytes: " + strconv.FormatUint(totalBytes, 10))
		totalBytes += hdr.Filesize
		_, ok := streamIDtoNumChunks[streamID]
		if ok {
			delete(streamIDtoNumChunks, streamID)
		}
		if err := os.RemoveAll(tmp); err != nil {
			log.Println("removing stream tmp dir failed: " + tmp)
		}
	}
}
*/
