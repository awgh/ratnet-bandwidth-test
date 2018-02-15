package main

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"time"

	"github.com/awgh/bencrypt/ecc"
	"github.com/awgh/ratnet/api"
	"github.com/awgh/ratnet/nodes/qldb"
	"github.com/awgh/ratnet/policy"
	"github.com/awgh/ratnet/router"
	"github.com/awgh/ratnet/transports/https"
	"github.com/awgh/ratnet/transports/tls"
	"github.com/awgh/ratnet/transports/udp"
)

const (
	hdrMagic    uint32 = 0xF113
	chunkMagic  uint32 = 0xF114
	resendMagic uint32 = 0xF115
	//chunksize     uint32 = 49007 // this is for base64-encoding in the db (QL limits recordsize)
	//chunksize     uint32 = 65432 // for raw binary in the db (QL limits recordsize)
	chunksize     uint32 = 64 * 1024
	hdrsize       uint32 = 279
	chunkHdrSize  uint32 = 12
	chunkDataSize uint32 = chunksize - chunkHdrSize
)

// WARNING DANGER TODO FAKE CODE!!!
// PROGRAM IS NOT SAFE TO USE UNTIL THESE KEYS ARE REPLACED!!!
var pubprivkeyb64Ecc = "Tcksa18txiwMEocq7NXdeMwz6PPBD+nxCjb/WCtxq1+dln3M3IaOmg+YfTIbBpk+jIbZZZiT+4CoeFzaJGEWmg=="

//var pubkeyb64Ecc = "Tcksa18txiwMEocq7NXdeMwz6PPBD+nxCjb/WCtxq18="

// Header manifest for a file transfer
type Header struct {
	StreamID  uint32
	Filesize  uint64
	Chunksize uint32
	NumChunks uint32
	Filename  string
}

type RequestResend struct {
	StreamID uint32
	Chunks   []uint32
}

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to `file`")
var memprofile = flag.String("memprofile", "", "write memory profile to `file`")
var logfile = flag.String("logfile", "", "write logs to `file`")

func init() {
	gob.Register(&RequestResend{})
}

func main() {
	var dbFile, rxDir, txDir, tmpDir, proto string
	var publicPort int

	flag.StringVar(&dbFile, "dbfile", "ratnet.ql", "QL Database File")
	flag.StringVar(&proto, "t", "udp", "Transport Protocol (udp|tls|https)")
	flag.IntVar(&publicPort, "p", 20001, "Public Listening Port (*)")
	flag.StringVar(&rxDir, "rxdir", "inbox", "Download Directory")
	flag.StringVar(&txDir, "txdir", "outbox", "Upload Directory")
	flag.StringVar(&tmpDir, "tempdir", "/tmp", "Temp File Directory")

	//flag.StringVar(&sentDir, "sentdir", "sent", "Completed Uploads Directory")
	flag.Parse()

	if proto != "udp" && proto != "tls" && proto != "https" {
		log.Println("Invalid protocol, options are: udp, tls, https")
		os.Exit(1)
	}

	var f *os.File
	if *logfile != "" {
		// Config default logger
		var err error
		log.SetFlags(log.Lshortfile | log.Lmicroseconds)
		f, err = os.OpenFile(*logfile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			log.Fatalf("error opening logfile: %v", err)
		}
		defer f.Close()
		log.SetOutput(f)
	}
	// CPU profiling support
	if *cpuprofile != "" {
		cf, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(cf)
		defer pprof.StopCPUProfile()
	}

	// capture ctrl+c and stop CPU profiler
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for sig := range c {
			log.Printf("captured %v, stopping profiler and exiting..", sig)

			// Memory profiling support
			if *memprofile != "" {
				mf, err := os.Create(*memprofile)
				if err != nil {
					log.Fatal("could not create memory profile: ", err)
				}
				runtime.GC() // get up-to-date statistics
				if err := pprof.WriteHeapProfile(mf); err != nil {
					log.Fatal("could not write memory profile: ", err)
				}
				mf.Close()
			}
			//
			if *cpuprofile != "" {
				pprof.StopCPUProfile()
			}
			if *logfile != "" {
				f.Close()
			}
			os.Exit(1)
		}
	}()

	listenPublic := fmt.Sprintf(":%d", publicPort)

	// QLDB Node Mode
	node := qldb.New(new(ecc.KeyPair), new(ecc.KeyPair))
	node.BootstrapDB(dbFile)

	// TODO: hardcoded test key because this is TEST PROGRAM until this is fixed.
	if err := node.AddChannel("fixme", pubprivkeyb64Ecc); err != nil {
		log.Fatal(err.Error())
	}

	router := router.NewDefaultRouter()
	router.ForwardConsumedChannels = false
	node.SetRouter(router)
	//

	var transportPublic api.Transport
	switch proto {
	case "udp":
		transportPublic = udp.New(node)
	case "tls":
		transportPublic = tls.New("cert.pem", "key.pem", node, true)
	case "https":
		transportPublic = https.New("cert.pem", "key.pem", node, true)
	}
	//transportPublic.SetByteLimit(10000 * 1024) // todo this doesn't change the value in the global map

	p2p := policy.NewP2P(transportPublic, listenPublic, node, false, 50, 30000)
	node.SetPolicy(p2p)
	log.Println("Public Server starting: ", listenPublic)

	node.FlushOutbox(0)
	if err := node.Start(); err != nil {
		log.Fatal("node didn't start: " + err.Error())
	}

	go handlerLoop(node, txDir, rxDir, tmpDir)

	log.Printf("transport byte limit set to: %d\n", transportPublic.ByteLimit())

	var streamID uint32
	files, _ := ioutil.ReadDir(txDir)
	for _, f := range files {
		filename := filepath.Join(txDir, f.Name())
		log.Println("Sending ", filename, " with size ", f.Size())

		streamIDtoFilename[streamID] = f.Name()

		numChunks64 := f.Size() / int64(chunkDataSize)
		if f.Size()%int64(chunkDataSize) != 0 {
			numChunks64++
		}
		if numChunks64 > math.MaxUint32 {
			log.Println("File is too big, cancelling send")
			continue
		}
		numChunks := uint32(numChunks64)
		log.Println("Sending Header with ", numChunks, " chunks")

		hdr := make([]byte, hdrsize)
		binary.LittleEndian.PutUint32(hdr, hdrMagic)
		binary.LittleEndian.PutUint32(hdr[4:], streamID)
		binary.LittleEndian.PutUint64(hdr[8:], uint64(f.Size()))
		binary.LittleEndian.PutUint32(hdr[16:], chunksize)
		binary.LittleEndian.PutUint32(hdr[20:], numChunks)
		copy(hdr[24:], f.Name())

		if err := node.SendChannel("fixme", hdr); err != nil {
			log.Fatal(err.Error())
		}

		inputFile, err := os.Open(filename)
		if err != nil {
			log.Println(err.Error())
			continue
		}
		var n int
		var batchSize uint32 = 100
		chunkBatch := make([][]byte, batchSize)
		for x := range chunkBatch {
			chunkBatch[x] = make([]byte, chunksize)
		}
		chunksInBatch := 0

		for i := uint32(0); i < numChunks-1; i++ {
			chunk := make([]byte, chunksize)
			binary.LittleEndian.PutUint32(chunk, chunkMagic)
			binary.LittleEndian.PutUint32(chunk[4:], streamID)
			binary.LittleEndian.PutUint32(chunk[8:], i)

			n, err = inputFile.ReadAt(chunk[12:], int64(chunkDataSize)*int64(i))
			if err == io.EOF {
				log.Fatal("EOF too early, code is broken")
			}
			if n != int(chunkDataSize) || err != nil {
				log.Fatalf("chunk read underflow: n=%d , i= %d, err=%+v\n", n, i, err.Error())
			}
			log.Printf("xxx %d %d\n", i, n)
			chunkBatch[i%batchSize] = chunk[:] // chunksize
			chunksInBatch++

			if i%batchSize == batchSize-1 { // last chunk in batch (i is 0-indexed, numChunks is 1-indexed)
				if erra := node.SendChannelBulk("fixme", chunkBatch); erra != nil {
					log.Fatal(erra.Error())
				}
				log.Printf("bulk sent: %d messages\n", len(chunkBatch))
				chunkBatch = nil // reset chunkBatch
				chunkBatch = make([][]byte, batchSize)
				for x := range chunkBatch {
					chunkBatch[x] = make([]byte, chunksize)
				}
				chunksInBatch = 0
			}
		}
		log.Printf("at remainder, chunksInBatch= %d\n", chunksInBatch)
		if chunksInBatch > 0 {
			chunk := make([]byte, chunksize)
			binary.LittleEndian.PutUint32(chunk, chunkMagic)
			binary.LittleEndian.PutUint32(chunk[4:], streamID)
			binary.LittleEndian.PutUint32(chunk[8:], numChunks-1)

			n, err = inputFile.ReadAt(chunk[12:], int64(chunkDataSize)*int64(numChunks-1))
			if err == io.EOF {
				log.Printf("EOF in remainder, n=%d\n", n)
			} else if n == 0 || err != nil {
				log.Fatalf("chunk remainder error: n=%d , i= %d, err=%+v\n", n, numChunks-1, err.Error())
			}
			if n > 0 { // add the last chunk, if there is one
				log.Printf("xxx %d %d\n", numChunks-1, n)
				chunkBatch[(numChunks-1)%batchSize] = chunk[:12+n] // chunkHdrSize+n
				chunksInBatch++
			}
			if len(chunkBatch) > 0 { // add any remaining chunks that didn't fill a block
				err = node.SendChannelBulk("fixme", chunkBatch[:chunksInBatch])
				if err != nil {
					log.Fatal(err.Error())
				}
			}
		}
		log.Println("Sending complete: " + filename)
		/*
			go func() {
				if err := os.Remove(filename); err != nil {
					log.Println(err)
				}
			}()
		*/
		defer inputFile.Close()
		streamID++
	}
	for {
		runtime.GC()
		time.Sleep(1 * time.Second)

		//for each stream dir in tmp dir, check for done-ness
		streamDirs, err := getStreamDirs(tmpDir)
		if err != nil {
			log.Println(err)
		} else {

			//sidToMissing := make(map[uint32][]byte, 0)

			for _, sid := range streamDirs {
				done, useMissing, missing := isStreamComplete(tmpDir, sid)
				if done {
					completeFile(tmpDir, sid, rxDir)
				} else if useMissing {
					// request missing pieces
					rr := &RequestResend{StreamID: sid, Chunks: missing}

					node.FlushOutbox(0) // this is just for the test

					magic := make([]byte, 4)
					binary.LittleEndian.PutUint32(magic, resendMagic)
					//use default gob encoder
					var buf bytes.Buffer
					enc := gob.NewEncoder(&buf)
					if err := enc.Encode(rr); err != nil {
						log.Println("resend request TX gob encode failed: " + err.Error())
					} else {
						log.Printf("resend request sent: %+v\n", rr)
						node.SendChannel("fixme", append(magic, buf.Bytes()...))
					}
				} /* else if !useMissing {
					bytes.Compare(a, b)
				} */
			}
		}
	}
}

func handlerLoop(node *qldb.Node, txDir, rxDir, tmpDir string) {
	for {

		skipStreams := make(map[uint32]bool) // streamID's for files we've seen

		// read a message from the output channel
		msg := <-node.Out()
		//log.Println("Receiving stuff!")
		buf := msg.Content.Bytes()
		// read and validate the header
		magic := binary.LittleEndian.Uint32(buf)
		switch magic {

		case hdrMagic:
			// Read Header Fields

			var hdr Header
			hdr.StreamID = binary.LittleEndian.Uint32(buf[4:])
			hdr.Filesize = binary.LittleEndian.Uint64(buf[8:])
			hdr.Chunksize = binary.LittleEndian.Uint32(buf[16:])
			hdr.NumChunks = binary.LittleEndian.Uint32(buf[20:])
			filename := make([]byte, 255)
			copy(filename, buf[24:])
			filename = bytes.TrimRight(filename, string([]byte{0}))
			hdr.Filename = string(filename)

			log.Printf("read header: %v\n", hdr)
			if _, err := os.Stat(filepath.Join(txDir, hdr.Filename)); err == nil {
				// todo: this should be based on file hash, not file name
				// this is a file we're sending
				skipStreams[hdr.StreamID] = true
				log.Println("Skipping file we already have")
				continue
			}
			if tmp, err := mkStreamDir(tmpDir, hdr.StreamID); err != nil {
				log.Println(err)
			} else {
				// Write Manifest File
				b, err := json.Marshal(hdr)
				if err != nil {
					log.Println(err)
				}
				err = ioutil.WriteFile(filepath.Join(tmp, "manifest.json"), b, 0644)
				if err != nil {
					log.Println(err)
				}
				streamIDtoNumChunks[hdr.StreamID] = hdr.NumChunks // update cache
			}
		case chunkMagic:
			streamID := binary.LittleEndian.Uint32(buf[4:])
			chunkID := binary.LittleEndian.Uint32(buf[8:])

			log.Printf("read chunk: %d %d\n", streamID, chunkID)
			if _, ok := skipStreams[streamID]; ok {
				continue // we're sending this file, don't receive it
			}
			if tmp, err := mkStreamDir(tmpDir, streamID); err != nil {
				log.Println(err)
			} else {
				// Write Chunk File
				chunkFile := filepath.Join(tmp, hex(chunkID))
				err = ioutil.WriteFile(chunkFile, buf[12:], 0600)
				if err != nil {
					log.Println(err)
				}
			}
		case resendMagic:
			var rr RequestResend
			b := bytes.NewBuffer(buf[4:])
			dec := gob.NewDecoder(b)
			if err := dec.Decode(&rr); err != nil {
				log.Println("resend request RX gob decode failed: " + err.Error())
				return
			}

			log.Printf("Got Resend Request: %+v\n", rr)

			v, ok := streamIDtoFilename[rr.StreamID]
			if !ok {
				log.Printf("Invalid stream ID in resend request: %08x\n", rr.StreamID)
				continue
			}
			filename := filepath.Join(txDir, v)
			inputFile, err := os.Open(filename)
			if err != nil {
				log.Println(err.Error())
				continue
			}

			chunkBatch := make([][]byte, len(rr.Chunks))
			for x := range chunkBatch {
				chunkBatch[x] = make([]byte, chunksize)
			}
			chunksInBatch := 0
			for i := range rr.Chunks {
				chunk := make([]byte, chunksize)
				binary.LittleEndian.PutUint32(chunk, chunkMagic)
				binary.LittleEndian.PutUint32(chunk[4:], rr.StreamID)
				binary.LittleEndian.PutUint32(chunk[8:], rr.Chunks[i])

				n, err := inputFile.ReadAt(chunk[12:], int64(chunkDataSize)*int64(rr.Chunks[i]))
				if err == io.EOF {
					log.Printf("EOF in Resend, n=%d\n", n)
				} else if n == 0 || err != nil {
					log.Fatalf("chunk resend error: n=%d , i= %d, err=%+v\n", n, rr.Chunks[i], err.Error())
				}
				if n > 0 {
					log.Printf("xxx %d %d\n", rr.Chunks[i], n)
					chunkBatch[i] = chunk[:12+n] // chunkHdrSize+n
					chunksInBatch++
				}
			}
			if len(chunkBatch) > 0 {
				err = node.SendChannelBulk("fixme", chunkBatch[:chunksInBatch])
				if err != nil {
					log.Fatal(err.Error())
				}
			}

		default:
			log.Println("Magic Doesn't Match!")
			continue
		}
	}
}
