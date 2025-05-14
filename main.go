package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync"

	"github.com/joho/godotenv"

	_ "net/http/pprof"

	"github.com/lf-edge/eve-libs/nettrace"
	"github.com/lf-edge/eve-libs/zedUpload"
	"github.com/lf-edge/eve-libs/zedUpload/types"
	"github.com/lf-edge/eve/pkg/pillar/base"
	"github.com/lf-edge/eve/pkg/pillar/netdump"
	"github.com/sirupsen/logrus"
)

var (
	logger *logrus.Logger
	log    *base.LogObject
)

const (
	SyncAwsTr          zedUpload.SyncTransportType = "s3"
	SyncAzureTr        zedUpload.SyncTransportType = "azure"
	SyncGSTr           zedUpload.SyncTransportType = "google"
	SyncHttpTr         zedUpload.SyncTransportType = "http"
	SyncSftpTr         zedUpload.SyncTransportType = "sftp"
	SyncOCIRegistryTr  zedUpload.SyncTransportType = "oci"
	progressFileSuffix                             = ".progress"
)

// Notify simple struct to pass notification messages
type Notify struct{}

// CancelChannel is the type we send over a channel for the per-download cancels
type CancelChannel chan Notify

func loadDownloadedParts(locFilename string) types.DownloadedParts {
	var downloadedParts types.DownloadedParts
	fd, err := os.Open(locFilename + progressFileSuffix)
	if err == nil {
		decoder := json.NewDecoder(fd)
		err = decoder.Decode(&downloadedParts)
		if err != nil {
			log.Errorf("failed to decode progress file: %s", err)
		}
		err := fd.Close()
		if err != nil {
			log.Errorf("failed to close progress file: %s", err)
		}
	}
	return downloadedParts
}

func saveDownloadedParts(locFilename string, downloadedParts types.DownloadedParts) {
	fd, err := os.Create(locFilename + progressFileSuffix)
	if err != nil {
		log.Errorf("error creating progress file: %s", err)
	} else {
		encoder := json.NewEncoder(fd)
		err = encoder.Encode(downloadedParts)
		if err != nil {
			log.Errorf("failed to encode progress file: %s", err)
		}
		err := fd.Close()
		if err != nil {
			log.Errorf("failed to close progress file: %s", err)
		}
	}
}

func main() {
	// Setup logger
	logger = logrus.New()
	logger.SetLevel(logrus.TraceLevel)
	log = base.NewSourceLogObject(logger, "main", 1234)

	if err := godotenv.Load(); err != nil {
		log.Noticef("warning: no .env file found (%v)\n", err)
	}

	accountURL := os.Getenv("ACCOUNT_URL")
	container := os.Getenv("CONTAINER")
	accountName := os.Getenv("ACCOUNT_NAME")
	accountKey := os.Getenv("ACCOUNT_KEY")
	remoteFile := os.Getenv("REMOTE_FILE")
	localFile := os.Getenv("LOCAL_FILE")

	// WaitGroup to block main until download is done
	var wg sync.WaitGroup
	wg.Add(1)
	// Start pprof listener
	go func() {
		defer wg.Done()
		fmt.Println("pprof listening on :6060")
		if err := http.ListenAndServe("0.0.0.0:6060", nil); err != nil {
			fmt.Printf("pprof failed: %v\n", err)
		}
	}()

	//headerFieldsOpt := nettrace.HdrFieldsOptValueLenOnly
	//headerFieldsOpt = nettrace.HdrFieldsOptWithValues
	//headerFieldsOpt = nettrace.HdrFieldsOptDisabled

	traceOpts := []nettrace.TraceOpt{
		&nettrace.WithLogging{
			CustomLogger: &base.LogrusWrapper{Log: log},
		},
		&nettrace.WithConntrack{},
		&nettrace.WithDNSQueryTrace{},
		/*&nettrace.WithHTTPReqTrace{
			HeaderFields: headerFieldsOpt,
		},*/
	}
	var tracedReq netdump.TracedNetRequest
	auth := &zedUpload.AuthInput{
		AuthType: "password",
		Uname:    accountName,
		Password: accountKey,
	}

	dCtx, _ := zedUpload.NewDronaCtx("zdownloader", 0)
	dEndPoint, _ := dCtx.NewSyncerDest(SyncAzureTr, accountURL, container, auth)
	dEndPoint.WithNetTracing(traceOpts...)

	downloadedParts := loadDownloadedParts(remoteFile)
	downloadedPartsHash := downloadedParts.Hash()

	var respChan = make(chan *zedUpload.DronaRequest)

	// create Request
	req := dEndPoint.NewRequest(zedUpload.SyncOpDownload, remoteFile, localFile,
		int64(18887540736), true, respChan)
	if req == nil {
		return
	}
	req = req.WithDoneParts(downloadedParts)
	req = req.WithCancel(context.Background())
	defer req.Cancel()
	req = req.WithLogger(logger)

	// Tell caller where we can be cancelled
	/*cancelChan := make(chan Notify, 1)
	receiveChan := make(chan CancelChannel, 1)
	receiveChan <- cancelChan
	// if we are done before event from cancelChan do nothing
	doneChan := make(chan Notify)
	defer close(doneChan)
	go func() {
		select {
		case <-doneChan:
			log.Functionf("doneChan")
			// remove cancel channel
			receiveChan <- nil
		case _, ok := <-cancelChan:
			if ok {
				errStr := fmt.Sprintf("cancelled by user: <%s>, <%s>, <%s>",
					container, remoteFile)
				log.Error(errStr)
				_ = req.Cancel()
			} else {
				log.Warnf("cancelChan closed")
				return
			}
		}
	}()*/

	req.Post()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for resp := range respChan {
			newDownloadedParts := resp.GetDoneParts()
			newDownloadedPartsHash := newDownloadedParts.Hash()
			if downloadedPartsHash != newDownloadedPartsHash {
				downloadedPartsHash = newDownloadedPartsHash
				downloadedParts = newDownloadedParts
				saveDownloadedParts(localFile, downloadedParts)
			}

			if resp.IsDnUpdate() {
				currentSize, totalSize, _ := resp.Progress()
				log.Functionf("Update progress for %v: %v/%v",
					resp.GetLocalName(), currentSize, totalSize)
				// sometime, the download goes to an infinite loop,
				// showing it has downloaded, more than it is supposed to
				// aborting download, marking it as an error
				if currentSize > totalSize {
					errStr := fmt.Sprintf("Size '%v' provided in image config of '%s' is incorrect.\nDownload status (%v / %v). Aborting the download",
						totalSize, resp.GetLocalName(), currentSize, totalSize)
					log.Errorln(errStr)
					return
				}
				dEndPoint.GetNetTrace("AzureDownload")
				continue
			}
			err := resp.GetDnStatus()
			if resp.IsError() {
				fmt.Println("Download failed: %v, %v", tracedReq, err)
				return
			}

			fmt.Println("Download done: %v size %d", resp.GetLocalName(), resp.GetAsize())
			return
		}

	}()
	wg.Wait()
	fmt.Printf("Download succeeded\n")
}
