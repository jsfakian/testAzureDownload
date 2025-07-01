package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/joho/godotenv"

	_ "net/http/pprof"

	"github.com/lf-edge/eve-libs/nettrace"
	"github.com/lf-edge/eve-libs/zedUpload"
	"github.com/lf-edge/eve-libs/zedUpload/types"
	"github.com/lf-edge/eve/pkg/pillar/base"
	"github.com/sirupsen/logrus"
)

var (
	logger *logrus.Logger
	log    *base.LogObject
)

const (
	SyncAwsTr          zedUpload.SyncTransportType = "s3"
	SyncAzureTr        zedUpload.SyncTransportType = "azure"
	progressFileSuffix                             = ".progress"
)

type Notify struct{}
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
	logger = logrus.New()
	logger.SetLevel(logrus.TraceLevel)
	log = base.NewSourceLogObject(logger, "main", 1234)

	_ = godotenv.Load()

	transport := os.Getenv("TRANSPORT")

	// Azure values
	azureURL := os.Getenv("ACCOUNT_URL")
	azureContainer := os.Getenv("CONTAINER")
	azureRemoteFile := os.Getenv("REMOTE_FILE")
	azureLocalFile := os.Getenv("LOCAL_FILE")
	azureAccountName := os.Getenv("ACCOUNT_NAME")
	azureAccountKey := os.Getenv("ACCOUNT_KEY")

	// AWS values
	awsRegion := os.Getenv("AWS_ACCOUNT_URL") // this is actually the region
	awsContainer := os.Getenv("AWS_CONTAINER")
	awsRemoteFile := os.Getenv("AWS_REMOTE_FILE")
	awsLocalFile := os.Getenv("AWS_LOCAL_FILE") // reuse same local output or change if needed
	awsAccessKey := os.Getenv("AWS_KEY_ID")
	awsSecretKey := os.Getenv("AWS_KEY_SECRET")
	//awsToken := os.Getenv("AWS_TOKEN")

	var (
		auth       *zedUpload.AuthInput
		accountURL string
		container  string
		remoteFile string
		localFile  string
		syncTr     zedUpload.SyncTransportType
	)

	switch transport {
	case "azure":
		syncTr = SyncAzureTr
		auth = &zedUpload.AuthInput{
			AuthType: "password",
			Uname:    azureAccountName,
			Password: azureAccountKey,
		}
		accountURL = azureURL
		container = azureContainer
		remoteFile = azureRemoteFile
		localFile = azureLocalFile
	case "aws":
		syncTr = SyncAwsTr
		if strings.HasPrefix(awsRegion, "http") {
			log.Fatalf("For AWS, AWS_ACCOUNT_URL must be the region (e.g., me-central-1), not a full URL")
		}
		auth = &zedUpload.AuthInput{
			AuthType: "s3",
			Uname:    awsAccessKey,
			Password: awsSecretKey,
		}
		accountURL = awsRegion
		container = awsContainer
		remoteFile = awsRemoteFile
		localFile = awsLocalFile
	default:
		log.Fatalf("Unsupported TRANSPORT: %s", transport)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		fmt.Println("pprof listening on :6060")
		_ = http.ListenAndServe("0.0.0.0:6060", nil)
	}()

	traceOpts := []nettrace.TraceOpt{
		&nettrace.WithLogging{CustomLogger: &base.LogrusWrapper{Log: log}},
		&nettrace.WithConntrack{},
		&nettrace.WithDNSQueryTrace{},
	}

	dCtx, _ := zedUpload.NewDronaCtx("mydownloader", 0)
	dEndPoint, err := dCtx.NewSyncerDest(syncTr, accountURL, container, auth)
	if err != nil {
		log.Fatalf("Failed to create endpoint: %v", err)
	}
	dEndPoint.WithNetTracing(traceOpts...)

	downloadedParts := loadDownloadedParts(remoteFile)
	downloadedPartsHash := downloadedParts.Hash()

	respChan := make(chan *zedUpload.DronaRequest)
	objSize := int64(3750756352)

	req := dEndPoint.NewRequest(zedUpload.SyncOpDownload, remoteFile, localFile, objSize, true, respChan)

	if req == nil {
		log.Errorf("Failed to create request")
		return
	}
	req = req.WithDoneParts(downloadedParts)
	req = req.WithCancel(context.Background())
	defer req.Cancel()
	req = req.WithLogger(logger)

	req.Post()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for resp := range respChan {
			newParts := resp.GetDoneParts()
			if downloadedPartsHash != newParts.Hash() {
				downloadedParts = newParts
				downloadedPartsHash = newParts.Hash()
				saveDownloadedParts(localFile, downloadedParts)
			}

			if resp.IsDnUpdate() {
				currentSize, totalSize, _ := resp.Progress()
				log.Functionf("Progress: %v/%v for %s", currentSize, totalSize, resp.GetLocalName())
				if currentSize > totalSize {
					log.Errorf("Aborting: current > total size (%v > %v)", currentSize, totalSize)
					return
				}
				dEndPoint.GetNetTrace("DownloadTrace")
				continue
			}

			if resp.IsError() {
				log.Errorf("Download failed: %v", resp.GetDnStatus())
				return
			}

			log.Functionf("Download done: %s (%d bytes)", resp.GetLocalName(), resp.GetAsize())
			return
		}
	}()
	wg.Wait()
	fmt.Println("Download succeeded")
}
