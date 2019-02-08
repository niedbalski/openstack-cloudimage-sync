package main

import (
	"crypto/sha256"
	"fmt"
	"github.com/juju/errors"
	"github.com/juju/juju/environs/imagedownloads"
	"github.com/juju/juju/environs/imagemetadata"
	"github.com/juju/juju/environs/simplestreams"
	log "github.com/sirupsen/logrus"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"sync"
	"time"
)

type Image struct {
	Distro, Architecture, Release string
	File                          *os.File
}

type ImageUploadHandler struct {
	ErrorChannel *chan error
	Uploader     *GlanceImageUploader
}

func NewImageUploadHandler(cloudname string, configPath string, errChannel *chan error) (*ImageUploadHandler, error) {
	glanceImageUploader, err := NewGlanceImageUploader(cloudname, configPath)
	if err != nil {
		return nil, err
	}
	return &ImageUploadHandler{ErrorChannel: errChannel, Uploader: glanceImageUploader}, nil
}

func (uploader *ImageUploadHandler) Handle(images *chan Image) {
	for {
		select {
		case image := <-*images:
			{
				go uploader.Uploader.Upload(&image)
			}
		}
	}
}

type ImageFetchHandler struct {
	ImagesChannel *chan Image
	ErrorChannel  *chan error
	Fetchers      []ImageFetcher
	WaitGroup     *sync.WaitGroup
	Config        ImageSource
	Name          string
	ImageBasePath string
}

type ImageFetcher interface {
	Fetch(wg *sync.WaitGroup, images *chan Image, errorChannel *chan error)
	GetImageURL() (string, error)
}

type BaseImageFetcher struct {
	ImageBasePath, Name, Release, Architecture string
}

type UbuntuImageFetcher struct {
	BaseImageFetcher
}

const (
	BIOSFType           = "disk1.img"
	UEFIFType           = "uefi1.img"
	UbuntuLatestRelease = "xenial"
	UbuntuImagesBaseURL = imagemetadata.UbuntuCloudImagesURL + "/" + imagemetadata.ReleasedImagesPath
)

func (fetcher *UbuntuImageFetcher) GetImageURL() (string, error) {
	var ftype = BIOSFType

	if fetcher.Release == "" || fetcher.Release == "latest" {
		fetcher.Release = UbuntuLatestRelease
	}

	if fetcher.Architecture == "arm64" {
		ftype = UEFIFType
	}

	metadata, err := imagedownloads.One(fetcher.Architecture, fetcher.Release,
		"released",
		ftype, func() simplestreams.DataSource {
			return simplestreams.NewURLSignedDataSource(
				"ubuntu cloud images",
				UbuntuImagesBaseURL,
				imagemetadata.SimplestreamsImagesPublicKey,
				false,
				simplestreams.DEFAULT_CLOUD_DATA,
				true,
			)
		},
	)

	if err != nil {
		return "", err
	}

	url, err := metadata.DownloadURL()
	if err != nil {
		return "", err
	}

	return url.String(), nil
}

func (fetcher *UbuntuImageFetcher) Fetch(wg *sync.WaitGroup, images *chan Image, errChannel *chan error) {
	defer wg.Done()

	var image Image

	image = Image{Release: fetcher.Release, Distro: fetcher.Name, Architecture: fetcher.Architecture}

	imageURL, err := fetcher.GetImageURL()
	if err != nil {
		*errChannel <- err
		return
	}

	log.Infof("Downloading image: %s", imageURL)
	req, err := http.NewRequest("GET", imageURL, nil)
	if err != nil {
		*errChannel <- err
		return
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		*errChannel <- err
		return
	}

	defer func() {
		resp.Body.Close()
	}()

	if resp.StatusCode != 200 {
		*errChannel <- fmt.Errorf(resp.Status)
		return
	}

	hash := sha256.New()
	image.File, err = ioutil.TempFile(fetcher.ImageBasePath, "image")
	if err != nil {
		*errChannel <- err
		return
	}

	writer := io.MultiWriter(image.File, hash)
	_, err = io.Copy(writer, resp.Body)
	if err != nil {
		*errChannel <- err
		return
	}

	*images <- image
}

func NewUbuntuImageFetcher(release, architecture, basepath string) (*UbuntuImageFetcher, error) {
	return &UbuntuImageFetcher{BaseImageFetcher{ImageBasePath: basepath, Name: "ubuntu", Architecture: architecture, Release: release}}, nil
}

func NewImageFetcher(distro, release, architecture, basepath string) (ImageFetcher, error) {
	switch distro {
	case "ubuntu":
		{
			return NewUbuntuImageFetcher(release, architecture, basepath)
		}
	}
	return nil, fmt.Errorf("Not found handler for: %s", distro)
}

func NewImageFetcherHandler(config ImageSource, errChannel *chan error) (*ImageFetchHandler, error) {
	var handler ImageFetchHandler
	var imageChannel chan Image
	dir, err := ioutil.TempDir("", "images")
	if err != nil {
		return nil, err
	}

	imageChannel = make(chan Image)

	handler.ImageBasePath = dir
	handler.Config = config
	handler.WaitGroup = &sync.WaitGroup{}
	handler.ImagesChannel = &imageChannel
	handler.ErrorChannel = errChannel

	for distro, config := range config.DistroSources {
		for release, releaseConfig := range config.Releases {
			for _, architecture := range releaseConfig.Architectures {
				fetcher, err := NewImageFetcher(distro, release, architecture, handler.ImageBasePath)
				if err != nil {
					return nil, err
				}
				handler.Fetchers = append(handler.Fetchers, fetcher)
			}
		}
	}

	return &handler, nil
}

func (handler *ImageFetchHandler) Handle() {
	for {
		<-time.After(time.Second * 3)
		for _, fetcher := range handler.Fetchers {
			handler.WaitGroup.Add(1)
			go fetcher.Fetch(handler.WaitGroup, handler.ImagesChannel, handler.ErrorChannel)
		}
		handler.WaitGroup.Wait()
	}
}

func main() {

	var errChannel chan error
	errChannel = make(chan error)

	config, err := NewConfigFromFile("./example-config.yml")
	if err != nil {
		panic(err)
	}

	fetcher, err := NewImageFetcherHandler(config.ImageSources, &errChannel)
	if err != nil {
		panic(err)
	}

	uploader, err := NewImageUploadHandler("uk.linaro.cloud", "/home/niedbalski/.config/openstack/clouds.yml", &errChannel)
	if err != nil {
		panic(err)
	}

	go fetcher.Handle()
	go uploader.Handle(fetcher.ImagesChannel)

	for {
		select {
		case err := <-errChannel:
			{
				log.Errorf("Error handling image upload: %s", errors.Trace(err))
			}
		}
	}
}
