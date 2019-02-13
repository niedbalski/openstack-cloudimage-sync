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
	"os/signal"
	"sync"
	"syscall"
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
			case image := <-*images: {
				go uploader.Uploader.Upload(&image, uploader.ErrorChannel)
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
	Fetch(imageURL string, errorChannel *chan error)
	Cleanup() error
	GetName() string
	GetImageURL() (string, error)
	GetErrorChannel() *chan error
}

type BaseImageFetcher struct {
	ImageBasePath, Name, Release, Architecture string
	ErrorChannel                               *chan error
	ImagesChannel                              *chan Image
	WaitGroup                                  *sync.WaitGroup
}

func (fetcher *BaseImageFetcher) GetErrorChannel() *chan error {
	return fetcher.ErrorChannel
}

func (fetcher *BaseImageFetcher) GetName() string {
	return fmt.Sprintf("%s-%s-%s", fetcher.Name, fetcher.Release, fetcher.Architecture)
}

func (fetcher *BaseImageFetcher) Cleanup() error {
	log.Infof("Cleaning up base image directory: %s for fetcher: %s", fetcher.ImageBasePath, fetcher.GetName())
	return os.RemoveAll(fetcher.ImageBasePath)
}

func (fetcher *BaseImageFetcher) Fetch(imageURL string, errChannel *chan error) {
	var image Image

	defer fetcher.WaitGroup.Done()

	image = Image{Release: fetcher.Release, Distro: fetcher.Name, Architecture: fetcher.Architecture}

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

	*fetcher.ImagesChannel <- image
}

type DebianImageFetcher struct {
	BaseImageFetcher
}

const (
	DebianStableRelease = "current-9"
	DebianTestingRelease = "testing"
	DebianBaseOpenstackImagesURL = "https://cdimage.debian.org/cdimage/openstack"
)

var DebianReleaseMap = map[string]string{ DebianStableRelease: "9", DebianTestingRelease: "testing" }

func (fetcher *DebianImageFetcher) GetImageURL() (string, error) {
	return fmt.Sprintf("%s/%s/debian-%s-openstack-%s.qcow2", DebianBaseOpenstackImagesURL, fetcher.Release, DebianReleaseMap[fetcher.Release], fetcher.Architecture), nil
}

func NewDebianImageFetcher(release, architecture, basepath string, wg *sync.WaitGroup, imagesChannel *chan Image, errorChannel *chan error) (*DebianImageFetcher, error) {
	dir, err := ioutil.TempDir(basepath, "debian")
	if err != nil {
		return nil, err
	}

	if release == "stretch" || release == "latest" {
		release = DebianStableRelease
	}

	if release == "buster" {
		release = DebianTestingRelease
	}

	return &DebianImageFetcher{BaseImageFetcher{
		ErrorChannel:  errorChannel,
		ImagesChannel: imagesChannel,
		WaitGroup:     wg,
		ImageBasePath: dir, Name: "debian", Architecture: architecture, Release: release},
	}, nil

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

func NewUbuntuImageFetcher(release, architecture, basepath string, wg *sync.WaitGroup, imagesChannel *chan Image, errorChannel *chan error) (*UbuntuImageFetcher, error) {
	dir, err := ioutil.TempDir(basepath, "ubuntu")
	if err != nil {
		return nil, err
	}

	if release == "" || release == "latest" {
		release = UbuntuLatestRelease
	}

	return &UbuntuImageFetcher{BaseImageFetcher{
		ErrorChannel:  errorChannel,
		ImagesChannel: imagesChannel,
		WaitGroup:     wg,
		ImageBasePath: dir, Name: "ubuntu", Architecture: architecture, Release: release},
	}, nil
}

func NewImageFetcher(distro, release, architecture, basepath string, wg *sync.WaitGroup, imagesChannel *chan Image, errorChannel *chan error) (ImageFetcher, error) {
	switch distro {
		case "ubuntu": {
			return NewUbuntuImageFetcher(release, architecture, basepath, wg, imagesChannel, errorChannel)
		}
		case "debian": {
			return NewDebianImageFetcher(release, architecture, basepath, wg, imagesChannel, errorChannel)
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
				fetcher, err := NewImageFetcher(distro, release, architecture, handler.ImageBasePath, handler.WaitGroup, handler.ImagesChannel, handler.ErrorChannel)
				if err != nil {
					return nil, err
				}
				handler.Fetchers = append(handler.Fetchers, fetcher)
			}
		}
	}

	return &handler, nil
}

func (handler *ImageFetchHandler) Cleanup() {
	for _, fetcher := range handler.Fetchers {
		fetcher.Cleanup()
	}
	os.RemoveAll(handler.ImageBasePath)
}

func (handler *ImageFetchHandler) Handle(filterFetchers func([]ImageFetcher) []ImageFetcher) {
	for {
		for _, fetcher := range filterFetchers(handler.Fetchers) {
			handler.WaitGroup.Add(1)
			var errorChannel chan error
			errorChannel = make(chan error)

			imageURL, err := fetcher.GetImageURL()
			if err != nil {
				*handler.ErrorChannel <- fmt.Errorf("%s cannot get image url: %s", fetcher.GetName(), err)
				return
			}

			go fetcher.Fetch(imageURL, &errorChannel)
			go func(fetcher ImageFetcher) {
				for {
					select {
						case e := <-errorChannel: {
							*handler.ErrorChannel <- fmt.Errorf("%s fetcher, cannot fetch image url: %s", fetcher.GetName(), e)
						}
					}
				}
			}(fetcher)
		}

		handler.WaitGroup.Wait()
		<-time.After(time.Second * 30)
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

	// Fetch new images
	go fetcher.Handle(uploader.Uploader.FilterFetchers)

	// Wait for new images to be uploaded into glance
	go uploader.Handle(fetcher.ImagesChannel)

	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-c
		fetcher.Cleanup()
		os.Exit(1)
	}()

	// Check for common errors on any of the channels (fetcher/uploader)
	for {
		select {
		case err := <-errChannel:
			{
				log.Errorf("Error handling image fetch/upload: %s", errors.Trace(err))
			}
		}
	}
}
