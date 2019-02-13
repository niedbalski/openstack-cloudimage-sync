package main

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"gopkg.in/niedbalski/goose.v3/client"
	"gopkg.in/niedbalski/goose.v3/glance"
	"gopkg.in/niedbalski/goose.v3/identity"
	"os"
)

type GlanceImageUploader struct {
	Config          *Cloud
	Client          *glance.Client
}

type ImageUploadResult struct {
	Error error
	Image *glance.CreateImageResponse
}

func NewGlanceImageUploader(cloudName string, cloudConfigPath string) (*GlanceImageUploader, error) {
	var credentials identity.Credentials
	var newClient client.AuthenticatingClient

	c, err := NewCloudConfigFromFile(cloudConfigPath)
	if err != nil {
		return nil, err
	}

	config, err := c.GetByName(cloudName)
	if err != nil {
		return nil, err
	}

	credentials.URL = config.Auth.AuthURL
	credentials.ProjectDomain = config.Auth.ProjectDomainName
	credentials.UserDomain = config.Auth.UserDomainName
	credentials.Region = config.Region
	credentials.User = config.Auth.Username
	credentials.Secrets = config.Auth.Password
	credentials.TenantName = config.Auth.ProjectName

	if config.IdentityAPIVersion == "3" {
		newClient = client.NewClient(&credentials, identity.AuthUserPassV3, nil)
	} else {
		newClient = client.NewClient(&credentials, identity.AuthUserPass, nil)
	}

	newClient.SetRequiredServiceTypes([]string{"image"})
	newClient.Authenticate()

	return &GlanceImageUploader{Config: config, Client: glance.New(newClient)}, nil
}

func (uploader *GlanceImageUploader) HasImage(imageName string) bool {
	images, err := uploader.Client.ListImagesV2()
	if err != nil {
		return false
	}

	for _, image := range images {
		if image.Name == imageName {
			return true
		}
	}
	return false
}

func (uploader *GlanceImageUploader) FilterFetchers(fetchers []ImageFetcher) []ImageFetcher {
	var filtered []ImageFetcher
	for _, fetcher := range fetchers {
		if !uploader.HasImage(fetcher.GetName()) {
			log.Infof("Adding %s to the list of images to fetch", fetcher.GetName())
			filtered = append(filtered, fetcher)
		}
	}
	log.Infof("Found %d new images to fetch", len(filtered))
	return filtered
}

func (uploader *GlanceImageUploader) Upload(image *Image, errChannel *chan error) {
	imageName := fmt.Sprintf("%s-%s-%s", image.Distro, image.Release, image.Architecture)
	log.Infof("Uploading image:%s to glance", imageName)

	file, err := os.Open(image.File.Name())
	if err != nil {
		*errChannel <- err
		return
	}

	defer file.Close()

	uploadedImage, err := uploader.Client.CreateImageFromFile(file, glance.ImageOpts{
		Name:            imageName,
		DiskFormat:      "qcow2",
		ContainerFormat: "bare",
		Visibility:      "public",
	})

	if err != nil {
		*errChannel <- err
		return
	}

	log.Info("Image name: %s, ID: %s - uploaded to glance at %s", uploadedImage.ID, uploadedImage.Name, uploadedImage.UpdatedAt)
}
