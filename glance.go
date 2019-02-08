package main

import (
	log "github.com/sirupsen/logrus"
	"gopkg.in/cheggaaa/pb.v1"
	"gopkg.in/niedbalski/goose.v3/client"
	"gopkg.in/niedbalski/goose.v3/glance"
	"gopkg.in/niedbalski/goose.v3/identity"
)

type GlanceImageUploader struct {
	Config          *Cloud
	Client          *glance.Client
	Results         chan ImageUploadResult
	ProgressBarPool *pb.Pool
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
	results := make(chan ImageUploadResult)

	pool := pb.NewPool()

	return &GlanceImageUploader{Config: config, Client: glance.New(newClient), ProgressBarPool: pool, Results: results}, nil
}

func (uploader *GlanceImageUploader) Upload(image *Image) {
	//imageName := fmt.Sprintf("%s-%s-%s", image.Distro, image.Release, image.Architecture)
	log.Infof("Uploading image:%s to glance", image.File.Name())

	//file, err := os.Open(image.FilePath)
	//if err != nil {
	//	*uploader.Results <- FetchResult{Error: err, Image: image}
	//}
	//
	//defer file.Close()
	//
	//info, err := file.Stat()
	//if err != nil {
	//	*uploader.Results <- FetchResult{Error: err, Image: image}
	//}
	//
	//bar := pb.New(int(info.Size())).SetUnits(pb.U_BYTES)
	//bar.Start()
	//reader := bar.NewProxyReader(file)
	//
	//_, err = uploader.Client.CreateImageFromFile(reader, glance.ImageOpts{
	//	Name:            imageName,
	//	DiskFormat:      "qcow2",
	//	ContainerFormat: "bare",
	//	Visibility:      "public",
	//})
	//
	//
	//if err != nil {
	//	*uploader.Results <- FetchResult{Error: err, Image: image}
	//}
	//
	//bar.Finish()
	//image.Uploaded = true
	//*uploader.Results <- FetchResult{Error: nil, Image: image}
}
