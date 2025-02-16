package sss_test

import (
	"log"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/wzshiming/sss"
)

var (
	s      *sss.SSS
	bucket = "sss-test-bucket"

	url = `sss://minioadmin:minioadmin@` + bucket + `.region?forcepathstyle=true&secure=false&chunksize=` + strconv.Itoa(5*1024*1024) + `&regionendpoint=http://127.0.0.1:9000`
)

func TestMain(m *testing.M) {
	var err error
	s, err = sss.NewSSS(sss.WithURL(url))
	if err != nil {
		log.Fatal(err)
	}

	err = exec.Command("docker", "compose", "up", "-d").Run()
	if err != nil {
		log.Fatal(err)
	}

	time.Sleep(1 * time.Second)

	_, err = s.S3().HeadBucket(&s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	})

	if err != nil {
		if s3Err, ok := err.(awserr.Error); ok && s3Err.Code() == "NotFound" {
			_, err = s.S3().CreateBucket(&s3.CreateBucketInput{
				Bucket: aws.String(bucket),
			})
			if err != nil {
				log.Fatal(err)
			}
		} else {
			log.Fatal(err)
		}

		s.S3().CreateBucket(&s3.CreateBucketInput{
			Bucket: aws.String(bucket),
		})
	}

	code := m.Run()
	if code != 0 {
		os.Exit(code)
	}

	err = exec.Command("docker", "compose", "down").Run()
	if err != nil {
		log.Fatal(err)
	}
}
