package aws

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/credentials"
	aws3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/openimsdk/tools/s3"
)

const (
	minPartSize int64 = 1024 * 1024 * 5        // 5MB（S3 最小分片大小）
	maxPartSize int64 = 1024 * 1024 * 1024 * 5 // 5GB（S3 最大分片大小）
	maxNumSize  int64 = 10000                  // S3 最大分片数量
)

type Config struct {
	Endpoint           string // 自定义端点，用于 S3 兼容存储（如 RustFS、MinIO 等）
	Region             string
	Bucket             string
	BucketURL          string // 公开访问的 Bucket URL（可选，用于生成外部访问链接）
	AccessKeyID        string
	SecretAccessKey    string
	SessionToken       string
	PublicRead         bool // 是否公开读取
	InsecureSkipVerify bool // 是否跳过 TLS 证书验证（用于自签名证书）
}

func NewAws(conf Config) (*Aws, error) {
	cfg := aws.Config{
		Region:      conf.Region,
		Credentials: credentials.NewStaticCredentialsProvider(conf.AccessKeyID, conf.SecretAccessKey, conf.SessionToken),
	}

	// 构建 S3 客户端选项
	var opts []func(*aws3.Options)
	// presign 使用的端点，优先外部 bucketURL（希望对外暴露的域名），否则退回内部 endpoint
	var presignEndpoint string

	// 支持自定义 endpoint（S3 兼容存储：RustFS、MinIO、Ceph 等）
	if conf.Endpoint != "" {
		// 检查是否需要自定义 HTTP 客户端
		// 1. HTTP endpoint 需要禁用 TLS
		// 2. HTTPS + InsecureSkipVerify 需要跳过证书验证（用于自签名证书）
		isHTTP := strings.HasPrefix(strings.ToLower(conf.Endpoint), "http://")

		if isHTTP || conf.InsecureSkipVerify {
			customHTTPClient := &http.Client{
				Timeout: time.Minute * 10, // 设置合理的超时时间
				Transport: &http.Transport{
					TLSClientConfig: &tls.Config{
						InsecureSkipVerify: isHTTP || conf.InsecureSkipVerify,
					},
					MaxIdleConns:        100,
					MaxIdleConnsPerHost: 100,
					IdleConnTimeout:     90 * time.Second,
				},
			}
			cfg.HTTPClient = customHTTPClient
		}

		// AWS 官方推荐：使用 BaseEndpoint + UsePathStyle 支持 S3 兼容存储
		// 参考: https://docs.aws.amazon.com/sdk-for-go/v2/developer-guide/configure-endpoints.html
		// 参考: https://github.com/aws/aws-sdk-go-v2/issues/2573
		opts = append(opts, func(o *aws3.Options) {
			o.BaseEndpoint = aws.String(conf.Endpoint)
			o.UsePathStyle = true // S3 兼容存储必须使用 path-style（如 MinIO、RustFS、Ceph）
		})
		presignEndpoint = conf.Endpoint
	}

	client := aws3.NewFromConfig(cfg, opts...)

	// 处理 BucketURL
	bucketURL := conf.BucketURL
	if bucketURL == "" && conf.Endpoint != "" {
		// 如果没有指定 BucketURL，则根据 endpoint 和 bucket 构建
		bucketURL = strings.TrimSuffix(conf.Endpoint, "/") + "/" + conf.Bucket
	}
	if bucketURL != "" && !strings.HasSuffix(bucketURL, "/") {
		bucketURL += "/"
	}
	if bucketURL != "" {
		presignEndpoint = strings.TrimSuffix(bucketURL, "/")
	}

	// Debug log to verify config propagation.
	log.Printf("[aws] NewAws config endpoint=%q bucketURL=%q rawBucketURL=%q bucket=%q", conf.Endpoint, bucketURL, conf.BucketURL, conf.Bucket)

	// presign 客户端：使用对外域名（bucketURL）做签名，保证上传/下载用同一 Host，不破坏签名
	var presignOpts []func(*aws3.Options)
	if presignEndpoint != "" {
		presignOpts = append(presignOpts, func(o *aws3.Options) {
			o.BaseEndpoint = aws.String(presignEndpoint)
			o.UsePathStyle = true
		})
	}
	presignClient := aws3.NewPresignClient(aws3.NewFromConfig(cfg, presignOpts...))

	return &Aws{
		bucket:     conf.Bucket,
		bucketURL:  bucketURL,
		endpoint:   conf.Endpoint,
		client:     client,
		presign:    presignClient,
		publicRead: conf.PublicRead,
	}, nil
}

// replaceEndpointURL 将内部 endpoint URL 替换为外部 bucketURL
// 例如: http://rustfs:9000/openim/path -> https://static.mixchat.cc/path
func (a *Aws) replaceEndpointURL(rawURL string) string {
	if a.endpoint == "" || a.bucketURL == "" {
		return rawURL
	}
	// 构建内部 URL 前缀（endpoint + bucket）
	internalPrefix := strings.TrimSuffix(a.endpoint, "/") + "/" + a.bucket
	// 外部 URL 前缀
	externalPrefix := strings.TrimSuffix(a.bucketURL, "/")
	// 替换
	return strings.Replace(rawURL, internalPrefix, externalPrefix, 1)
}

type Aws struct {
	bucket     string
	bucketURL  string
	endpoint   string // 内部 endpoint，用于 URL 替换
	client     *aws3.Client
	presign    *aws3.PresignClient
	publicRead bool
}

func (a *Aws) Engine() string {
	return "aws"
}

func (a *Aws) PartLimit() (*s3.PartLimit, error) {
	return &s3.PartLimit{
		MinPartSize: minPartSize,
		MaxPartSize: maxPartSize,
		MaxNumSize:  maxNumSize,
	}, nil
}

func (a *Aws) formatETag(etag string) string {
	return strings.Trim(etag, `"`)
}

func (a *Aws) publicACL() *types.ObjectCannedACL {
	if !a.publicRead {
		return nil
	}
	acl := types.ObjectCannedACLPublicRead
	return &acl
}

func (a *Aws) PartSize(ctx context.Context, size int64) (int64, error) {
	if size <= 0 {
		return 0, errors.New("size must be greater than 0")
	}
	if size > maxPartSize*maxNumSize {
		return 0, fmt.Errorf("aws size must be less than the maximum allowed limit")
	}
	if size <= minPartSize*maxNumSize {
		return minPartSize, nil
	}
	partSize := size / maxNumSize
	if size%maxNumSize != 0 {
		partSize++
	}
	return partSize, nil
}

func (a *Aws) IsNotFound(err error) bool {
	var respErr *awshttp.ResponseError
	if !errors.As(err, &respErr) {
		return false
	}
	if respErr == nil || respErr.Response == nil {
		return false
	}
	return respErr.Response.StatusCode == http.StatusNotFound
}

func (a *Aws) PresignedPutObject(ctx context.Context, name string, expire time.Duration, opt *s3.PutOption) (*s3.PresignedPutResult, error) {
	input := &aws3.PutObjectInput{Bucket: aws.String(a.bucket), Key: aws.String(name)}
	if acl := a.publicACL(); acl != nil {
		input.ACL = *acl
	}
	res, err := a.presign.PresignPutObject(ctx, input, aws3.WithPresignExpires(expire), withDisableHTTPPresignerHeaderV4(nil))
	if err != nil {
		return nil, err
	}
	return &s3.PresignedPutResult{URL: res.URL, Header: res.SignedHeader}, nil
}

func (a *Aws) DeleteObject(ctx context.Context, name string) error {
	_, err := a.client.DeleteObject(ctx, &aws3.DeleteObjectInput{Bucket: aws.String(a.bucket), Key: aws.String(name)})
	return err
}

func (a *Aws) CopyObject(ctx context.Context, src string, dst string) (*s3.CopyObjectInfo, error) {
	res, err := a.client.CopyObject(ctx, &aws3.CopyObjectInput{
		Bucket:     aws.String(a.bucket),
		CopySource: aws.String(a.bucket + "/" + src),
		Key:        aws.String(dst),
	})
	if err != nil {
		return nil, err
	}
	if res.CopyObjectResult == nil || res.CopyObjectResult.ETag == nil || *res.CopyObjectResult.ETag == "" {
		return nil, errors.New("CopyObject etag is nil")
	}
	return &s3.CopyObjectInfo{
		Key:  dst,
		ETag: a.formatETag(*res.CopyObjectResult.ETag),
	}, nil
}

func (a *Aws) StatObject(ctx context.Context, name string) (*s3.ObjectInfo, error) {
	res, err := a.client.HeadObject(ctx, &aws3.HeadObjectInput{Bucket: aws.String(a.bucket), Key: aws.String(name)})
	if err != nil {
		return nil, err
	}
	if res.ETag == nil || *res.ETag == "" {
		return nil, errors.New("GetObjectAttributes etag is nil")
	}
	if res.ContentLength == nil {
		return nil, errors.New("GetObjectAttributes object size is nil")
	}
	info := &s3.ObjectInfo{
		ETag: a.formatETag(*res.ETag),
		Key:  name,
		Size: *res.ContentLength,
	}
	if res.LastModified == nil {
		info.LastModified = time.Unix(0, 0)
	} else {
		info.LastModified = *res.LastModified
	}
	return info, nil
}

func (a *Aws) InitiateMultipartUpload(ctx context.Context, name string, opt *s3.PutOption) (*s3.InitiateMultipartUploadResult, error) {
	input := &aws3.CreateMultipartUploadInput{Bucket: aws.String(a.bucket), Key: aws.String(name)}
	if acl := a.publicACL(); acl != nil {
		input.ACL = *acl
	}
	res, err := a.client.CreateMultipartUpload(ctx, input)
	if err != nil {
		return nil, err
	}
	if res.UploadId == nil || *res.UploadId == "" {
		return nil, errors.New("CreateMultipartUpload upload id is nil")
	}
	return &s3.InitiateMultipartUploadResult{
		Key:      name,
		Bucket:   a.bucket,
		UploadID: *res.UploadId,
	}, nil
}

func (a *Aws) CompleteMultipartUpload(ctx context.Context, uploadID string, name string, parts []s3.Part) (*s3.CompleteMultipartUploadResult, error) {
	params := &aws3.CompleteMultipartUploadInput{
		Bucket:   aws.String(a.bucket),
		Key:      aws.String(name),
		UploadId: aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: make([]types.CompletedPart, 0, len(parts)),
		},
	}
	for _, part := range parts {
		params.MultipartUpload.Parts = append(params.MultipartUpload.Parts, types.CompletedPart{
			ETag:       aws.String(part.ETag),
			PartNumber: aws.Int32(int32(part.PartNumber)),
		})
	}
	res, err := a.client.CompleteMultipartUpload(ctx, params)
	if err != nil {
		return nil, err
	}
	if res.ETag == nil || *res.ETag == "" {
		return nil, errors.New("CompleteMultipartUpload etag is nil")
	}
	info := &s3.CompleteMultipartUploadResult{
		Key:    name,
		Bucket: a.bucket,
		ETag:   a.formatETag(*res.ETag),
	}
	if res.Location != nil {
		info.Location = *res.Location
	}
	return info, nil
}

func (a *Aws) AbortMultipartUpload(ctx context.Context, uploadID string, name string) error {
	_, err := a.client.AbortMultipartUpload(ctx, &aws3.AbortMultipartUploadInput{Bucket: aws.String(a.bucket), Key: aws.String(name), UploadId: aws.String(uploadID)})
	return err
}

func (a *Aws) ListUploadedParts(ctx context.Context, uploadID string, name string, partNumberMarker int, maxParts int) (*s3.ListUploadedPartsResult, error) {
	params := &aws3.ListPartsInput{
		Bucket:           aws.String(a.bucket),
		Key:              aws.String(name),
		UploadId:         aws.String(uploadID),
		PartNumberMarker: aws.String(strconv.Itoa(partNumberMarker)),
		MaxParts:         aws.Int32(int32(maxParts)),
	}
	res, err := a.client.ListParts(ctx, params)
	if err != nil {
		return nil, err
	}
	info := &s3.ListUploadedPartsResult{
		Key:           name,
		UploadID:      uploadID,
		UploadedParts: make([]s3.UploadedPart, 0, len(res.Parts)),
	}
	if res.MaxParts != nil {
		info.MaxParts = int(*res.MaxParts)
	}
	if res.NextPartNumberMarker != nil {
		info.NextPartNumberMarker, _ = strconv.Atoi(*res.NextPartNumberMarker)
	}
	for _, part := range res.Parts {
		var val s3.UploadedPart
		if part.PartNumber != nil {
			val.PartNumber = int(*part.PartNumber)
		}
		if part.LastModified != nil {
			val.LastModified = *part.LastModified
		}
		if part.Size != nil {
			val.Size = *part.Size
		}
		info.UploadedParts = append(info.UploadedParts, val)
	}
	return info, nil
}

func (a *Aws) AuthSign(ctx context.Context, uploadID string, name string, expire time.Duration, partNumbers []int) (*s3.AuthSignResult, error) {
	res := &s3.AuthSignResult{
		Parts: make([]s3.SignPart, 0, len(partNumbers)),
	}
	params := &aws3.UploadPartInput{
		Bucket:   aws.String(a.bucket),
		Key:      aws.String(name),
		UploadId: aws.String(uploadID),
	}
	opt := aws3.WithPresignExpires(expire)
	for _, number := range partNumbers {
		params.PartNumber = aws.Int32(int32(number))
		val, err := a.presign.PresignUploadPart(ctx, params, opt)
		if err != nil {
			return nil, err
		}
		u, err := url.Parse(val.URL)
		if err != nil {
			return nil, err
		}
		query := u.Query()
		u.RawQuery = ""
		urlstr := u.String()
		if res.URL == "" {
			res.URL = urlstr
		}
		if res.URL == urlstr {
			urlstr = ""
		}
		res.Parts = append(res.Parts, s3.SignPart{
			PartNumber: number,
			URL:        urlstr,
			Query:      query,
			Header:     val.SignedHeader,
		})
	}
	return res, nil
}

func (a *Aws) AccessURL(ctx context.Context, name string, expire time.Duration, opt *s3.AccessURLOption) (string, error) {
	params := &aws3.GetObjectInput{
		Bucket: aws.String(a.bucket),
		Key:    aws.String(name),
	}
	res, err := a.presign.PresignGetObject(ctx, params, aws3.WithPresignExpires(expire), withDisableHTTPPresignerHeaderV4(opt))
	if err != nil {
		return "", err
	}
	// 将内部 endpoint URL 替换为外部 bucketURL
	return a.replaceEndpointURL(res.URL), nil
}

func (a *Aws) FormData(ctx context.Context, name string, size int64, contentType string, duration time.Duration) (*s3.FormData, error) {
	return nil, errors.New("aws does not currently support form data file uploads")
}

func withDisableHTTPPresignerHeaderV4(opt *s3.AccessURLOption) func(options *aws3.PresignOptions) {
	return func(options *aws3.PresignOptions) {
		options.Presigner = &disableHTTPPresignerHeaderV4{
			opt:       opt,
			presigner: options.Presigner,
		}
	}
}

type disableHTTPPresignerHeaderV4 struct {
	opt       *s3.AccessURLOption
	presigner aws3.HTTPPresignerV4
}

func (d *disableHTTPPresignerHeaderV4) PresignHTTP(ctx context.Context, credentials aws.Credentials, r *http.Request, payloadHash string, service string, region string, signingTime time.Time, optFns ...func(*v4.SignerOptions)) (url string, signedHeader http.Header, err error) {
	optFns = append(optFns, func(options *v4.SignerOptions) {
		options.DisableHeaderHoisting = true
	})
	r.Header.Del("Amz-Sdk-Request")
	d.setOption(r.URL)
	return d.presigner.PresignHTTP(ctx, credentials, r, payloadHash, service, region, signingTime, optFns...)
}

func (d *disableHTTPPresignerHeaderV4) setOption(u *url.URL) {
	if d.opt == nil {
		return
	}
	query := u.Query()
	if d.opt.ContentType != "" {
		query.Set("response-content-type", d.opt.ContentType)
	}
	if d.opt.Filename != "" {
		query.Set("response-content-disposition", `attachment; filename*=UTF-8''`+url.PathEscape(d.opt.Filename))
	}
	u.RawQuery = query.Encode()
}
