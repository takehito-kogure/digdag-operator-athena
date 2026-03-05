package pro.civitaspo.digdag.plugin.athena.aws.s3


import pro.civitaspo.digdag.plugin.athena.aws.{Aws, AwsService}
import software.amazon.awssdk.core.sync.ResponseTransformer
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{DeleteObjectRequest, GetObjectRequest, GetObjectResponse, ListObjectsV2Request}

import scala.jdk.CollectionConverters._


case class S3(aws: Aws)
    extends AwsService(aws)
        with java.io.Closeable
{
    private var s3ClientOpt: Option[S3Client] = None

    private def s3Client: S3Client = s3ClientOpt.getOrElse {
        val b = aws.configureClient(S3Client.builder())
        aws.httpClientOption.foreach(b.httpClient)
        val c = b.build()
        s3ClientOpt = Some(c)
        c
    }

    override def close(): Unit = s3ClientOpt.foreach(_.close())

    def withS3[A](f: S3Client => A): A = f(s3Client)

    def readObject(location: String): String =
    {
        readObject(uri = S3Uri(location))
    }

    def readObject(uri: S3Uri): String =
    {
        readObject(bucket = uri.bucket, key = uri.key)
    }

    def readObject(bucket: String,
                   key: String): String =
    {
        withS3(_.getObject(
            GetObjectRequest.builder().bucket(bucket).key(key).build(),
            ResponseTransformer.toBytes[GetObjectResponse]()
        )).asUtf8String()
    }

    def ls(location: String): Seq[S3Uri] =
    {
        ls(uri = S3Uri(location))
    }

    def ls(uri: S3Uri): Seq[S3Uri] =
    {
        ls(bucket = uri.bucket, prefix = uri.key)
    }

    def ls(bucket: String,
           prefix: String): Seq[S3Uri] =
    {
        withS3(_.listObjectsV2Paginator(
            ListObjectsV2Request.builder().bucket(bucket).prefix(prefix).build()
        )).contents().asScala.toSeq.map(o => S3Uri(bucket = bucket, key = o.key()))
    }

    def rm(location: String): Unit =
    {
        rm(uri = S3Uri(location))
    }

    def rm(uri: S3Uri): Unit =
    {
        rm(bucket = uri.bucket, key = uri.key)
    }

    def rm(bucket: String,
           key: String): Unit =
    {
        withS3(_.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(key).build()))
    }

    def rm_r(location: String): Seq[S3Uri] =
    {
        rm_r(uri = S3Uri(location))
    }

    def rm_r(uri: S3Uri): Seq[S3Uri] =
    {
        rm_r(bucket = uri.bucket, prefix = uri.key)
    }

    def rm_r(bucket: String,
             prefix: String): Seq[S3Uri] =
    {
        withS3(_.listObjectsV2Paginator(
            ListObjectsV2Request.builder().bucket(bucket).prefix(prefix).build()
        )).contents().asScala.map { obj =>
            val uri = S3Uri(bucket = bucket, key = obj.key())
            rm(uri = uri)
            uri
        }.toSeq
    }

    def hasObjects(location: String): Boolean =
    {
        hasObjects(uri = S3Uri(location))
    }

    def hasObjects(uri: S3Uri): Boolean =
    {
        hasObjects(bucket = uri.bucket, prefix = uri.key)
    }

    def hasObjects(bucket: String,
                   prefix: String): Boolean =
    {
        withS3 { s3 =>
            s3.listObjectsV2(ListObjectsV2Request.builder()
                                 .bucket(bucket)
                                 .prefix(prefix)
                                 .maxKeys(1)
                                 .build()).keyCount() > 0
        }
    }
}
