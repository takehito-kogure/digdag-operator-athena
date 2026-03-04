package pro.civitaspo.digdag.plugin.athena.aws.s3


case class S3Uri(bucket: String, key: String)

object S3Uri
{
    def apply(path: String): S3Uri =
    {
        val uri = new java.net.URI(path)
        require(uri.getScheme == "s3", s"Not an s3:// URI: $path")
        val bucket = uri.getHost
        val key = uri.getPath.stripPrefix("/")
        S3Uri(bucket = bucket, key = key)
    }
}
