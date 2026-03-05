package pro.civitaspo.digdag.plugin.athena.aws


import com.google.common.base.Optional
import io.digdag.client.config.ConfigException
import pro.civitaspo.digdag.plugin.athena.aws.athena.Athena
import pro.civitaspo.digdag.plugin.athena.aws.glue.Glue
import pro.civitaspo.digdag.plugin.athena.aws.s3.S3
import pro.civitaspo.digdag.plugin.athena.aws.sts.Sts
import software.amazon.awssdk.auth.credentials._
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder
import software.amazon.awssdk.http.SdkHttpClient
import software.amazon.awssdk.http.apache.{ApacheHttpClient, ProxyConfiguration}
import software.amazon.awssdk.profiles.ProfileFile
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.regions.providers._


case class Aws(conf: AwsConf)
    extends java.io.Closeable
{
    private[aws] def configureClient[B <: AwsClientBuilder[B, _]](builder: B): B =
    {
        builder.credentialsProvider(credentialsProvider)
        if (conf.endpoint.isPresent) {
            builder.endpointOverride(new java.net.URI(conf.endpoint.get()))
        }
        else {
            builder.region(Region.of(region))
        }
        builder
    }

    // Lazily initialized once and closed in close()
    private var httpClientOpt: Option[SdkHttpClient] = None
    private var credentialsProviderOpt: Option[AwsCredentialsProvider] = None

    private[aws] def httpClientOption: Option[SdkHttpClient] =
    {
        if (!conf.useHttpProxy) return None
        httpClientOpt.orElse {
            val c = apacheHttpClient
            httpClientOpt = Some(c)
            httpClientOpt
        }
    }

    private var s3Opt: Option[S3] = None
    private var stsOpt: Option[Sts] = None
    private var athenaOpt: Option[Athena] = None
    private var glueOpt: Option[Glue] = None

    lazy val s3: S3 = { val svc = S3(this); s3Opt = Some(svc); svc }
    lazy val sts: Sts = { val svc = Sts(this); stsOpt = Some(svc); svc }
    lazy val athena: Athena = { val svc = Athena(this); athenaOpt = Some(svc); svc }
    lazy val glue: Glue = { val svc = Glue(this); glueOpt = Some(svc); svc }

    override def close(): Unit =
    {
        s3Opt.foreach(_.close())
        stsOpt.foreach(_.close())
        athenaOpt.foreach(_.close())
        glueOpt.foreach(_.close())
        httpClientOpt.foreach(_.close())
        credentialsProviderOpt.foreach {
            case c: java.io.Closeable => c.close()
            case _                    =>
        }
    }

    lazy val region: String =
    {
        conf.region.or {
            conf.authMethod match {
                case "env"        => resolveRegion(new SystemSettingsRegionProvider())
                case "instance"   => resolveRegion(new DefaultAwsRegionProviderChain())
                case "profile"    => resolveRegion(new AwsProfileRegionProvider())
                case "properties" => resolveRegion(new SystemSettingsRegionProvider())
                case _            => resolveRegion(new DefaultAwsRegionProviderChain())
            }
        }
    }

    private def resolveRegion(provider: AwsRegionProvider): String =
    {
        try provider.getRegion.id()
        catch { case _: Exception => Region.US_EAST_1.id() }
    }

    private[aws] def credentialsProvider: AwsCredentialsProvider =
    {
        credentialsProviderOpt.getOrElse {
            val p = if (!conf.roleArn.isPresent) standardCredentialsProvider
                    else assumeRoleCredentialsProvider(standardCredentialsProvider)
            credentialsProviderOpt = Some(p)
            p
        }
    }

    private def standardCredentialsProvider: AwsCredentialsProvider =
    {
        conf.authMethod match {
            case "basic"              => basicAuthMethodCredentialsProvider
            case "env"                => envAuthMethodCredentialsProvider
            case "instance"           => instanceAuthMethodCredentialsProvider
            case "profile"            => profileAuthMethodCredentialsProvider
            case "properties"         => propertiesAuthMethodCredentialsProvider
            case "anonymous"          => anonymousAuthMethodCredentialsProvider
            case "session"            => sessionAuthMethodCredentialsProvider
            case "web_identity_token" => webIdentityTokenAuthMethodCredentialsProvider
            case _                    =>
                throw new ConfigException(
                    s"""auth_method: "${conf.authMethod}" is not supported. available `auth_method`s are "basic", "env", "instance", "profile", "properties", "anonymous", or "session"."""
                    )
        }
    }

    private def assumeRoleCredentialsProvider(provider: AwsCredentialsProvider): AwsCredentialsProvider =
    {
        val tmpAws = Aws(this.conf.copy(roleArn = Optional.absent()))
        try {
            val cred = tmpAws.sts.assumeRole(
                roleArn = conf.roleArn.get(),
                roleSessionName = conf.roleSessionName,
                durationSeconds = conf.assumeRoleTimeoutDuration.getDuration.getSeconds.toInt
            )
            StaticCredentialsProvider.create(cred)
        }
        finally {
            tmpAws.close()
        }
    }

    private def basicAuthMethodCredentialsProvider: AwsCredentialsProvider =
    {
        if (!conf.accessKeyId.isPresent) throw new ConfigException(s"""`access_key_id` must be set when `auth_method` is "${conf.authMethod}".""")
        if (!conf.secretAccessKey.isPresent) throw new ConfigException(s"""`secret_access_key` must be set when `auth_method` is "${conf.authMethod}".""")
        StaticCredentialsProvider.create(
            AwsBasicCredentials.create(conf.accessKeyId.get(), conf.secretAccessKey.get())
        )
    }

    private def envAuthMethodCredentialsProvider: AwsCredentialsProvider =
    {
        if (!conf.isAllowedAuthMethodEnv) throw new ConfigException(s"""auth_method: "${conf.authMethod}" is not allowed.""")
        EnvironmentVariableCredentialsProvider.create()
    }

    private def instanceAuthMethodCredentialsProvider: AwsCredentialsProvider =
    {
        if (!conf.isAllowedAuthMethodInstance) throw new ConfigException(s"""auth_method: "${conf.authMethod}" is not allowed.""")
        DefaultCredentialsProvider.builder().build()
    }

    private def profileAuthMethodCredentialsProvider: AwsCredentialsProvider =
    {
        if (!conf.isAllowedAuthMethodProfile) throw new ConfigException(s"""auth_method: "${conf.authMethod}" is not allowed.""")
        if (!conf.profileFile.isPresent) {
            return ProfileCredentialsProvider.builder()
                .profileName(conf.profileName)
                .build()
        }
        ProfileCredentialsProvider.builder()
            .profileFile(ProfileFile.builder()
                             .content(java.nio.file.Paths.get(conf.profileFile.get()))
                             .`type`(ProfileFile.Type.CREDENTIALS)
                             .build())
            .profileName(conf.profileName)
            .build()
    }

    private def propertiesAuthMethodCredentialsProvider: AwsCredentialsProvider =
    {
        if (!conf.isAllowedAuthMethodProperties) throw new ConfigException(s"""auth_method: "${conf.authMethod}" is not allowed.""")
        SystemPropertyCredentialsProvider.create()
    }

    private def anonymousAuthMethodCredentialsProvider: AwsCredentialsProvider =
    {
        AnonymousCredentialsProvider.create()
    }

    private def sessionAuthMethodCredentialsProvider: AwsCredentialsProvider =
    {
        if (!conf.accessKeyId.isPresent) throw new ConfigException(s"""`access_key_id` must be set when `auth_method` is "${conf.authMethod}".""")
        if (!conf.secretAccessKey.isPresent) throw new ConfigException(s"""`secret_access_key` must be set when `auth_method` is "${conf.authMethod}".""")
        if (!conf.sessionToken.isPresent) throw new ConfigException(s"""`session_token` must be set when `auth_method` is "${conf.authMethod}".""")
        StaticCredentialsProvider.create(
            AwsSessionCredentials.create(conf.accessKeyId.get(), conf.secretAccessKey.get(), conf.sessionToken.get())
        )
    }

    private def webIdentityTokenAuthMethodCredentialsProvider: AwsCredentialsProvider =
    {
        if (!conf.isAllowedAuthMethodWebIdentityToken) throw new ConfigException(s"""auth_method: "${conf.authMethod}" is not allowed.""")
        if (!conf.webIdentityTokenFile.or(conf.defaultWebIdentityTokenFile).isPresent) throw new ConfigException(s"""`web_identity_token_file` or `athena.allow_auth_method_web_identity_token` (system) must be set when `auth_method` is "${conf.authMethod}".""")
        if (!conf.webIdentityRoleArn.or(conf.defaultWebIdentityRoleArn).isPresent) throw new ConfigException(s"""`web_identity_role_arn` or `athena.allow_auth_method_web_identity_role_arn` (system) must be set when `auth_method` is "${conf.authMethod}".""")
        WebIdentityTokenFileCredentialsProvider.builder()
            .webIdentityTokenFile(java.nio.file.Paths.get(conf.webIdentityTokenFile.or(conf.defaultWebIdentityTokenFile).get()))
            .roleArn(conf.webIdentityRoleArn.or(conf.defaultWebIdentityRoleArn).get())
            .roleSessionName(conf.roleSessionName)
            .build()
    }

    private def apacheHttpClient: SdkHttpClient =
    {
        val host: String = conf.httpProxy.getSecret("host")
        val port: Optional[String] = conf.httpProxy.getSecretOptional("port")
        val scheme: String = conf.httpProxy.getSecretOptional("scheme").or("https")
        val user: Optional[String] = conf.httpProxy.getSecretOptional("user")
        val password: Optional[String] = conf.httpProxy.getSecretOptional("password")

        val portPart = if (port.isPresent) s":${port.get()}" else ""
        val proxyUri = new java.net.URI(s"$scheme://$host$portPart")

        val proxyBuilder = ProxyConfiguration.builder().endpoint(proxyUri)
        if (user.isPresent) proxyBuilder.username(user.get())
        if (password.isPresent) proxyBuilder.password(password.get())

        ApacheHttpClient.builder()
            .proxyConfiguration(proxyBuilder.build())
            .build()
    }
}
