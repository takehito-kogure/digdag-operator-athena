package pro.civitaspo.digdag.plugin.athena.aws.sts


import pro.civitaspo.digdag.plugin.athena.aws.{Aws, AwsService}
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials
import software.amazon.awssdk.services.sts.StsClient
import software.amazon.awssdk.services.sts.model.{AssumeRoleRequest, GetCallerIdentityRequest, PolicyDescriptorType}

import scala.jdk.CollectionConverters._


case class Sts(aws: Aws)
    extends AwsService(aws)
        with java.io.Closeable
{
    private var stsClientOpt: Option[StsClient] = None

    private def stsClient: StsClient = stsClientOpt.getOrElse {
        val b = aws.configureClient(StsClient.builder())
        aws.httpClientOption.foreach(b.httpClient)
        val c = b.build()
        stsClientOpt = Some(c)
        c
    }

    override def close(): Unit =
    {
        stsClientOpt.foreach(_.close())
        stsClientOpt = None
    }

    def withSts[A](f: StsClient => A): A = f(stsClient)

    def getCallerIdentityAccountId: String =
    {
        withSts(_.getCallerIdentity(GetCallerIdentityRequest.builder().build())).account()
    }

    def assumeRole(roleSessionName: String,
                   roleArn: String,
                   durationSeconds: Int,
                   externalId: Option[String] = None,
                   policy: Option[String] = None,
                   policyArns: Option[Seq[PolicyDescriptorType]] = None,
                   serialNumber: Option[String] = None,
                   tokenCode: Option[String] = None): AwsSessionCredentials =
    {
        val builder = AssumeRoleRequest.builder()
            .roleSessionName(roleSessionName)
            .roleArn(roleArn)
            .durationSeconds(durationSeconds)
        externalId.foreach(builder.externalId)
        policy.foreach(builder.policy)
        policyArns.foreach(x => builder.policyArns(x.asJava))
        serialNumber.foreach(builder.serialNumber)
        tokenCode.foreach(builder.tokenCode)

        val c = withSts(_.assumeRole(builder.build())).credentials()
        AwsSessionCredentials.create(c.accessKeyId(), c.secretAccessKey(), c.sessionToken())
    }
}
