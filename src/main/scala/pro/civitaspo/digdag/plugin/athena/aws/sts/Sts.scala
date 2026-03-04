package pro.civitaspo.digdag.plugin.athena.aws.sts


import com.amazonaws.auth.BasicSessionCredentials
import com.amazonaws.services.securitytoken.{AWSSecurityTokenService, AWSSecurityTokenServiceClientBuilder}
import com.amazonaws.services.securitytoken.model.{AssumeRoleRequest, GetCallerIdentityRequest, PolicyDescriptorType}
import pro.civitaspo.digdag.plugin.athena.aws.{Aws, AwsService}

import scala.jdk.CollectionConverters._


case class Sts(aws: Aws)
    extends AwsService(aws)
        with java.io.Closeable
{
    private var stsClientOpt: Option[AWSSecurityTokenService] = None

    private def stsClient: AWSSecurityTokenService = stsClientOpt.getOrElse {
        val c = aws.buildService(AWSSecurityTokenServiceClientBuilder.standard())
        stsClientOpt = Some(c)
        c
    }

    override def close(): Unit = stsClientOpt.foreach(_.shutdown())

    def withSts[A](f: AWSSecurityTokenService => A): A = f(stsClient)

    def getCallerIdentityAccountId: String =
    {
        withSts(_.getCallerIdentity(new GetCallerIdentityRequest())).getAccount
    }

    def assumeRole(roleSessionName: String,
                   roleArn: String,
                   durationSeconds: Int,
                   externalId: Option[String] = None,
                   policy: Option[String] = None,
                   policyArns: Option[Seq[PolicyDescriptorType]] = None,
                   serialNumber: Option[String] = None,
                   tokenCode: Option[String] = None): BasicSessionCredentials =
    {
        val req = new AssumeRoleRequest()
        req.setRoleSessionName(roleSessionName)
        req.setRoleArn(roleArn)
        req.setDurationSeconds(durationSeconds)
        externalId.foreach(req.setExternalId)
        policy.foreach(req.setPolicy)
        policyArns.foreach(x => req.setPolicyArns(x.asJava))
        serialNumber.foreach(req.setSerialNumber)
        tokenCode.foreach(req.setTokenCode)

        val c = withSts(_.assumeRole(req)).getCredentials
        new BasicSessionCredentials(c.getAccessKeyId, c.getSecretAccessKey, c.getSessionToken)
    }

}
