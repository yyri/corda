package net.corda.core.identity

import org.bouncycastle.cert.X509CertificateHolder
import java.security.PublicKey
import java.security.cert.CertPath

/**
 * A party plus the X.509 certificate and path linking the party back to a trust root.
 */
interface IPartyAndCertificate<P : AbstractParty> {
    val party: P
    val certificate: X509CertificateHolder
    val certPath: CertPath
    val owningKey: PublicKey
        get() = party.owningKey
}
