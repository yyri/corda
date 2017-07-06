package net.corda.core.identity

import net.corda.core.serialization.CordaSerializable
import org.bouncycastle.cert.X509CertificateHolder
import java.security.PublicKey
import java.security.cert.CertPath

/**
 * An anonymised party plus the certificate and path linking it back to a trust root. Anonymised party's certificates are
 * expected to be signed by their well known identity, and therefore that should be the immediately preceeding
 * certificate in the path.
 */
@CordaSerializable
data class AnonymousPartyAndCertificate(override val party: AnonymousParty,
                                        override val certificate: X509CertificateHolder,
                                        override val certPath: CertPath) : IPartyAndCertificate<AnonymousParty> {
    constructor(certPath: CertPath, certificate: X509CertificateHolder, identity: PublicKey)
            : this(AnonymousParty(identity), certificate, certPath)

    override fun toString(): String = party.toString()
}