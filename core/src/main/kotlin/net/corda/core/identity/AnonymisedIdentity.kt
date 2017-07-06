package net.corda.core.identity

import net.corda.core.serialization.CordaSerializable
import org.bouncycastle.cert.X509CertificateHolder
import java.security.PublicKey
import java.security.cert.CertPath

@CordaSerializable
data class AnonymisedIdentity(override val party: AnonymousParty,
                              override val certificate: X509CertificateHolder,
                              override val certPath: CertPath) : IPartyAndCertificate<AnonymousParty> {
    constructor(certPath: CertPath, certificate: X509CertificateHolder, identity: PublicKey)
            : this(AnonymousParty(identity), certificate, certPath)

    override fun toString(): String = party.toString()
}