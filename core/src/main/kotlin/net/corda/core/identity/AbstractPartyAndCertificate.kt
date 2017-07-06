package net.corda.core.identity

import net.corda.core.serialization.CordaSerializable
import org.bouncycastle.cert.X509CertificateHolder
import java.security.PublicKey
import java.security.cert.CertPath

/**
 * A full party plus the X.509 certificate and path linking the party back to a trust root. Equality of
 * [PartyAndCertificate] instances is based on the party only, as certificate and path are data associated with the party,
 * not part of the identifier themselves.
 */
@CordaSerializable
abstract class AbstractPartyAndCertificate<P : AbstractParty>() {
    abstract val party: P
    abstract val certificate: X509CertificateHolder
    abstract val certPath: CertPath
    val owningKey: PublicKey
        get() = party.owningKey

    override fun equals(other: Any?): Boolean {
        return if (other is AbstractPartyAndCertificate<*>)
            party == other.party
        else
            false
    }

    override fun hashCode(): Int = party.hashCode()
    override fun toString(): String = party.toString()
}
