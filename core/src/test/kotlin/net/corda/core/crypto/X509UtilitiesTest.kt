package net.corda.core.crypto

import net.corda.core.crypto.Crypto.EDDSA_ED25519_SHA512
import net.corda.core.crypto.Crypto.generateKeyPair
import net.corda.core.crypto.X509Utilities.DEFAULT_TLS_SIGNATURE_SCHEME
import net.corda.core.crypto.X509Utilities.createSelfSignedCACertificate
import net.corda.core.internal.div
import net.corda.core.internal.toTypedArray
import net.corda.node.services.config.createKeystoreForCordaNode
import net.corda.node.utilities.*
import net.corda.testing.MEGA_CORP
import net.corda.testing.getTestX509Name
import org.bouncycastle.asn1.x500.X500Name
import org.bouncycastle.asn1.x509.BasicConstraints
import org.bouncycastle.asn1.x509.Extension
import org.bouncycastle.asn1.x509.KeyUsage
import org.bouncycastle.operator.jcajce.JcaContentVerifierProviderBuilder
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.IOException
import java.net.InetAddress
import java.net.InetSocketAddress
import java.nio.file.Path
import java.security.KeyStore
import java.security.PrivateKey
import java.security.SecureRandom
import java.security.cert.Certificate
import java.security.cert.X509Certificate
import java.util.*
import java.util.stream.Stream
import javax.net.ssl.*
import kotlin.concurrent.thread
import kotlin.test.*

class X509UtilitiesTest {
    @Rule
    @JvmField
    val tempFolder: TemporaryFolder = TemporaryFolder()

    @Test
    fun `create valid self-signed CA certificate`() {
        val caKey = generateKeyPair(DEFAULT_TLS_SIGNATURE_SCHEME)
        val caCert = createSelfSignedCACertificate(getTestX509Name("Test Cert"), caKey)
        assertTrue { caCert.subject.commonName == "Test Cert" } // using our subject common name
        assertEquals(caCert.issuer, caCert.subject) //self-signed
        caCert.isValidOn(Date()) // throws on verification problems
        caCert.isSignatureValid(JcaContentVerifierProviderBuilder().build(caKey.public)) // throws on verification problems
        val basicConstraints = BasicConstraints.getInstance(caCert.getExtension(Extension.basicConstraints).parsedValue)
        val keyUsage = KeyUsage.getInstance(caCert.getExtension(Extension.keyUsage).parsedValue)
        assertFalse { keyUsage.hasUsages(5) } // Bit 5 == keyCertSign according to ASN.1 spec (see full comment on KeyUsage property)
        assertNull(basicConstraints.pathLenConstraint) // No length constraint specified on this CA certificate
    }

    @Test
    fun `load and save a PEM file certificate`() {
        val tmpCertificateFile = tempFile("cacert.pem")
        val caKey = generateKeyPair(DEFAULT_TLS_SIGNATURE_SCHEME)
        val caCert = createSelfSignedCACertificate(getTestX509Name("Test Cert"), caKey)
        X509Utilities.saveCertificateAsPEMFile(caCert, tmpCertificateFile)
        val readCertificate = X509Utilities.loadCertificateFromPEMFile(tmpCertificateFile)
        assertEquals(caCert, readCertificate)
    }

    @Test
    fun `create valid server certificate chain`() {
        val caKey = generateKeyPair(DEFAULT_TLS_SIGNATURE_SCHEME)
        val caCert = createSelfSignedCACertificate(getTestX509Name("Test CA Cert"), caKey)
        val subject = getTestX509Name("Server Cert")
        val keyPair = generateKeyPair(DEFAULT_TLS_SIGNATURE_SCHEME)
        val serverCert = X509Utilities.createCertificate(CertificateType.TLS, caCert, caKey, subject, keyPair.public)
        assertTrue { serverCert.subject.toString().contains("CN=Server Cert") } // using our subject common name
        assertEquals(caCert.issuer, serverCert.issuer) // Issued by our CA cert
        serverCert.isValidOn(Date()) // throws on verification problems
        serverCert.isSignatureValid(JcaContentVerifierProviderBuilder().build(caKey.public)) // throws on verification problems
        val basicConstraints = BasicConstraints.getInstance(serverCert.getExtension(Extension.basicConstraints).parsedValue)
        val keyUsage = KeyUsage.getInstance(serverCert.getExtension(Extension.keyUsage).parsedValue)
        assertFalse { keyUsage.hasUsages(5) } // Bit 5 == keyCertSign according to ASN.1 spec (see full comment on KeyUsage property)
        assertNull(basicConstraints.pathLenConstraint) // Non-CA certificate
    }

    @Test
    fun `storing EdDSA key in java keystore`() {
        val tmpKeyStore = tempFile("keystore.jks")

        val keyPair = generateKeyPair(EDDSA_ED25519_SHA512)
        val selfSignCert = createSelfSignedCACertificate(X500Name("CN=Test"), keyPair)

        assertTrue(Arrays.equals(selfSignCert.subjectPublicKeyInfo.encoded, keyPair.public.encoded))

        // Save the EdDSA private key with self sign cert in the keystore.
        val keyStore = loadOrCreateKeyStore(tmpKeyStore, "keystorepass")
        keyStore.setKeyEntry("Key", keyPair.private, "password".toCharArray(),
                Stream.of(selfSignCert).map { it.cert }.toTypedArray())
        keyStore.save(tmpKeyStore, "keystorepass")

        // Load the keystore from file and make sure keys are intact.
        val keyStore2 = loadOrCreateKeyStore(tmpKeyStore, "keystorepass")
        val privateKey = keyStore2.getKey("Key", "password".toCharArray())
        val pubKey = keyStore2.getCertificate("Key").publicKey

        assertNotNull(pubKey)
        assertNotNull(privateKey)
        assertEquals(keyPair.public, pubKey)
        assertEquals(keyPair.private, privateKey)
    }

    @Test
    fun `signing EdDSA key with EcDSA certificate`() {
        val tmpKeyStore = tempFile("keystore.jks")
        val ecDSAKey = generateKeyPair(Crypto.ECDSA_SECP256R1_SHA256)
        val ecDSACert = createSelfSignedCACertificate(X500Name("CN=Test"), ecDSAKey)
        val edDSAKeypair = generateKeyPair(EDDSA_ED25519_SHA512)
        val edDSACert = X509Utilities.createCertificate(CertificateType.TLS, ecDSACert, ecDSAKey, X500Name("CN=TestEdDSA"), edDSAKeypair.public)

        // Save the EdDSA private key with cert chains.
        val keyStore = loadOrCreateKeyStore(tmpKeyStore, "keystorepass")
        keyStore.setKeyEntry("Key", edDSAKeypair.private, "password".toCharArray(),
                Stream.of(ecDSACert, edDSACert).map { it.cert }.toTypedArray())
        keyStore.save(tmpKeyStore, "keystorepass")

        // Load the keystore from file and make sure keys are intact.
        val keyStore2 = loadOrCreateKeyStore(tmpKeyStore, "keystorepass")
        val privateKey = keyStore2.getKey("Key", "password".toCharArray())
        val certs = keyStore2.getCertificateChain("Key")

        val pubKey = certs.last().publicKey

        assertEquals(2, certs.size)
        assertNotNull(pubKey)
        assertNotNull(privateKey)
        assertEquals(edDSAKeypair.public, pubKey)
        assertEquals(edDSAKeypair.private, privateKey)
    }

    @Test
    fun `create full CA keystore`() {
        val tmpKeyStore = tempFile("keystore.jks")
        val tmpTrustStore = tempFile("truststore.jks")

        // Generate Root and Intermediate CA cert and put both into key store and root ca cert into trust store
        createCAKeyStoreAndTrustStore(tmpKeyStore, "keystorepass", "keypass", tmpTrustStore, "trustpass")

        // Load back generated root CA Cert and private key from keystore and check against copy in truststore
        val keyStore = loadKeyStore(tmpKeyStore, "keystorepass")
        val trustStore = loadKeyStore(tmpTrustStore, "trustpass")
        val rootCaCert = keyStore.getCertificate(X509Utilities.CORDA_ROOT_CA) as X509Certificate
        val rootCaPrivateKey = keyStore.getKey(X509Utilities.CORDA_ROOT_CA, "keypass".toCharArray()) as PrivateKey
        val rootCaFromTrustStore = trustStore.getCertificate(X509Utilities.CORDA_ROOT_CA) as X509Certificate
        assertEquals(rootCaCert, rootCaFromTrustStore)
        rootCaCert.checkValidity(Date())
        rootCaCert.verify(rootCaCert.publicKey)

        // Now sign something with private key and verify against certificate public key
        val testData = "12345".toByteArray()
        val caSignature = Crypto.doSign(DEFAULT_TLS_SIGNATURE_SCHEME, rootCaPrivateKey, testData)
        assertTrue { Crypto.isValid(DEFAULT_TLS_SIGNATURE_SCHEME, rootCaCert.publicKey, caSignature, testData) }

        // Load back generated intermediate CA Cert and private key
        val intermediateCaCert = keyStore.getCertificate(X509Utilities.CORDA_INTERMEDIATE_CA) as X509Certificate
        val intermediateCaCertPrivateKey = keyStore.getKey(X509Utilities.CORDA_INTERMEDIATE_CA, "keypass".toCharArray()) as PrivateKey
        intermediateCaCert.checkValidity(Date())
        intermediateCaCert.verify(rootCaCert.publicKey)

        // Now sign something with private key and verify against certificate public key
        val intermediateSignature = Crypto.doSign(DEFAULT_TLS_SIGNATURE_SCHEME, intermediateCaCertPrivateKey, testData)
        assertTrue { Crypto.isValid(DEFAULT_TLS_SIGNATURE_SCHEME, intermediateCaCert.publicKey, intermediateSignature, testData) }
    }

    @Test
    fun `create server certificate in keystore for SSL`() {
        val tmpCAKeyStore = tempFile("keystore.jks")
        val tmpTrustStore = tempFile("truststore.jks")
        val tmpSSLKeyStore = tempFile("sslkeystore.jks")
        val tmpServerKeyStore = tempFile("serverkeystore.jks")

        // Generate Root and Intermediate CA cert and put both into key store and root ca cert into trust store
        createCAKeyStoreAndTrustStore(tmpCAKeyStore,
                "cakeystorepass",
                "cakeypass",
                tmpTrustStore,
                "trustpass")

        // Load signing intermediate CA cert
        val caKeyStore = loadKeyStore(tmpCAKeyStore, "cakeystorepass")
        val caCertAndKey = caKeyStore.getCertificateAndKeyPair(X509Utilities.CORDA_INTERMEDIATE_CA, "cakeypass")

        // Generate server cert and private key and populate another keystore suitable for SSL
        createKeystoreForCordaNode(tmpSSLKeyStore, tmpServerKeyStore, "serverstorepass", "serverkeypass", caKeyStore, "cakeypass", MEGA_CORP.name)

        // Load back server certificate
        val serverKeyStore = loadKeyStore(tmpServerKeyStore, "serverstorepass")
        val serverCertAndKey = serverKeyStore.getCertificateAndKeyPair(X509Utilities.CORDA_CLIENT_CA, "serverkeypass")

        serverCertAndKey.certificate.isValidOn(Date())
        serverCertAndKey.certificate.isSignatureValid(JcaContentVerifierProviderBuilder().build(caCertAndKey.certificate.subjectPublicKeyInfo))

        assertTrue { serverCertAndKey.certificate.subject.toString().contains(MEGA_CORP.name.commonName) }

        // Load back server certificate
        val sslKeyStore = loadKeyStore(tmpSSLKeyStore, "serverstorepass")
        val sslCertAndKey = sslKeyStore.getCertificateAndKeyPair(X509Utilities.CORDA_CLIENT_TLS, "serverkeypass")

        sslCertAndKey.certificate.isValidOn(Date())
        sslCertAndKey.certificate.isSignatureValid(JcaContentVerifierProviderBuilder().build(serverCertAndKey.certificate.subjectPublicKeyInfo))

        assertTrue { sslCertAndKey.certificate.subject.toString().contains(MEGA_CORP.name.commonName) }
        // Now sign something with private key and verify against certificate public key
        val testData = "123456".toByteArray()
        val signature = Crypto.doSign(DEFAULT_TLS_SIGNATURE_SCHEME, serverCertAndKey.keyPair.private, testData)
        val publicKey = Crypto.toSupportedPublicKey(serverCertAndKey.certificate.subjectPublicKeyInfo)
        assertTrue { Crypto.isValid(DEFAULT_TLS_SIGNATURE_SCHEME, publicKey, signature, testData) }
    }

    @Test
    fun `create server cert and use in SSL socket`() {
        val tmpCAKeyStore = tempFile("keystore.jks")
        val tmpTrustStore = tempFile("truststore.jks")
        val tmpSSLKeyStore = tempFile("sslkeystore.jks")
        val tmpServerKeyStore = tempFile("serverkeystore.jks")

        // Generate Root and Intermediate CA cert and put both into key store and root ca cert into trust store
        val caKeyStore = createCAKeyStoreAndTrustStore(tmpCAKeyStore,
                "cakeystorepass",
                "cakeypass",
                tmpTrustStore,
                "trustpass")

        // Generate server cert and private key and populate another keystore suitable for SSL
        createKeystoreForCordaNode(tmpSSLKeyStore, tmpServerKeyStore, "serverstorepass", "serverstorepass", caKeyStore, "cakeypass", MEGA_CORP.name)
        val keyStore = loadKeyStore(tmpSSLKeyStore, "serverstorepass")
        val trustStore = loadKeyStore(tmpTrustStore, "trustpass")

        val context = SSLContext.getInstance("TLS")
        val keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm())
        keyManagerFactory.init(keyStore, "serverstorepass".toCharArray())
        val keyManagers = keyManagerFactory.keyManagers
        val trustMgrFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm())
        trustMgrFactory.init(trustStore)
        val trustManagers = trustMgrFactory.trustManagers
        context.init(keyManagers, trustManagers, SecureRandom())

        val serverSocketFactory = context.serverSocketFactory
        val clientSocketFactory = context.socketFactory

        val serverSocket = serverSocketFactory.createServerSocket(0) as SSLServerSocket // use 0 to get first free socket
        val serverParams = SSLParameters(arrayOf("TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256",
                "TLS_ECDH_ECDSA_WITH_AES_128_CBC_SHA256",
                "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA",
                "TLS_ECDH_ECDSA_WITH_AES_128_CBC_SHA",
                "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
                "TLS_ECDH_ECDSA_WITH_AES_128_GCM_SHA256"),
                arrayOf("TLSv1.2"))
        serverParams.wantClientAuth = true
        serverParams.needClientAuth = true
        serverParams.endpointIdentificationAlgorithm = null // Reconfirm default no server name indication, use our own validator.
        serverSocket.sslParameters = serverParams
        serverSocket.useClientMode = false

        val clientSocket = clientSocketFactory.createSocket() as SSLSocket
        val clientParams = SSLParameters(arrayOf("TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256",
                "TLS_ECDH_ECDSA_WITH_AES_128_CBC_SHA256",
                "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA",
                "TLS_ECDH_ECDSA_WITH_AES_128_CBC_SHA",
                "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
                "TLS_ECDH_ECDSA_WITH_AES_128_GCM_SHA256"),
                arrayOf("TLSv1.2"))
        clientParams.endpointIdentificationAlgorithm = null // Reconfirm default no server name indication, use our own validator.
        clientSocket.sslParameters = clientParams
        clientSocket.useClientMode = true
        // We need to specify this explicitly because by default the client binds to 'localhost' and we want it to bind
        // to whatever <hostname> resolves to(as that's what the server binds to). In particular on Debian <hostname>
        // resolves to 127.0.1.1 instead of the external address of the interface, so the ssl handshake fails.
        clientSocket.bind(InetSocketAddress(InetAddress.getLocalHost(), 0))

        val lock = Object()
        var done = false
        var serverError = false

        val serverThread = thread {
            try {
                val sslServerSocket = serverSocket.accept()
                assertTrue(sslServerSocket.isConnected)
                val serverInput = DataInputStream(sslServerSocket.inputStream)
                val receivedString = serverInput.readUTF()
                assertEquals("Hello World", receivedString)
                synchronized(lock) {
                    done = true
                    lock.notifyAll()
                }
                sslServerSocket.close()
            } catch (ex: Throwable) {
                serverError = true
            }
        }

        clientSocket.connect(InetSocketAddress(InetAddress.getLocalHost(), serverSocket.localPort))
        assertTrue(clientSocket.isConnected)

        // Double check hostname manually
        val peerChain = clientSocket.session.peerCertificates
        val peerX500Principal = (peerChain[0] as X509Certificate).subjectX500Principal
        val x500name = X500Name(peerX500Principal.name)
        assertEquals(MEGA_CORP.name, x500name)
        X509Utilities.validateCertificateChain(trustStore.getX509Certificate(X509Utilities.CORDA_ROOT_CA), *peerChain)
        val output = DataOutputStream(clientSocket.outputStream)
        output.writeUTF("Hello World")
        var timeout = 0
        synchronized(lock) {
            while (!done) {
                timeout++
                if (timeout > 10) throw IOException("Timed out waiting for server to complete")
                lock.wait(1000)
            }
        }

        clientSocket.close()
        serverThread.join(1000)
        assertFalse { serverError }
        serverSocket.close()
        assertTrue(done)
    }

    private fun tempFile(name: String): Path = tempFolder.root.toPath() / name

    /**
     * All in one wrapper to manufacture a root CA cert and an Intermediate CA cert.
     * Normally this would be run once and then the outputs would be re-used repeatedly to manufacture the server certs
     * @param keyStoreFilePath The output KeyStore path to publish the private keys of the CA root and intermediate certs into.
     * @param storePassword The storage password to protect access to the generated KeyStore and public certificates
     * @param keyPassword The password that protects the CA private keys.
     * Unlike the SSL libraries that tend to assume the password is the same as the keystore password.
     * These CA private keys should be protected more effectively with a distinct password.
     * @param trustStoreFilePath The output KeyStore to place the Root CA public certificate, which can be used as an SSL truststore
     * @param trustStorePassword The password to protect the truststore
     * @return The KeyStore object that was saved to file
     */
    private fun createCAKeyStoreAndTrustStore(keyStoreFilePath: Path,
                                              storePassword: String,
                                              keyPassword: String,
                                              trustStoreFilePath: Path,
                                              trustStorePassword: String
    ): KeyStore {
        val rootCAKey = generateKeyPair(DEFAULT_TLS_SIGNATURE_SCHEME)
        val rootCACert = createSelfSignedCACertificate(X509Utilities.getX509Name("Corda Node Root CA","London","demo@r3.com",null), rootCAKey)

        val intermediateCAKeyPair = Crypto.generateKeyPair(X509Utilities.DEFAULT_TLS_SIGNATURE_SCHEME)
        val intermediateCACert = X509Utilities.createCertificate(CertificateType.INTERMEDIATE_CA, rootCACert, rootCAKey, X509Utilities.getX509Name("Corda Node Intermediate CA","London","demo@r3.com",null), intermediateCAKeyPair.public)

        val keyPass = keyPassword.toCharArray()
        val keyStore = loadOrCreateKeyStore(keyStoreFilePath, storePassword)

        keyStore.addOrReplaceKey(X509Utilities.CORDA_ROOT_CA, rootCAKey.private, keyPass, arrayOf<Certificate>(rootCACert.cert))

        keyStore.addOrReplaceKey(X509Utilities.CORDA_INTERMEDIATE_CA,
                intermediateCAKeyPair.private,
                keyPass,
                Stream.of(intermediateCACert, rootCACert).map { it.cert }.toTypedArray<Certificate>())

        keyStore.save(keyStoreFilePath, storePassword)

        val trustStore = loadOrCreateKeyStore(trustStoreFilePath, trustStorePassword)

        trustStore.addOrReplaceCertificate(X509Utilities.CORDA_ROOT_CA, rootCACert.cert)
        trustStore.addOrReplaceCertificate(X509Utilities.CORDA_INTERMEDIATE_CA, intermediateCACert.cert)

        trustStore.save(trustStoreFilePath, trustStorePassword)

        return keyStore
    }

    @Test
    fun `Get correct private key type from Keystore`() {
        val keyPair = generateKeyPair(Crypto.ECDSA_SECP256R1_SHA256)
        val selfSignCert = createSelfSignedCACertificate(X500Name("CN=Test"), keyPair)
        val keyStore = loadOrCreateKeyStore(tempFile("testKeystore.jks"), "keystorepassword")
        keyStore.setKeyEntry("Key", keyPair.private, "keypassword".toCharArray(), arrayOf(selfSignCert.cert))

        val keyFromKeystore = keyStore.getKey("Key", "keypassword".toCharArray())
        val keyFromKeystoreCasted = keyStore.getSupportedKey("Key", "keypassword")

        assertTrue(keyFromKeystore is java.security.interfaces.ECPrivateKey) // by default JKS returns SUN EC key
        assertTrue(keyFromKeystoreCasted is org.bouncycastle.jce.interfaces.ECPrivateKey)
    }
}
