package seaweedfs.client;

import org.junit.Test;

import java.util.Base64;

import static seaweedfs.client.SeaweedCipher.decrypt;
import static seaweedfs.client.SeaweedCipher.encrypt;

public class SeaweedCipherTest {

    @Test
    public void testSameAsGoImplemnetation() throws Exception {
        byte[] secretKey = "256-bit key for AES 256 GCM encr".getBytes();

        String plainText = "Now we need to generate a 256-bit key for AES 256 GCM";

        System.out.println("Original Text : " + plainText);

        byte[] cipherText = encrypt(plainText.getBytes(), secretKey);
        System.out.println("Encrypted Text : " + Base64.getEncoder().encodeToString(cipherText));

        byte[] decryptedText = decrypt(cipherText, secretKey);
        System.out.println("DeCrypted Text : " + new String(decryptedText));
    }

    @Test
    public void testEncryptDecrypt() throws Exception {
        byte[] secretKey = SeaweedCipher.genCipherKey();

        String plainText = "Now we need to generate a 256-bit key for AES 256 GCM";

        System.out.println("Original Text : " + plainText);

        byte[] cipherText = encrypt(plainText.getBytes(), secretKey);
        System.out.println("Encrypted Text : " + Base64.getEncoder().encodeToString(cipherText));

        byte[] decryptedText = decrypt(cipherText, secretKey);
        System.out.println("DeCrypted Text : " + new String(decryptedText));
    }

}
