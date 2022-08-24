package seaweedfs.client;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.security.SecureRandom;

public class SeaweedCipher {
    // AES-GCM parameters
    public static final int AES_KEY_SIZE = 256; // in bits
    public static final int GCM_NONCE_LENGTH = 12; // in bytes
    public static final int GCM_TAG_LENGTH = 16; // in bytes

    private static SecureRandom random = new SecureRandom();

    public static byte[] genCipherKey() throws Exception {
        byte[] key = new byte[AES_KEY_SIZE / 8];
        random.nextBytes(key);
        return key;
    }

    public static byte[] encrypt(byte[] clearTextbytes, byte[] cipherKey) throws Exception {
        return encrypt(clearTextbytes, 0, clearTextbytes.length, cipherKey);
    }

    public static byte[] encrypt(byte[] clearTextbytes, int offset, int length, byte[] cipherKey) throws Exception {

        final byte[] nonce = new byte[GCM_NONCE_LENGTH];
        random.nextBytes(nonce);
        GCMParameterSpec spec = new GCMParameterSpec(GCM_TAG_LENGTH * 8, nonce);
        SecretKeySpec keySpec = new SecretKeySpec(cipherKey, "AES");

        Cipher AES_cipherInstance = Cipher.getInstance("AES/GCM/NoPadding");
        AES_cipherInstance.init(Cipher.ENCRYPT_MODE, keySpec, spec);

        byte[] encryptedText = AES_cipherInstance.doFinal(clearTextbytes, offset, length);

        byte[] iv = AES_cipherInstance.getIV();
        byte[] message = new byte[GCM_NONCE_LENGTH + length + GCM_TAG_LENGTH];
        System.arraycopy(iv, 0, message, 0, GCM_NONCE_LENGTH);
        System.arraycopy(encryptedText, 0, message, GCM_NONCE_LENGTH, encryptedText.length);

        return message;
    }

    public static byte[] decrypt(byte[] encryptedText, byte[] cipherKey) throws Exception {
        final Cipher AES_cipherInstance = Cipher.getInstance("AES/GCM/NoPadding");
        GCMParameterSpec params = new GCMParameterSpec(GCM_TAG_LENGTH * 8, encryptedText, 0, GCM_NONCE_LENGTH);
        SecretKeySpec keySpec = new SecretKeySpec(cipherKey, "AES");
        AES_cipherInstance.init(Cipher.DECRYPT_MODE, keySpec, params);
        byte[] decryptedText = AES_cipherInstance.doFinal(encryptedText, GCM_NONCE_LENGTH, encryptedText.length - GCM_NONCE_LENGTH);
        return decryptedText;
    }

}
