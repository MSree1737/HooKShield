package com.hookshield.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * Thread-safe, constant-time cryptographic HMAC-SHA256 signature verifier
 * designed to protect against timing attacks.
 */
public final class HmacSignatureVerifier {

    private static final Logger log = LoggerFactory.getLogger(HmacSignatureVerifier.class);
    private static final String HMAC_SHA256_ALGORITHM = "HmacSHA256";
    private static final HexFormat HEX_FORMAT = HexFormat.of();

    private HmacSignatureVerifier() {
        // Utility class
    }

    /**
     * Verifies that the given signature matches the HMAC-SHA256 of the payload using the secret.
     * Uses constant-time comparison via MessageDigest.isEqual to mitigate timing side-channel attacks.
     *
     * @param payload          The raw payload string received in the webhook request.
     * @param secret           The shared secret configured for the tenant provider.
     * @param providedSignature The signature header value provided by the sender.
     * @return true if the signature is valid; false otherwise.
     */
    public static boolean verify(String payload, String secret, String providedSignature) {
        if (payload == null || secret == null || providedSignature == null || providedSignature.isBlank()) {
            return false;
        }

        try {
            String cleanSignature = extractSignatureValue(providedSignature.trim());
            byte[] expectedBytes = computeHmacSha256(payload, secret);
            byte[] providedBytes = parseSignatureBytes(cleanSignature);

            if (providedBytes == null) {
                return false;
            }

            return MessageDigest.isEqual(expectedBytes, providedBytes);
        } catch (Exception e) {
            log.warn("HMAC verification failed due to exception: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Computes the raw HMAC-SHA256 bytes for a given payload and secret.
     */
    public static byte[] computeHmacSha256(String payload, String secret) {
        try {
            Mac mac = Mac.getInstance(HMAC_SHA256_ALGORITHM);
            SecretKeySpec secretKeySpec = new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), HMAC_SHA256_ALGORITHM);
            mac.init(secretKeySpec);
            return mac.doFinal(payload.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            throw new IllegalStateException("Failed to calculate HMAC-SHA256", e);
        }
    }

    /**
     * Computes the lowercase hex string for HMAC-SHA256.
     */
    public static String computeHmacSha256Hex(String payload, String secret) {
        return HEX_FORMAT.formatHex(computeHmacSha256(payload, secret));
    }

    /**
     * Extracts signature tokens across various webhook providers:
     * - GitHub: "sha256=1234abcd..." -> "1234abcd..."
     * - Stripe: "t=123,v1=1234abcd..." -> "1234abcd..."
     */
    private static String extractSignatureValue(String signatureHeader) {
        if (signatureHeader.startsWith("sha256=")) {
            return signatureHeader.substring("sha256=".length()).trim();
        }

        if (signatureHeader.contains("v1=")) {
            for (String part : signatureHeader.split(",")) {
                String trimmed = part.trim();
                if (trimmed.startsWith("v1=")) {
                    return trimmed.substring("v1=".length()).trim();
                }
            }
        }

        return signatureHeader;
    }

    private static byte[] parseSignatureBytes(String hexString) {
        try {
            return HEX_FORMAT.parseHex(hexString);
        } catch (IllegalArgumentException e) {
            // If not valid hex, attempt fallback Base64 decoding
            try {
                return java.util.Base64.getDecoder().decode(hexString);
            } catch (IllegalArgumentException ex) {
                return null;
            }
        }
    }
}
