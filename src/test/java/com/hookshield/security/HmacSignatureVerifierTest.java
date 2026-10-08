package com.hookshield.security;

import com.hookshield.service.HmacSignatureVerifier;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class HmacSignatureVerifierTest {

    private static final String SECRET = "whsec_super_secret_signing_key_12345";
    private static final String PAYLOAD = "{\"event\":\"payment_intent.succeeded\",\"amount\":2000}";

    @Test
    @DisplayName("Should verify valid standard raw HMAC-SHA256 signature")
    void testValidStandardHmacSignature() {
        String hexSignature = HmacSignatureVerifier.computeHmacSha256Hex(PAYLOAD, SECRET);

        boolean isValid = HmacSignatureVerifier.verify(PAYLOAD, SECRET, hexSignature);
        assertThat(isValid).isTrue();
    }

    @Test
    @DisplayName("Should verify GitHub style header prefixed with 'sha256='")
    void testValidGitHubHeaderSignature() {
        String hexSignature = HmacSignatureVerifier.computeHmacSha256Hex(PAYLOAD, SECRET);
        String githubHeader = "sha256=" + hexSignature;

        boolean isValid = HmacSignatureVerifier.verify(PAYLOAD, SECRET, githubHeader);
        assertThat(isValid).isTrue();
    }

    @Test
    @DisplayName("Should verify Stripe style header formatted as 't=timestamp,v1=signature'")
    void testValidStripeHeaderSignature() {
        String hexSignature = HmacSignatureVerifier.computeHmacSha256Hex(PAYLOAD, SECRET);
        String stripeHeader = "t=1710000000,v1=" + hexSignature;

        boolean isValid = HmacSignatureVerifier.verify(PAYLOAD, SECRET, stripeHeader);
        assertThat(isValid).isTrue();
    }

    @Test
    @DisplayName("Should reject tampered payload with altered content")
    void testRejectTamperedPayload() {
        String hexSignature = HmacSignatureVerifier.computeHmacSha256Hex(PAYLOAD, SECRET);
        String tamperedPayload = "{\"event\":\"payment_intent.succeeded\",\"amount\":999999}";

        boolean isValid = HmacSignatureVerifier.verify(tamperedPayload, SECRET, hexSignature);
        assertThat(isValid).isFalse();
    }

    @Test
    @DisplayName("Should reject invalid signature string")
    void testRejectInvalidSignature() {
        boolean isValid = HmacSignatureVerifier.verify(PAYLOAD, SECRET, "deadbeef0102030405060708");
        assertThat(isValid).isFalse();
    }

    @Test
    @DisplayName("Should reject null or empty arguments safely")
    void testRejectNullArguments() {
        assertThat(HmacSignatureVerifier.verify(null, SECRET, "sig")).isFalse();
        assertThat(HmacSignatureVerifier.verify(PAYLOAD, null, "sig")).isFalse();
        assertThat(HmacSignatureVerifier.verify(PAYLOAD, SECRET, null)).isFalse();
        assertThat(HmacSignatureVerifier.verify(PAYLOAD, SECRET, "   ")).isFalse();
    }
}
